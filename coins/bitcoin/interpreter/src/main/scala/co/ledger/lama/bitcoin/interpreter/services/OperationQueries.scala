package co.ledger.lama.bitcoin.interpreter.services

import java.util.UUID

import cats.data.NonEmptyList
import co.ledger.lama.bitcoin.common.models.interpreter._
import co.ledger.lama.bitcoin.interpreter.models.{OperationToSave, TransactionAmounts}
import co.ledger.lama.bitcoin.interpreter.models.implicits._
import co.ledger.lama.common.logging.IOLogging
import co.ledger.lama.common.models.Sort
import doobie._
import doobie.implicits._
import doobie.postgres.implicits._
import fs2.{Chunk, Stream}

object OperationQueries extends IOLogging {

  def fetchTransaction(
      accountId: UUID,
      hash: String
  ): ConnectionIO[Option[TransactionView]] = {
    log.logger.debug(s"Fetching transaction for accountId $accountId and hash $hash")
    for {
      tx <- fetchTx(accountId, hash)
      _ = log.logger.debug(s"Transaction $tx")
      inputs <- fetchInputs(accountId, hash).compile.toList
      _ = log.logger.debug(s"Inputs $inputs")
      outputs <- fetchOutputs(accountId, hash).compile.toList
      _ = log.logger.debug(s"Outputs $outputs")
    } yield {
      tx.map(
        _.copy(
          inputs = inputs,
          outputs = outputs
        )
      )
    }
  }

  def fetchTransactionAmounts(
      accountId: UUID
  ): Stream[ConnectionIO, TransactionAmounts] =
    sql"""SELECT tx.account_id,
                 tx.hash,
                 tx.block_hash,
                 tx.block_height,
                 tx.block_time,
                 tx.fees,
                 COALESCE(tx.input_amount, 0),
                 COALESCE(tx.output_amount, 0),
                 COALESCE(tx.change_amount, 0)
          FROM transaction_amount tx
            LEFT JOIN operation op
              ON op.hash = tx.hash
              AND op.account_id = tx.account_id
          WHERE op.hash IS NULL
          AND tx.account_id = $accountId
       """
      .query[TransactionAmounts]
      .stream

  def countUTXOs(accountId: UUID): ConnectionIO[Int] =
    sql"""SELECT COUNT(*)
          FROM output o
            LEFT JOIN input i
              ON o.account_id = i.account_id
              AND o.address = i.address
              AND o.output_index = i.output_index
              AND o.hash = i.output_hash
            INNER JOIN transaction tx
              ON o.account_id = tx.account_id
              AND o.hash = tx.hash
          WHERE o.account_id = $accountId
            AND o.derivation IS NOT NULL
            AND i.address IS NULL
            AND tx.block_hash IS NOT NULL
       """
      .query[Int]
      .unique

  def fetchUTXOs(
      accountId: UUID,
      sort: Sort = Sort.Ascending,
      limit: Option[Int] = None,
      offset: Option[Int] = None
  ): Stream[ConnectionIO, Utxo] = {
    val orderF  = Fragment.const(s"ORDER BY tx.block_time $sort, tx.hash $sort")
    val limitF  = limit.map(l => fr"LIMIT $l").getOrElse(Fragment.empty)
    val offsetF = offset.map(o => fr"OFFSET $o").getOrElse(Fragment.empty)

    val query =
      sql"""SELECT tx.hash, o.output_index, o.value, o.address, o.script_hex, o.change_type, o.derivation, tx.block_time
            FROM output o
              LEFT JOIN input i
                ON o.account_id = i.account_id
                AND o.address = i.address
                AND o.output_index = i.output_index
			          AND o.hash = i.output_hash
              INNER JOIN transaction tx
                ON o.account_id = tx.account_id
                AND o.hash = tx.hash
                AND tx.block_hash IS NOT NULL
            WHERE o.account_id = $accountId
              AND o.derivation IS NOT NULL
              AND i.address IS NULL
         """ ++ orderF ++ limitF ++ offsetF
    query.query[Utxo].stream
  }

  def fetchUnconfirmedUTXOs(
      accountId: UUID
  ): Stream[ConnectionIO, Utxo] =
    sql"""SELECT tx.hash, o.output_index, o.value, o.address, o.script_hex, o.change_type, o.derivation, tx.received_at
            FROM output o
              LEFT JOIN input i
                ON o.account_id = i.account_id
                AND o.address = i.address
                AND o.output_index = i.output_index
			          AND o.hash = i.output_hash
              INNER JOIN transaction tx
                ON o.account_id = tx.account_id
                AND o.hash = tx.hash
                AND tx.block_hash IS NULL
            WHERE o.account_id = $accountId
              AND o.derivation IS NOT NULL
              AND i.address IS NULL
         """.query[Utxo].stream

  def saveOperations(operation: Chunk[OperationToSave]): ConnectionIO[Int] = {
    val query =
      """INSERT INTO operation (
         uid, account_id, hash, operation_type, amount, fees, time, block_hash, block_height
       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT ON CONSTRAINT operation_pkey DO NOTHING
    """
    Update[OperationToSave](query).updateMany(operation)
  }

  def deleteUnconfirmedOperations(accountId: UUID): doobie.ConnectionIO[Int] = {
    sql"""DELETE FROM operation
         WHERE account_id = $accountId
         AND block_height IS NULL
       """.update.run
  }

  private def fetchTx(
      accountId: UUID,
      hash: String
  ): ConnectionIO[Option[TransactionView]] =
    sql"""SELECT id, hash, block_hash, block_height, block_time, received_at, lock_time, fees, confirmations
          FROM transaction
          WHERE hash = $hash
          AND account_id = $accountId
       """
      .query[TransactionView]
      .option

  private def fetchInputs(
      accountId: UUID,
      hash: String
  ): Stream[ConnectionIO, InputView] = {
    sql"""SELECT output_hash, output_index, input_index, value, address, script_signature, txinwitness, sequence, derivation
          FROM input
          WHERE account_id = $accountId
          AND hash = $hash
       """
      .query[InputView]
      .stream
  }

  private def fetchOutputs(
      accountId: UUID,
      hash: String
  ): Stream[ConnectionIO, OutputView] =
    sql"""SELECT output_index, value, address, script_hex, change_type, derivation
          FROM output
          WHERE account_id = $accountId
          AND hash = $hash
       """
      .query[OutputView]
      .stream

  def countOperations(accountId: UUID, blockHeight: Long = 0L): ConnectionIO[Int] =
    sql"""SELECT COUNT(*) FROM operation WHERE account_id = $accountId AND (block_height >= $blockHeight OR block_height IS NULL)"""
      .query[Int]
      .unique

  def fetchOperations(
      accountId: UUID,
      blockHeight: Long = 0L,
      sort: Sort = Sort.Descending,
      limit: Option[Int] = None,
      offset: Option[Int] = None
  ): Stream[ConnectionIO, Operation] = {
    val orderF  = Fragment.const(s"ORDER BY time $sort, hash $sort")
    val limitF  = limit.map(l => fr"LIMIT $l").getOrElse(Fragment.empty)
    val offsetF = offset.map(o => fr"OFFSET $o").getOrElse(Fragment.empty)

    val query =
      sql"""SELECT uid, account_id, hash, operation_type, amount, fees, time, block_height
              FROM operation
             WHERE account_id = $accountId
               AND (block_height >= $blockHeight
                OR block_height IS NULL)
         """ ++ orderF ++ limitF ++ offsetF
    query.query[Operation].stream
  }

  def findOperation(
      accountId: Operation.AccountId,
      operationId: Operation.UID
  ): ConnectionIO[Option[Operation]] = {

    sql"""SELECT uid, account_id, hash, operation_type, amount, fees, time, block_height
              FROM operation
             WHERE account_id = ${accountId.value}
               AND uid = ${operationId.hex}
             LIMIT 1
         """
      .query[Operation]
      .option
  }

  def flagBelongingInputs(
      accountId: UUID,
      addresses: NonEmptyList[AccountAddress]
  ): ConnectionIO[Int] = {
    val queries = addresses.map { addr =>
      sql"""UPDATE input
            SET derivation = ${addr.derivation.toList}
            WHERE account_id = $accountId
            AND address = ${addr.accountAddress}
         """
    }

    queries.traverse(_.update.run).map(_.toList.sum)
  }

  def flagBelongingOutputs(
      accountId: UUID,
      addresses: NonEmptyList[AccountAddress],
      changeType: ChangeType
  ): ConnectionIO[Int] = {
    val queries = addresses.map { addr =>
      sql"""UPDATE output
            SET change_type = $changeType,
                derivation = ${addr.derivation.toList}
            WHERE account_id = $accountId
            AND address = ${addr.accountAddress}
         """
    }

    queries.traverse(_.update.run).map(_.toList.sum)
  }

  def removeFromCursor(accountId: UUID, blockHeight: Long): ConnectionIO[Int] =
    sql"""DELETE from operation
          WHERE account_id = $accountId
          AND (block_height >= $blockHeight
              OR block_height IS NULL)
       """.update.run
}
