package co.ledger.lama.bitcoin.interpreter.services

import java.util.UUID
import cats.data.NonEmptyList
import cats.implicits._
import co.ledger.lama.bitcoin.common.models.interpreter._
import co.ledger.lama.common.models.implicits._
import co.ledger.lama.bitcoin.interpreter.models.{OperationToSave, TransactionAmounts}
import co.ledger.lama.bitcoin.interpreter.models.implicits._
import co.ledger.lama.common.logging.IOLogging
import co.ledger.lama.common.models.Sort
import doobie._
import doobie.implicits._
import doobie.postgres.implicits._
import io.circe.syntax._
import fs2.{Chunk, Pipe, Stream}
import io.circe.Json

import java.time.Instant

object OperationQueries extends IOLogging {

  case class Tx(
      id: String,
      hash: String,
      receivedAt: Instant,
      lockTime: Long,
      fees: BigInt,
      block: Option[BlockView],
      confirmations: Int
  )

  case class Op(
      uid: Operation.UID,
      accountId: UUID,
      hash: String,
      operationType: OperationType,
      amount: BigInt,
      fees: BigInt,
      time: Instant,
      blockHeight: Option[Long]
  )

  def fetchInputsWithOutputsOrderedByTxHash(
      accountId: UUID,
      sort: Sort,
      txHashes: NonEmptyList[String]
  ): Stream[doobie.ConnectionIO, (String, (List[InputView], List[OutputView]))] = {
    log.logger.debug(
      s"Fetching inputs and outputs for accountId $accountId and hashes in $txHashes"
    )

    def groupByTxHash[T]: Pipe[ConnectionIO, (String, T), (String, Chunk[T])] =
      _.groupAdjacentBy { case (txHash, _) => txHash }
        .map { case (txHash, chunks) => txHash -> chunks.map(_._2) }

    val inputs  = fetchInputs(accountId, sort, txHashes).stream.through(groupByTxHash)
    val outputs = fetchOutputs(accountId, sort, txHashes).stream.through(groupByTxHash)

    inputs
      .zip(outputs)
      .collect {
        case ((txhash1, i), (txHash2, o)) if txhash1 == txHash2 =>
          txhash1 -> (
            i.toList.flatten.sortBy(i => (i.outputHash, i.outputIndex)),
            o.toList.flatten.sortBy(_.outputIndex)
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
            AND i.address IS NULL""".query[Int].unique

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
            WHERE o.account_id = $accountId
              AND o.derivation IS NOT NULL
              AND i.address IS NULL
         """ ++ orderF ++ limitF ++ offsetF
    query.query[Utxo].stream
  }

  def saveOperations(operation: Chunk[OperationToSave]): ConnectionIO[Int] = {
    val query =
      """INSERT INTO operation (
         uid, account_id, hash, operation_type, amount, fees, time, block_hash, block_height
       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT ON CONSTRAINT operation_pkey DO NOTHING
    """
    Update[OperationToSave](query).updateMany(operation)
  }

  def fetchUnconfirmedTransactionsViews(
      accountId: UUID
  ): ConnectionIO[List[TransactionView]] = {
    log.logger.debug(s"Fetching transactions for accountId $accountId")
    sql"""SELECT transaction_views
          FROM unconfirmed_transaction_view
          WHERE account_id = $accountId
       """
      .query[Json]
      .option
      .map { t =>
        t.map(_.as[List[TransactionView]]) match {
          case Some(result) =>
            result.leftMap { error =>
              log.error("Could not parse TansactionView list json : ", error)
              error
            }
          case None => Right(List.empty[TransactionView])
        }
      }
      .rethrow
  }

  def deleteUnconfirmedTransactionsViews(accountId: UUID): doobie.ConnectionIO[Int] = {
    sql"""DELETE FROM unconfirmed_transaction_view
         WHERE account_id = $accountId
       """.update.run
  }

  def deleteUnconfirmedOperations(accountId: UUID): doobie.ConnectionIO[Int] = {
    sql"""DELETE FROM operation
         WHERE account_id = $accountId
         AND block_height IS NULL
       """.update.run
  }

  def saveUnconfirmedTransactionView(
      accountId: UUID,
      txs: List[TransactionView]
  ): ConnectionIO[Int] = {
    sql"""INSERT INTO unconfirmed_transaction_view(
            account_id, transaction_views
          ) VALUES (
            $accountId, ${txs.asJson}
          )
       """.update.run
  }

  private def operationOrder(sort: Sort)                = Fragment.const(s"ORDER BY o.time $sort, o.hash $sort")
  private def allTxHashes(hashes: NonEmptyList[String]) = Fragments.in(fr"o.hash", hashes)

  private def fetchInputs(
      accountId: UUID,
      sort: Sort,
      txHashes: NonEmptyList[String]
  ) = {

    val belongsToTxs = allTxHashes(txHashes)

    (sql"""
          SELECT o.hash, i.output_hash, i.output_index, i.input_index, i.value, i.address, i.script_signature, i.txinwitness, i.sequence, i.derivation
            FROM operation o 
            LEFT JOIN input i on i.account_id = o.account_id and i.hash = o.hash
           WHERE o.account_id = $accountId
             AND $belongsToTxs
       """ ++ operationOrder(sort))
      .query[(String, Option[InputView])]
  }

  private def fetchOutputs(
      accountId: UUID,
      sort: Sort,
      txHashes: NonEmptyList[String]
  ) = {

    val belongsToTxs = allTxHashes(txHashes)

    (
      sql"""
          SELECT o.hash, output.output_index, output.value, output.address, output.script_hex, output.change_type, output.derivation
            FROM operation o  
            LEFT JOIN output on output.account_id = o.account_id and output.hash = o.hash
           WHERE o.account_id = $accountId
             AND $belongsToTxs
       """ ++ operationOrder(sort)
    ).query[(String, Option[OutputView])]
  }

  def countOperations(accountId: UUID, blockHeight: Long = 0L): ConnectionIO[Int] =
    sql"""SELECT COUNT(*) FROM operation WHERE account_id = $accountId AND (block_height >= $blockHeight OR block_height IS NULL)"""
      .query[Int]
      .unique

  private val operationWithTx =
    sql"""
         SELECT 
           o.uid, o.account_id, o.hash, o.operation_type, o.amount, o.fees, o.time, o.block_height,
           t.id, t.hash, t.received_at, t.lock_time, t.fees, t.block_hash, t.block_height, t.block_time, t.confirmations
           FROM "transaction" t 
           JOIN "operation" o on t.hash = o.hash and o.account_id = t.account_id 
       """

  def fetchOperations(
      accountId: UUID,
      blockHeight: Long = 0L,
      sort: Sort = Sort.Descending,
      limit: Option[Int] = None,
      offset: Option[Int] = None
  ): Stream[ConnectionIO, (Op, Tx)] = {
    val limitF  = limit.map(l => fr"LIMIT $l").getOrElse(Fragment.empty)
    val offsetF = offset.map(o => fr"OFFSET $o").getOrElse(Fragment.empty)

    val filter =
      fr"""
             WHERE o.account_id = $accountId
               AND (o.block_height >= $blockHeight
                OR o.block_height IS NULL)
         """ ++ operationOrder(sort) ++ limitF ++ offsetF

    (operationWithTx ++ filter)
      .query[(Op, Tx)]
      .stream
  }

  def findOperation(
      accountId: Operation.AccountId,
      operationId: Operation.UID
  ): ConnectionIO[Option[(Op, Tx)]] = {

    val filter =
      fr"""
           WHERE o.account_id = ${accountId.value}
             AND o.uid = ${operationId.hex}
           LIMIT 1
        """

    (operationWithTx ++ filter)
      .query[(Op, Tx)]
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
