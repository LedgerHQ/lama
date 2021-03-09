package co.ledger.lama.bitcoin.interpreter.services

import java.util.UUID

import co.ledger.lama.bitcoin.common.models.interpreter.{
  BlockView,
  InputView,
  OutputView,
  TransactionView
}
import co.ledger.lama.bitcoin.interpreter.models.implicits._
import doobie._
import doobie.implicits._
import doobie.postgres.implicits._
import fs2.Stream

object TransactionQueries {

  def fetchMostRecentBlocks(accountId: UUID): Stream[ConnectionIO, BlockView] = {
    sql"""SELECT DISTINCT block_hash, block_height, block_time
          FROM transaction
          WHERE account_id = $accountId
          ORDER BY block_height DESC
          LIMIT 200 -- the biggest reorg that happened on bitcoin was 53 blocks long
       """.query[BlockView].stream
  }

  def saveTransaction(tx: TransactionView, accountId: UUID): ConnectionIO[Int] =
    for {

      txStatement <- insertTx(accountId, tx)

      _ <- insertInputs(
        accountId,
        tx.hash,
        tx.inputs.toList
      )

      _ <- insertOutputs(accountId, tx.hash, tx.outputs.toList)

    } yield {
      txStatement
    }

  def deleteUnconfirmedTransactions(accountId: UUID): doobie.ConnectionIO[Int] = {
    sql"""DELETE FROM transaction
         WHERE account_id = $accountId
         AND block_hash IS NULL
       """.update.run
  }

  private def insertTx(
      accountId: UUID,
      tx: TransactionView
  ): doobie.ConnectionIO[Int] = {

    val update =
      fr"""DO UPDATE SET
              block_hash   = ${tx.block.map(_.hash)}, 
              block_height = ${tx.block.map(_.height)}, 
              block_time   = ${tx.block.map(_.time)}
            WHERE transaction.block_hash IS NULL
       """

    val noUpdate = fr"""DO NOTHING"""

    val query = sql"""INSERT INTO transaction (
            account_id, id, hash, block_hash, block_height, block_time, received_at, lock_time, fees, confirmations
          ) VALUES (
            $accountId,
            ${tx.id},
            ${tx.hash},
            ${tx.block.map(_.hash)},
            ${tx.block.map(_.height)},
            ${tx.block.map(_.time)},
            ${tx.receivedAt},
            ${tx.lockTime},
            ${tx.fees},
            ${tx.confirmations}
          ) ON CONFLICT ON CONSTRAINT transaction_pkey """ ++
      tx.block.map(_ => update).getOrElse(noUpdate)

    query.update.run
  }

  private def insertInputs(
      accountId: UUID,
      txHash: String,
      inputs: List[InputView]
  ): doobie.ConnectionIO[Int] = {
    val query =
      s"""INSERT INTO input (
            account_id, hash, output_hash, output_index, input_index, value, address, script_signature, txinwitness, sequence, derivation
          ) VALUES (
            '$accountId', '$txHash', ?, ?, ?, ?, ?, ?, ?, ?, ?
          )
          ON CONFLICT ON CONSTRAINT input_pkey DO NOTHING
       """
    Update[InputView](query).updateMany(inputs)
  }

  private def insertOutputs(
      accountId: UUID,
      txHash: String,
      outputs: List[OutputView]
  ) = {
    val query = s"""INSERT INTO output (
            account_id, hash, output_index, value, address, script_hex, change_type, derivation
          ) VALUES (
            '$accountId', '$txHash', ?, ?, ?, ?, ?, ?
          ) ON CONFLICT ON CONSTRAINT output_pkey DO NOTHING
        """
    Update[OutputView](query).updateMany(outputs)
  }

  def removeFromCursor(accountId: UUID, blockHeight: Long): ConnectionIO[Int] =
    sql"""DELETE from transaction
          WHERE account_id = $accountId
          AND block_height >= $blockHeight
       """.update.run
}
