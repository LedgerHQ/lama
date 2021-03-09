package co.ledger.lama.bitcoin.interpreter.services

import java.time.Instant
import java.util.UUID

import co.ledger.lama.bitcoin.common.models.interpreter.{BalanceHistory, BlockchainBalance}
import co.ledger.lama.bitcoin.interpreter.models.implicits._
import doobie._
import doobie.implicits._
import doobie.postgres.implicits._
import fs2.Stream

object BalanceQueries {

  def getUncomputedBalanceHistories(
      accountId: UUID,
      blockHeight: Long
  ): Stream[ConnectionIO, BalanceHistory] =
    sql"""
          WITH ops as (
            SELECT
              account_id,
              block_height,
              time,
              SUM( ( case when operation_type = 'send' then -1 else 1 end ) * amount) as balance
            FROM operation
            WHERE account_id = $accountId
              AND block_height > $blockHeight
            GROUP BY account_id, time, block_height
            ORDER BY time
          )
          
          SELECT
            account_id,
            SUM(balance) OVER (PARTITION BY account_id ORDER by block_height) as balance,
            block_height,
            time
          FROM ops
       """
      .query[BalanceHistory]
      .stream

  def getBlockchainBalance(
      accountId: UUID
  ): ConnectionIO[BlockchainBalance] = {
    val balanceAndUtxosQuery =
      sql"""SELECT COALESCE(SUM(o.value), 0), COALESCE(COUNT(o.value), 0)
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
        .query[(BigInt, Int)]
        .unique

    val receivedAndSentQuery =
      sql"""SELECT
              COALESCE(SUM(CASE WHEN operation_type = 'receive' THEN amount ELSE 0 END), 0) as received,
              COALESCE(SUM(CASE WHEN operation_type = 'send' THEN amount ELSE 0 END), 0) as sent
            FROM operation
            WHERE account_id = $accountId
         """
        .query[(BigInt, BigInt)]
        .unique

    for {
      result1 <- balanceAndUtxosQuery
      result2 <- receivedAndSentQuery
    } yield {
      val (balance, utxos) = result1
      val (received, sent) = result2
      BlockchainBalance(balance, utxos, received, sent)
    }
  }

  def getUnconfirmedBalance(
      accountId: UUID
  ): ConnectionIO[BigInt] =
    sql"""SELECT COALESCE(SUM(o.value), 0)
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
            AND tx.block_hash IS NULL
      """
      .query[BigInt]
      .unique

  def saveBalanceHistory(
      balances: List[BalanceHistory]
  ): ConnectionIO[Int] = {
    val query = """INSERT INTO balance_history(
            account_id, balance, block_height, time
          ) VALUES(?, ?, ?, ?)
       """
    Update[BalanceHistory](query).updateMany(balances)
  }

  def getBalanceHistory(
      accountId: UUID,
      start: Option[Instant],
      end: Option[Instant]
  ): Stream[ConnectionIO, BalanceHistory] = {
    val from = start.map(s => fr"AND time >= $s").getOrElse(Fragment.empty)
    val to   = end.map(e => fr"AND time <= $e").getOrElse(Fragment.empty)

    (
      sql"""SELECT account_id, balance, block_height, time
          FROM balance_history
          WHERE account_id = $accountId
          """ ++
        from ++
        to ++
        sql"""ORDER BY time ASC"""
    )
      .query[BalanceHistory]
      .stream
  }

  def getBalanceHistoryCount(
      accountId: UUID
  ): ConnectionIO[Int] =
    sql"""SELECT COUNT(balance)
          FROM balance_history
          WHERE account_id = $accountId
       """.query[Int].unique

  def removeBalancesHistoryFromCursor(accountId: UUID, blockHeight: Long): ConnectionIO[Int] =
    sql"""DELETE from balance_history
          WHERE account_id = $accountId
          AND block_height >= $blockHeight
       """.update.run

  def getLastBalance(accountId: UUID): ConnectionIO[Option[BalanceHistory]] =
    sql"""SELECT account_id, balance, block_height, time
          FROM balance_history
          WHERE account_id = $accountId
          ORDER BY block_height DESC
          LIMIT 1
       """.query[BalanceHistory].option

  def getLastBalanceBefore(accountId: UUID, time: Instant): ConnectionIO[Option[BalanceHistory]] =
    sql"""SELECT account_id, balance, block_height, time
          FROM balance_history
          WHERE account_id = $accountId
          AND time < $time
          ORDER BY block_height DESC
          LIMIT 1
       """.query[BalanceHistory].option
}
