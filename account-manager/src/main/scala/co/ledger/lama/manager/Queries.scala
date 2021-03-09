package co.ledger.lama.manager

import java.util.UUID

import cats.data.NonEmptyList
import co.ledger.lama.common.models.messages.WorkerMessage
import co.ledger.lama.common.models.{
  AccountIdentifier,
  AccountInfo,
  Coin,
  CoinFamily,
  Sort,
  SyncEvent,
  TriggerableEvent,
  TriggerableStatus
}
import co.ledger.lama.manager.models._
import co.ledger.lama.manager.models.implicits._
import doobie.{Fragment, Fragments}
import doobie.free.connection.ConnectionIO
import doobie.implicits._
import doobie.postgres.implicits._
import fs2.Stream
import io.circe.JsonObject
import io.circe.syntax.EncoderOps
import org.postgresql.util.PGInterval

object Queries {

  def countAccounts(): ConnectionIO[Int] =
    sql"""SELECT COUNT(*) FROM account_sync_status"""
      .query[Int]
      .unique

  def fetchPublishableWorkerMessages(
      coinFamily: CoinFamily,
      coin: Coin
  ): Stream[ConnectionIO, WorkerMessage[JsonObject]] =
    sql"""SELECT "key", coin_family, coin, account_id, sync_id, status, "cursor", "error", updated
          FROM workable_event
          WHERE coin_family = $coinFamily
          AND coin = $coin
       """
      .query[WorkerMessage[JsonObject]]
      .stream

  def fetchTriggerableEvents(
      coinFamily: CoinFamily,
      coin: Coin
  ): Stream[ConnectionIO, TriggerableEvent[JsonObject]] =
    (
      sql"""SELECT account_id, sync_id, status, "cursor", "error", updated
            FROM account_sync_status
            WHERE updated + sync_frequency < CURRENT_TIMESTAMP
            AND coin_family = $coinFamily
            AND coin = $coin
            AND """
        ++ Fragments.in(
          fr"status",
          NonEmptyList.fromListUnsafe(TriggerableStatus.all.values.toList)
        )
    ).query[TriggerableEvent[JsonObject]].stream

  def getAccounts(group: Option[String], offset: Int, limit: Int): Stream[ConnectionIO, AccountSyncStatus] = {
    val groupFilter = group.map(name => fr"group_col = $name")
    val q: Fragment = fr"""SELECT account_id, "key", coin_family, coin,
            extract(epoch FROM sync_frequency) / 60 * 60, label, group_col
            sync_id, status, "cursor", error, updated
          FROM account_sync_status
          """ ++
          Fragments.whereAndOpt(groupFilter) ++
          fr"""LIMIT $limit""" ++
          fr"""OFFSET $offset"""
      q.query[AccountSyncStatus]
      .stream
  }

  def getAccountInfo(accountId: UUID): ConnectionIO[Option[AccountInfo]] =
    sql"""SELECT account_id, "key", coin_family, coin, extract(epoch FROM sync_frequency)/60*60, label, group_col
          FROM account_info
          WHERE account_id = $accountId
         """
      .query[AccountInfo]
      .option

  def updateAccountSyncFrequency(
      accountId: UUID,
      syncFrequency: Long
  ): ConnectionIO[Int] = {
    val syncFrequencyInterval = new PGInterval()
    syncFrequencyInterval.setSeconds(syncFrequency.toDouble)

    sql"""UPDATE account_info SET sync_frequency=$syncFrequencyInterval WHERE account_id = $accountId""".update.run
  }

  def updateAccountLabel(
      accountId: UUID,
      label: String
  ): ConnectionIO[Int] = {

    sql"""UPDATE account_info SET label=$label WHERE account_id = $accountId
          """.update.run
  }

  def insertAccountInfo(
      accountIdentifier: AccountIdentifier,
      label: Option[String],
      syncFrequency: Long
  ): ConnectionIO[AccountInfo] = {
    val accountId  = accountIdentifier.id
    val key        = accountIdentifier.key
    val coinFamily = accountIdentifier.coinFamily
    val coin       = accountIdentifier.coin
    val group      = accountIdentifier.group

    val syncFrequencyInterval = new PGInterval()
    syncFrequencyInterval.setSeconds(syncFrequency.toDouble)

    sql"""INSERT INTO account_info(account_id, "key", coin_family, coin, sync_frequency, label, group_col)
          VALUES($accountId, $key, $coinFamily, $coin, $syncFrequencyInterval, $label, $group)
          RETURNING account_id, key, coin_family, coin, extract(epoch FROM sync_frequency)/60*60, label, group_col
          """
      .query[AccountInfo]
      .unique
  }

  def insertSyncEvent(e: SyncEvent[JsonObject]): ConnectionIO[Int] =
    sql"""INSERT INTO account_sync_event(account_id, sync_id, status, "cursor", "error")
          VALUES(${e.accountId}, ${e.syncId}, ${e.status}, ${e.cursor}, ${e.error.asJson.asObject})
          """.update.run

  def getLastSyncEvent(accountId: UUID): ConnectionIO[Option[SyncEvent[JsonObject]]] =
    sql"""SELECT account_id, sync_id, status, "cursor", "error", updated
          FROM account_sync_status
          WHERE account_id = $accountId
          """
      .query[SyncEvent[JsonObject]]
      .option

  def getSyncEvents(
      accountId: UUID,
      sort: Sort,
      limit: Option[Int] = None,
      offset: Option[Int] = None
  ): Stream[ConnectionIO, SyncEvent[JsonObject]] = {
    val orderF  = Fragment.const(s"ORDER BY updated $sort")
    val limitF  = limit.map(l => fr"LIMIT $l").getOrElse(Fragment.empty)
    val offsetF = offset.map(o => fr"OFFSET $o").getOrElse(Fragment.empty)

    val query =
      sql"""SELECT
              account_id,
              sync_id,
              status,
              "cursor",
              "error",
              updated
            FROM account_info JOIN account_sync_event USING (account_id)
            WHERE account_id = $accountId
            """ ++ orderF ++ limitF ++ offsetF

    query
      .query[SyncEvent[JsonObject]]
      .stream
  }

  def countSyncEvents(accountId: UUID): ConnectionIO[Int] =
    sql"""SELECT COUNT(*) FROM account_sync_event WHERE account_id = $accountId"""
      .query[Int]
      .unique
}
