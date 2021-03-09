package co.ledger.lama.manager.models

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID

import co.ledger.lama.common.models._
import co.ledger.lama.common.models.implicits._
import co.ledger.lama.common.models.messages.WorkerMessage
import doobie.util.meta.Meta
import doobie.postgres.implicits._
import doobie.implicits.javasql._
import doobie.util.{Get, Put, Read}
import io.circe.{Decoder, Encoder, Json, JsonObject}
import io.circe.syntax._

object implicits {

  implicit val uuidEncoder: Encoder[UUID] = Encoder.encodeString.contramap(_.toString)
  implicit val uuidDecoder: Decoder[UUID] = Decoder.decodeString.map(UUID.fromString)

  implicit val instantType: Meta[Instant] =
    TimestampMeta.imap[Instant] { ts =>
      Instant.ofEpochMilli(ts.getTime)
    }(Timestamp.from)

  implicit val jsonObjectGet: Get[Option[JsonObject]] =
    jsonMeta.get.map(_.asObject)

  implicit val jsonObjectPut: Put[Option[JsonObject]] =
    jsonMeta.put.contramap(_.map(Json.fromJsonObject).getOrElse(Json.Null))

  implicit val meta: Meta[Status] =
    pgEnumStringOpt("sync_status", Status.fromKey, _.name)

  implicit val workableStatusMeta: Meta[WorkableStatus] =
    pgEnumStringOpt("sync_status", WorkableStatus.fromKey, _.name)

  implicit val reportableStatusMeta: Meta[ReportableStatus] =
    pgEnumStringOpt("sync_status", ReportableStatus.fromKey, _.name)

  implicit val flaggedStatusMeta: Meta[FlaggedStatus] =
    pgEnumStringOpt("sync_status", FlaggedStatus.fromKey, _.name)

  implicit val triggerableStatusMeta: Meta[TriggerableStatus] =
    pgEnumStringOpt("sync_status", TriggerableStatus.fromKey, _.name)

  implicit val coinMeta: Meta[Coin] =
    pgEnumStringOpt("coin", Coin.fromKey, _.name)

  implicit val coinFamilyMeta: Meta[CoinFamily] =
    pgEnumStringOpt("coin_family", CoinFamily.fromKey, _.name)

  implicit lazy val syncEventRead: Read[SyncEvent[JsonObject]] =
    Read[
      (UUID, UUID, Status, Option[JsonObject], Option[JsonObject], Instant)
    ].map { case (accountId, syncId, status, cursor, error, updated) =>
      SyncEvent(
        accountId,
        syncId,
        status,
        cursor,
        error.flatMap(_.asJson.as[ReportError].toOption),
        updated
      )
    }

  implicit lazy val triggerableEventRead: Read[TriggerableEvent[JsonObject]] =
    Read[
      (UUID, UUID, TriggerableStatus, Option[JsonObject], Option[JsonObject], Instant)
    ].map { case (accountId, syncId, status, cursor, error, updated) =>
      TriggerableEvent(
        accountId,
        syncId,
        status,
        cursor,
        error.flatMap(_.asJson.as[ReportError].toOption),
        updated
      )
    }

  implicit lazy val workerMessageRead: Read[WorkerMessage[JsonObject]] =
    Read[
      (
          String,
          CoinFamily,
          Coin,
          String,
          UUID,
          UUID,
          WorkableStatus,
          Option[JsonObject],
          Option[JsonObject],
          Instant
      )
    ].map { case (key, coinFamily, coin, group, accountId, syncId, status, cursor, error, updated) =>
      WorkerMessage(
        account = AccountIdentifier(key, coinFamily, coin, group),
        event = WorkableEvent(
          accountId,
          syncId,
          status,
          cursor,
          error.flatMap(_.asJson.as[ReportError].toOption),
          updated
        )
      )
    }

  implicit val accountInfoRead: Read[AccountInfo] =
    Read[(UUID, String, CoinFamily, Coin, Long, Option[String], String)].map {
      case (accountId, key, coinFamily, coin, syncFrequency, label, group) =>
        AccountInfo(
          accountId,
          key,
          coinFamily,
          coin,
          syncFrequency,
          None,
          label,
          group
        )
    }

  implicit val doobieRead: Read[AccountSyncStatus] =
    Read[(AccountInfo, UUID, Status, Option[JsonObject], Option[ReportError], Instant)].map {
      case (
            accountInfo,
            syncId,
            status,
            cursor,
            error,
            updated
          ) =>
        AccountSyncStatus(
          accountInfo.id,
          accountInfo.key,
          accountInfo.coinFamily,
          accountInfo.coin,
          accountInfo.syncFrequency,
          accountInfo.label,
          accountInfo.group,
          syncId,
          status,
          cursor,
          error,
          updated
        )
    }
}
