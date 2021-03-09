package co.ledger.lama.manager.models

import java.time.Instant
import java.util.UUID

import co.ledger.lama.common.models.{Coin, CoinFamily, ReportError, Status}
import io.circe.JsonObject

case class AccountSyncStatus(
    id: UUID,
    key: String,
    coinFamily: CoinFamily,
    coin: Coin,
    syncFrequency: Long,
    label: Option[String],
    group: String,
    syncId: UUID,
    status: Status,
    cursor: Option[JsonObject],
    error: Option[ReportError],
    updated: Instant
)
