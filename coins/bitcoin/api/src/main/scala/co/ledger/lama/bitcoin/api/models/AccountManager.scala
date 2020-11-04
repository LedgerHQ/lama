package co.ledger.lama.bitcoin.api.models

import java.util.UUID

import co.ledger.lama.common.models.implicits._
import io.circe.Decoder
import io.circe.generic.extras.semiauto._
import co.ledger.lama.bitcoin.common.models.service.{Operation, Utxo}
import co.ledger.lama.common.models.SyncEvent

case class GetOperationsResult(
    truncated: Boolean,
    operations: Seq[Operation],
    size: Int
)

object GetOperationsResult {
  implicit val getOperationsResultDecoder: Decoder[GetOperationsResult] =
    deriveConfiguredDecoder[GetOperationsResult]
}

case class GetUTXOsResult(
    truncated: Boolean,
    utxos: Seq[Utxo],
    size: Int
)

object GetUTXOsResult {
  implicit val getUTXOsResultDecoder: Decoder[GetUTXOsResult] =
    deriveConfiguredDecoder[GetUTXOsResult]
}

case class AccountInfo(
    accountId: UUID,
    syncFrequency: Long,
    lastSyncEvent: Option[SyncEvent],
    balance: BigInt,
    utxos: Int,
    received: BigInt,
    sent: BigInt
)

object AccountInfo {
  implicit val getAccountInfoDecoder: Decoder[AccountInfo] =
    deriveConfiguredDecoder[AccountInfo]
}

case class AccountRegistered(accountId: UUID, syncId: UUID, syncFrequency: Long)

object AccountRegistered {
  implicit val accountRegisteredDecoder: Decoder[AccountRegistered] =
    deriveConfiguredDecoder[AccountRegistered]
}
