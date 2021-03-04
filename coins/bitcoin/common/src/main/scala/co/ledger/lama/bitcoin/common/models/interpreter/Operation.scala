package co.ledger.lama.bitcoin.common.models.interpreter

import co.ledger.lama.bitcoin.interpreter.protobuf
import co.ledger.lama.common.models.implicits._
import co.ledger.lama.common.utils.{TimestampProtoUtils, UuidUtils}
import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import io.circe.{Decoder, Encoder}
import java.security.MessageDigest
import java.time.Instant
import java.util.UUID

case class Operation(
    uid: Operation.UID,
    accountId: UUID,
    hash: String,
    transaction: TransactionView,
    operationType: OperationType,
    amount: BigInt,
    fees: BigInt,
    time: Instant,
    blockHeight: Option[Long]
) {
  def toProto: protobuf.Operation = {
    protobuf.Operation(
      UuidUtils.uuidToBytes(accountId),
      hash,
      Some(transaction.toProto),
      operationType.toProto,
      amount.toString,
      fees.toString,
      Some(TimestampProtoUtils.serialize(time)),
      blockHeight.getOrElse(-1L),
      uid = uid.hex
    )
  }
}

object Operation {
  case class UID(hex: String)       extends AnyVal
  case class AccountId(value: UUID) extends AnyVal
  case class TxId(value: String)    extends AnyVal

  object UID {
    implicit val encoder: Encoder[UID] = Encoder[String].contramap(_.hex)
    implicit val decoder: Decoder[UID] = Decoder[String].map(UID(_))
  }

  implicit val decoder: Decoder[Operation] = deriveConfiguredDecoder[Operation]
  implicit val encoder: Encoder[Operation] = deriveConfiguredEncoder[Operation]

  def fromProto(proto: protobuf.Operation): Operation = {
    Operation(
      UID(proto.uid),
      UuidUtils.unsafeBytesToUuid(proto.accountId),
      proto.hash,
      proto.transaction.map(TransactionView.fromProto).get, // An operation has a transaction
      OperationType.fromProto(proto.operationType),
      BigInt(proto.amount),
      BigInt(proto.fees),
      proto.time.map(TimestampProtoUtils.deserialize).getOrElse(Instant.now),
      if (proto.blockHeight >= 0) Some(proto.blockHeight) else None
    )
  }

  def uid(accountId: AccountId, txId: TxId, operationType: OperationType): UID = {

    val libcoreType = operationType match {
      case OperationType.Send    => "SEND"
      case OperationType.Receive => "RECEIVE"
    }

    val rawUid = s"uid:${accountId.value.toString.toLowerCase}+${txId.value}+$libcoreType"

    UID(
      MessageDigest
        .getInstance("SHA-256")
        .digest(rawUid.getBytes("UTF-8"))
        .map("%02x".format(_))
        .mkString
    )
  }

}
