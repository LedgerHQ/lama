package co.ledger.lama.bitcoin.common.models.interpreter

import java.time.Instant

import cats.data.NonEmptyList
import co.ledger.lama.bitcoin.interpreter.protobuf
import co.ledger.lama.common.models.implicits._
import co.ledger.lama.common.utils.TimestampProtoUtils
import io.circe.{Decoder, Encoder}
import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}

case class Utxo(
    transactionHash: String,
    outputIndex: Int,
    value: BigInt,
    address: String,
    scriptHex: String,
    changeType: Option[ChangeType],
    derivation: NonEmptyList[Int],
    time: Instant
) {
  def toProto: protobuf.Utxo =
    protobuf.Utxo(
      transactionHash,
      outputIndex,
      value.toString,
      address,
      scriptHex,
      changeType.getOrElse(ChangeType.External).toProto,
      derivation.toList,
      Some(TimestampProtoUtils.serialize(time))
    )
}

object Utxo {
  implicit val encoder: Encoder[Utxo] = deriveConfiguredEncoder[Utxo]
  implicit val decoder: Decoder[Utxo] = deriveConfiguredDecoder[Utxo]

  def fromProto(proto: protobuf.Utxo): Utxo =
    Utxo(
      proto.transactionHash,
      proto.outputIndex,
      BigInt(proto.value),
      proto.address,
      proto.scriptHex,
      Some(ChangeType.fromProto(proto.changeType)),
      NonEmptyList.fromListUnsafe(proto.derivation.toList),
      proto.time.map(TimestampProtoUtils.deserialize).getOrElse(Instant.now())
    )
}
