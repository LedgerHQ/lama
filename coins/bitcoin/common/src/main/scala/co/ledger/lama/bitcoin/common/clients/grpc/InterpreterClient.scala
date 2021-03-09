package co.ledger.lama.bitcoin.common.clients.grpc

import java.time.Instant
import java.util.UUID
import cats.effect.{ContextShift, IO}
import co.ledger.lama.bitcoin.common.models.interpreter._
import co.ledger.lama.bitcoin.common.utils.BtcProtoUtils._
import co.ledger.lama.bitcoin.interpreter.protobuf
import co.ledger.lama.common.clients.grpc.GrpcClient
import co.ledger.lama.common.models.{Coin, Sort}
import co.ledger.lama.common.utils.{TimestampProtoUtils, UuidUtils}
import io.grpc.{ManagedChannel, Metadata}

trait InterpreterClient {
  def saveTransactions(accountId: UUID, txs: List[TransactionView]): IO[Int]

  def removeDataFromCursor(accountId: UUID, blockHeightCursor: Option[Long]): IO[Int]

  def getLastBlocks(accountId: UUID): IO[List[BlockView]]

  def compute(
      accountId: UUID,
      coin: Coin,
      addresses: List[AccountAddress],
      lastBlockHeight: Option[Long]
  ): IO[Int]

  def getOperations(
      accountId: UUID,
      blockHeight: Long,
      limit: Int,
      offset: Int,
      sort: Option[Sort]
  ): IO[GetOperationsResult]

  def getOperation(
      accountId: UUID,
      operationId: String
  ): IO[Option[Operation]]

  def getUtxos(
      accountId: UUID,
      limit: Int,
      offset: Int,
      sort: Option[Sort]
  ): IO[GetUtxosResult]

  def getUnconfirmedUtxos(
      accountId: UUID
  ): IO[List[Utxo]]

  def getBalance(accountId: UUID): IO[CurrentBalance]

  def getBalanceHistory(
      accountId: UUID,
      start: Option[Instant],
      end: Option[Instant],
      interval: Option[Int]
  ): IO[GetBalanceHistoryResult]

}

class InterpreterGrpcClient(
    val managedChannel: ManagedChannel
)(implicit val cs: ContextShift[IO])
    extends InterpreterClient {

  val client: protobuf.BitcoinInterpreterServiceFs2Grpc[IO, Metadata] =
    GrpcClient.resolveClient(
      protobuf.BitcoinInterpreterServiceFs2Grpc.stub[IO],
      managedChannel,
      "InterpreterClient"
    )

  def saveTransactions(accountId: UUID, txs: List[TransactionView]): IO[Int] =
    client
      .saveTransactions(
        protobuf.SaveTransactionsRequest(
          accountId = UuidUtils uuidToBytes accountId,
          transactions = txs.map(_.toProto)
        ),
        new Metadata()
      )
      .map(_.count)

  def removeDataFromCursor(accountId: UUID, blockHeightCursor: Option[Long]): IO[Int] =
    client
      .removeDataFromCursor(
        protobuf.DeleteTransactionsRequest(
          UuidUtils uuidToBytes accountId,
          blockHeightCursor.getOrElse(0)
        ),
        new Metadata()
      )
      .map(_.count)

  def getLastBlocks(accountId: UUID): IO[List[BlockView]] =
    client
      .getLastBlocks(
        protobuf.GetLastBlocksRequest(
          UuidUtils uuidToBytes accountId
        ),
        new Metadata()
      )
      .map(_.blocks.map(BlockView.fromProto).toList)

  def compute(
      accountId: UUID,
      coin: Coin,
      addresses: List[AccountAddress],
      lastBlockHeight: Option[Long]
  ): IO[Int] =
    client
      .compute(
        protobuf.ComputeRequest(
          UuidUtils.uuidToBytes(accountId),
          addresses.map(_.toProto),
          coin.name,
          lastBlockHeight.getOrElse(0L)
        ),
        new Metadata()
      )
      .map(_.count)

  def getOperations(
      accountId: UUID,
      blockHeight: Long,
      limit: Int,
      offset: Int,
      sort: Option[Sort]
  ): IO[GetOperationsResult] =
    client
      .getOperations(
        protobuf.GetOperationsRequest(
          accountId = UuidUtils.uuidToBytes(accountId),
          blockHeight = blockHeight,
          limit = limit,
          offset = offset,
          sort = sort.map(_.toProto).getOrElse(protobuf.SortingOrder.DESC)
        ),
        new Metadata
      )
      .map(GetOperationsResult.fromProto)

  def getOperation(
      accountId: UUID,
      operationId: String
  ): IO[Option[Operation]] =
    client
      .getOperation(
        protobuf.GetOperationRequest(UuidUtils.uuidToBytes(accountId), operationId),
        new Metadata
      )
      .map(_.operation.map(Operation.fromProto))

  def getUtxos(
      accountId: UUID,
      limit: Int,
      offset: Int,
      sort: Option[Sort]
  ): IO[GetUtxosResult] = {
    client
      .getUtxos(
        protobuf.GetUtxosRequest(
          accountId = UuidUtils.uuidToBytes(accountId),
          limit = limit,
          offset = offset,
          sort = sort.map(_.toProto).getOrElse(protobuf.SortingOrder.DESC)
        ),
        new Metadata
      )
      .map(GetUtxosResult.fromProto)
  }

  def getUnconfirmedUtxos(
      accountId: UUID
  ): IO[List[Utxo]] = {
    client
      .getUnconfirmedUtxos(
        protobuf.GetUnconfirmedUtxosRequest(
          accountId = UuidUtils.uuidToBytes(accountId)
        ),
        new Metadata
      )
      .map(_.utxos.map(Utxo.fromProto).toList)
  }

  def getBalance(accountId: UUID): IO[CurrentBalance] = {
    client
      .getBalance(
        protobuf.GetBalanceRequest(
          accountId = UuidUtils.uuidToBytes(accountId)
        ),
        new Metadata
      )
      .map(CurrentBalance.fromProto)
  }

  def getBalanceHistory(
      accountId: UUID,
      start: Option[Instant],
      end: Option[Instant],
      interval: Option[Int]
  ): IO[GetBalanceHistoryResult] = {
    client
      .getBalanceHistory(
        protobuf.GetBalanceHistoryRequest(
          accountId = UuidUtils.uuidToBytes(accountId),
          start.map(TimestampProtoUtils.serialize),
          end.map(TimestampProtoUtils.serialize),
          interval.getOrElse(0)
        ),
        new Metadata
      )
      .map(GetBalanceHistoryResult.fromProto)
  }

}
