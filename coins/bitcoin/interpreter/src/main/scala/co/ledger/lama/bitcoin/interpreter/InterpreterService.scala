package co.ledger.lama.bitcoin.interpreter

import cats.effect.{ConcurrentEffect, IO}
import co.ledger.lama.bitcoin.common.models.interpreter._
import co.ledger.lama.common.logging.IOLogging
import co.ledger.lama.common.models._
import co.ledger.lama.common.utils.{TimestampProtoUtils, UuidUtils}
import io.grpc.{Metadata, ServerServiceDefinition}

trait InterpreterService extends protobuf.BitcoinInterpreterServiceFs2Grpc[IO, Metadata] {
  def definition(implicit ce: ConcurrentEffect[IO]): ServerServiceDefinition =
    protobuf.BitcoinInterpreterServiceFs2Grpc.bindService(this)
}

class InterpreterGrpcService(
    interpreter: Interpreter
) extends InterpreterService
    with IOLogging {

  def saveTransactions(
      request: protobuf.SaveTransactionsRequest,
      ctx: Metadata
  ): IO[protobuf.ResultCount] = {
    for {
      accountId  <- UuidUtils.bytesToUuidIO(request.accountId)
      _          <- log.info(s"Saving ${request.transactions.size} transactions for $accountId")
      txs        <- IO(request.transactions.map(TransactionView.fromProto).toList)
      savedCount <- interpreter.saveTransactions(accountId, txs)
    } yield protobuf.ResultCount(savedCount)
  }

  def getLastBlocks(
      request: protobuf.GetLastBlocksRequest,
      ctx: Metadata
  ): IO[protobuf.GetLastBlocksResult] = {
    for {
      accountId <- UuidUtils.bytesToUuidIO(request.accountId)
      _         <- log.info(s"""Getting blocks for account:
                               - accountId: $accountId
                               """)
      blocks    <- interpreter.getLastBlocks(accountId)
    } yield protobuf.GetLastBlocksResult(blocks.map(_.toProto))
  }

  def getOperations(
      request: protobuf.GetOperationsRequest,
      ctx: Metadata
  ): IO[protobuf.GetOperationsResult] = {
    for {
      accountId <- UuidUtils.bytesToUuidIO(request.accountId)
      sort = Sort.fromIsAsc(request.sort.isAsc)
      _ <- log.info(s"""Getting operations with parameters:
                  - accountId: $accountId
                  - blockHeight: ${request.blockHeight}
                  - limit: ${request.limit}
                  - offset: ${request.offset}
                  - sort: $sort""")
      opResult <- interpreter.getOperations(
        accountId,
        request.blockHeight,
        request.limit,
        request.offset,
        sort
      )
    } yield opResult.toProto
  }

  def getOperation(
      request: protobuf.GetOperationRequest,
      ctx: Metadata
  ): IO[protobuf.GetOperationResult] = {
    for {
      accountId   <- UuidUtils.bytesToUuidIO(request.accountId).map(Operation.AccountId)
      operationId <- IO.pure(Operation.UID(request.operationUid))
      operation   <- interpreter.getOperation(accountId, operationId)
    } yield protobuf.GetOperationResult(operation.map(_.toProto))
  }

  def getUtxos(request: protobuf.GetUtxosRequest, ctx: Metadata): IO[protobuf.GetUtxosResult] = {
    for {
      accountId <- UuidUtils.bytesToUuidIO(request.accountId)
      sort = Sort.fromIsAsc(request.sort.isAsc)
      _   <- log.info(s"""Getting UTXOs with parameters:
                               - accountId: $accountId
                               - limit: ${request.limit}
                               - offset: ${request.offset}
                               - sort: $sort""")
      res <- interpreter.getUtxos(accountId, request.limit, request.offset, sort)
    } yield {
      res.toProto
    }
  }

  def getUnconfirmedUtxos(
      request: protobuf.GetUnconfirmedUtxosRequest,
      ctx: Metadata
  ): IO[protobuf.GetUnconfirmedUtxosResult] =
    for {
      accountId        <- UuidUtils.bytesToUuidIO(request.accountId)
      _                <- log.info(s"""Getting UTXOs with parameters:
                         - accountId: $accountId""")
      unconfirmedUtxos <- interpreter.getUnconfirmedUtxos(accountId)
    } yield {
      protobuf.GetUnconfirmedUtxosResult(unconfirmedUtxos.map(_.toProto))
    }

  def removeDataFromCursor(
      request: protobuf.DeleteTransactionsRequest,
      ctx: Metadata
  ): IO[protobuf.ResultCount] = {
    for {
      accountId <- UuidUtils.bytesToUuidIO(request.accountId)
      blockHeight = request.blockHeight
      _     <- log.info(s"""Deleting data with parameters:
                      - accountId: $accountId
                      - blockHeight: $blockHeight""")
      txRes <- interpreter.removeDataFromCursor(accountId, blockHeight)
    } yield protobuf.ResultCount(txRes)
  }

  def compute(
      request: protobuf.ComputeRequest,
      ctx: Metadata
  ): IO[protobuf.ResultCount] =
    for {
      coin <- IO.fromOption(Coin.fromKey(request.coinId))(
        new IllegalArgumentException(s"Unknown coin type ${request.coinId}) in compute request")
      )
      accountId <- UuidUtils.bytesToUuidIO(request.accountId)
      addresses <- IO(request.addresses.map(AccountAddress.fromProto).toList)
      nbOps     <- interpreter.compute(accountId, addresses, coin)
    } yield protobuf.ResultCount(nbOps)

  def getBalance(
      request: protobuf.GetBalanceRequest,
      ctx: Metadata
  ): IO[protobuf.CurrentBalance] =
    for {
      accountId <- UuidUtils.bytesToUuidIO(request.accountId)
      info      <- interpreter.getBalance(accountId)
    } yield info.toProto

  def getBalanceHistory(
      request: protobuf.GetBalanceHistoryRequest,
      ctx: Metadata
  ): IO[protobuf.GetBalanceHistoryResult] =
    for {
      accountId <- UuidUtils.bytesToUuidIO(request.accountId)
      start = request.start.map(TimestampProtoUtils.deserialize)
      end   = request.end.map(TimestampProtoUtils.deserialize)

      _ <- log.info(s"""Getting balances with parameters:
                       - accountId: $accountId
                       - start: $start
                       - end: $end
                       - interval: ${request.interval}""")

      balances <- interpreter.getBalanceHistory(accountId, start, end, request.interval)
    } yield protobuf.GetBalanceHistoryResult(balances.map(_.toProto))

}
