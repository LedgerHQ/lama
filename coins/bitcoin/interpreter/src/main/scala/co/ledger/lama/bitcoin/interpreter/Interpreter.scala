package co.ledger.lama.bitcoin.interpreter

import java.time.Instant

import cats.effect.{ConcurrentEffect, ContextShift, IO}
import co.ledger.lama.bitcoin.common.models.explorer._
import co.ledger.lama.bitcoin.common.models.service._
import co.ledger.lama.bitcoin.interpreter.protobuf.{
  ComputeRequest,
  GetBalanceHistoryRequest,
  GetBalanceHistoryResult,
  ResultCount
}
import co.ledger.lama.bitcoin.interpreter.services.{
  BalanceService,
  FlaggingService,
  OperationService,
  TransactionService
}
import co.ledger.lama.common.logging.IOLogging
import co.ledger.lama.common.models.Sort
import co.ledger.lama.common.utils.{ProtobufUtils, UuidUtils}
import doobie.Transactor
import io.grpc.{Metadata, ServerServiceDefinition}

trait Interpreter extends protobuf.BitcoinInterpreterServiceFs2Grpc[IO, Metadata] {
  def definition(implicit ce: ConcurrentEffect[IO]): ServerServiceDefinition =
    protobuf.BitcoinInterpreterServiceFs2Grpc.bindService(this)
}

class DbInterpreter(
    db: Transactor[IO],
    maxConcurrent: Int
)(implicit cs: ContextShift[IO])
    extends Interpreter
    with IOLogging {

  val transactionService = new TransactionService(db, maxConcurrent)
  val operationService   = new OperationService(db, maxConcurrent)
  val flaggingService    = new FlaggingService(db)
  val balanceService     = new BalanceService(db)

  def saveTransactions(
      request: protobuf.SaveTransactionsRequest,
      ctx: Metadata
  ): IO[protobuf.ResultCount] = {
    for {
      accountId  <- UuidUtils.bytesToUuidIO(request.accountId)
      _          <- log.info(s"Saving transactions for $accountId")
      txs        <- IO(request.transactions.map(ConfirmedTransaction.fromProto).toList)
      savedCount <- transactionService.saveTransactions(accountId, txs)
    } yield ResultCount(savedCount)
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
      blocks <-
        transactionService
          .getLastBlocks(accountId)
          .map(_.toProto)
          .compile
          .toList
    } yield protobuf.GetLastBlocksResult(blocks)
  }

  def getOperations(
      request: protobuf.GetOperationsRequest,
      ctx: Metadata
  ): IO[protobuf.GetOperationsResult] = {
    val blockHeight = request.blockHeight
    val limit       = if (request.limit <= 0) 20 else request.limit
    val offset      = if (request.offset < 0) 0 else request.offset
    val sort        = Sort.fromIsAsc(request.sort.isAsc)

    for {
      accountId <- UuidUtils.bytesToUuidIO(request.accountId)
      _         <- log.info(s"""Getting operations with parameters:
                  |- accountId: $accountId
                  |- blockHeight: $blockHeight
                  |- limit: $limit
                  |- offset: $offset
                  |- sort: $sort""".stripMargin)
      opResult  <- operationService.getOperations(accountId, blockHeight, limit, offset, sort)
      (operations, truncated) = opResult
    } yield protobuf.GetOperationsResult(operations.map(_.toProto), truncated)
  }

  def getUTXOs(request: protobuf.GetUTXOsRequest, ctx: Metadata): IO[protobuf.GetUTXOsResult] = {
    val limit  = if (request.limit <= 0) 20 else request.limit
    val offset = if (request.offset < 0) 0 else request.offset
    val sort   = Sort.fromIsAsc(request.sort.isAsc)

    for {
      accountId <- UuidUtils.bytesToUuidIO(request.accountId)
      _         <- log.info(s"""Getting UTXOs with parameters:
                               |- accountId: $accountId
                               |- limit: $limit
                               |- offset: $offset
                               |- sort: $sort""".stripMargin)
      res       <- operationService.getUTXOs(accountId, sort, limit, offset)
      (utxos, truncated) = res
    } yield {
      protobuf.GetUTXOsResult(utxos.map(_.toProto), truncated)
    }
  }

  def removeDataFromCursor(
      request: protobuf.DeleteTransactionsRequest,
      ctx: Metadata
  ): IO[protobuf.ResultCount] = {
    for {
      accountId <- UuidUtils.bytesToUuidIO(request.accountId)
      blockHeight = request.blockHeight
      _     <- log.info(s"""Deleting data with parameters:
                      |- accountId: $accountId
                      |- blockHeight: $blockHeight""".stripMargin)
      txRes <- transactionService.removeFromCursor(accountId, blockHeight)
      _     <- log.info(s"Deleted $txRes transactions")
      balancesRes <- balanceService.removeBalancesHistoryFromCursor(
        accountId,
        blockHeight
      )
      _ <- log.info(s"Deleted $balancesRes balances history")
    } yield ResultCount(txRes)
  }

  def compute(
      request: ComputeRequest,
      ctx: Metadata
  ): IO[protobuf.ResultCount] =
    for {
      accountId <- UuidUtils.bytesToUuidIO(request.accountId)

      addresses <- IO(request.addresses.map(AccountAddress.fromProto).toList)

      _ <- log.info(s"Flagging inputs and outputs belong to account=$accountId")
      _ <- flaggingService.flagInputsAndOutputs(accountId, addresses)

      _        <- log.info("Computing operations")
      savedOps <- operationService.compute(accountId)

      _ <- log.info("Computing balance history")
      _ <- balanceService.compute(accountId)

    } yield ResultCount(savedOps)

  def getBalance(
      request: protobuf.GetBalanceRequest,
      ctx: Metadata
  ): IO[protobuf.BalanceHistory] =
    for {
      accountId <- UuidUtils.bytesToUuidIO(request.accountId)
      info      <- balanceService.getBalance(accountId)
    } yield info.toProto

  def getBalanceHistory(
      request: GetBalanceHistoryRequest,
      ctx: Metadata
  ): IO[GetBalanceHistoryResult] =
    for {
      accountId <- UuidUtils.bytesToUuidIO(request.accountId)

      start = request.start
        .map(ProtobufUtils.toInstant)
        .getOrElse(Instant.parse("2019-04-04T10:03:22Z").minusSeconds(86400))

      end = request.end
        .map(ProtobufUtils.toInstant)
        .getOrElse(Instant.parse("2019-04-04T10:03:22Z").plusSeconds(86400))

      _ <- log.info(s"""Getting balances with parameters:
                       |- accountId: $accountId
                       |- start: $start
                       |- offset: $end""".stripMargin)

      balances <- balanceService.getBalancesHistory(accountId, start, end)
    } yield GetBalanceHistoryResult(balances.map(_.toProto))
}
