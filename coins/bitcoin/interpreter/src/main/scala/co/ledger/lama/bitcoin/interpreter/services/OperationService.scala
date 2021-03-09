package co.ledger.lama.bitcoin.interpreter.services

import cats.data.OptionT

import java.util.UUID
import cats.effect.{ContextShift, IO}
import co.ledger.lama.bitcoin.common.models.interpreter.{
  GetOperationsResult,
  GetUtxosResult,
  Operation,
  Utxo
}
import co.ledger.lama.bitcoin.interpreter.models.{OperationToSave, TransactionAmounts}
import co.ledger.lama.common.logging.IOLogging
import co.ledger.lama.common.models.Sort
import doobie._
import doobie.implicits._
import fs2._

class OperationService(
    db: Transactor[IO],
    maxConcurrent: Int
) extends IOLogging {

  def getOperations(
      accountId: UUID,
      blockHeight: Long,
      limit: Int,
      offset: Int,
      sort: Sort
  )(implicit cs: ContextShift[IO]): IO[GetOperationsResult] =
    for {
      opsWithTx <- OperationQueries
        .fetchOperations(accountId, blockHeight, sort, Some(limit + 1), Some(offset))
        .transact(db)
        .parEvalMap(maxConcurrent) { op =>
          OperationQueries
            .fetchTransaction(op.accountId, op.hash)
            .transact(db)
            .map(tx => op.copy(transaction = tx))
        }
        .compile
        .toList

      total <- OperationQueries.countOperations(accountId, blockHeight).transact(db)

      // we get 1 more than necessary to know if there's more, then we return the correct number
      truncated = opsWithTx.size > limit

    } yield {
      val operations = opsWithTx.slice(0, limit)
      GetOperationsResult(operations, total, truncated)
    }

  def getOperation(
      accountId: Operation.AccountId,
      operationId: Operation.UID
  )(implicit cs: ContextShift[IO]): IO[Option[Operation]] = {

    val op = for {
      operation <- OptionT(OperationQueries.findOperation(accountId, operationId))
      tx        <- OptionT(OperationQueries.fetchTransaction(accountId.value, operation.hash))
    } yield operation.copy(transaction = Some(tx))

    op.value.transact(db)
  }

  def deleteUnconfirmedOperations(accountId: UUID): IO[Int] =
    OperationQueries
      .deleteUnconfirmedOperations(accountId)
      .transact(db)

  def getUtxos(
      accountId: UUID,
      sort: Sort,
      limit: Int,
      offset: Int
  ): IO[GetUtxosResult] =
    for {
      utxos <- OperationQueries
        .fetchUTXOs(accountId, sort, Some(limit + 1), Some(offset))
        .transact(db)
        .compile
        .toList

      total <- OperationQueries.countUTXOs(accountId).transact(db)

    } yield {
      // We get 1 more than necessary to know if there's more, then we return the correct number
      GetUtxosResult(utxos.slice(0, limit), total, truncated = utxos.size > limit)
    }

  def getUnconfirmedUtxos(accountId: UUID): IO[List[Utxo]] =
    OperationQueries
      .fetchUnconfirmedUTXOs(accountId)
      .transact(db)
      .compile
      .toList

  def removeFromCursor(accountId: UUID, blockHeight: Long): IO[Int] =
    OperationQueries.removeFromCursor(accountId, blockHeight).transact(db)

  def compute(accountId: UUID): Stream[IO, OperationToSave] =
    operationSource(accountId)
      .flatMap(op => Stream.chunk(op.computeOperations))

  private def operationSource(accountId: UUID): Stream[IO, TransactionAmounts] =
    OperationQueries
      .fetchTransactionAmounts(accountId)
      .transact(db)

  def saveOperationSink(implicit cs: ContextShift[IO]): Pipe[IO, OperationToSave, OperationToSave] =
    in =>
      in.chunkN(1000) // TODO : in conf
        .prefetch
        .parEvalMapUnordered(maxConcurrent) { batch =>
          OperationQueries.saveOperations(batch).transact(db).map(_ => batch)
        }
        .flatMap(Stream.chunk)

}
