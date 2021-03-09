package co.ledger.lama.bitcoin.interpreter

import cats.data.NonEmptyList
import cats.effect.IO
import co.ledger.lama.bitcoin.common.models.interpreter.{
  InputView,
  Operation,
  OutputView,
  TransactionView
}
import co.ledger.lama.bitcoin.interpreter.models.OperationToSave
import co.ledger.lama.bitcoin.interpreter.services.OperationQueries.Op
import co.ledger.lama.bitcoin.interpreter.services.{OperationQueries, TransactionQueries}
import co.ledger.lama.common.models.{Sort, TxHash}
import doobie.implicits._
import doobie.util.transactor.Transactor
import fs2.Chunk

import java.util.UUID

object QueryUtils {
  def fetchInputAndOutputs(
      db: Transactor[IO],
      accountId: UUID,
      txHash: TxHash
  ): IO[Option[(List[InputView], List[OutputView])]] = {
    OperationQueries
      .fetchInputsWithOutputsOrderedByTxHash(accountId, Sort.Descending, NonEmptyList.one(txHash))
      .transact(db)
      .map { case (_, inputWithOutputs) => inputWithOutputs }
      .compile
      .last
  }

  def fetchOpAndTx(
      db: Transactor[IO],
      accountId: Operation.AccountId,
      operationId: Operation.UID
  ): IO[Option[(Op, OperationQueries.Tx)]] =
    OperationQueries.findOperation(accountId, operationId).transact(db)

  def saveTx(db: Transactor[IO], transaction: TransactionView, accountId: UUID): IO[Unit] = {
    TransactionQueries
      .saveTransaction(transaction, accountId)
      .transact(db)
      .void
  }

  def saveUnconfirmedTxs(
      db: Transactor[IO],
      accountId: UUID,
      transactions: List[TransactionView]
  ): IO[Unit] = {
    TransactionQueries
      .saveUnconfirmedTransactions(accountId, transactions)
      .transact(db)
      .void
  }

  def saveUnconfirmedTxView(
      db: Transactor[IO],
      accountId: UUID,
      transactions: List[TransactionView]
  ): IO[Unit] = {
    OperationQueries
      .saveUnconfirmedTransactionView(accountId, transactions)
      .transact(db)
      .void
  }

  def fetchOps(db: Transactor[IO], accountId: UUID): IO[List[(Op, OperationQueries.Tx)]] = {
    OperationQueries
      .fetchOperations(accountId)
      .transact(db)
      .compile
      .toList
  }

  def saveOp(db: Transactor[IO], operation: OperationToSave): IO[Unit] = {
    OperationQueries
      .saveOperations(Chunk(operation))
      .transact(db)
      .void
  }

}
