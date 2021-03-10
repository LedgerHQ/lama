package co.ledger.lama.bitcoin.worker.services

import cats.Foldable
import cats.effect.{ContextShift, IO, Timer}
import cats.kernel.Monoid
import co.ledger.lama.bitcoin.common.clients.grpc.InterpreterClient
import co.ledger.lama.bitcoin.common.clients.http.ExplorerClient
import co.ledger.lama.bitcoin.common.models.explorer.{
  Block,
  ConfirmedTransaction,
  DefaultInput,
  Transaction,
  UnconfirmedTransaction
}
import co.ledger.lama.bitcoin.common.models.interpreter.AccountAddress
import co.ledger.lama.bitcoin.worker.services.Bookkeeper.BatchResult
import co.ledger.lama.bitcoin.worker.services.Keychain.KeychainId
import co.ledger.lama.common.models.Coin
import fs2.{Pipe, Stream}
import java.util.UUID

trait Bookkeeper[F[_]] {
  def record[Tx <: Transaction: Bookkeeper.Recordable](
      coin: Coin,
      accountId: UUID,
      keychainId: UUID,
      blockHash: Option[Bookkeeper.BlockHash]
  ): F[BatchResult]
}

object Bookkeeper {
  type AccountId = UUID
  type Address   = String
  type BlockHash = String

  def apply(
      keychain: Keychain,
      explorerClient: Coin => ExplorerClient,
      interpreterClient: InterpreterClient,
      maxTxsToSavePerBatch: Int,
      maxConcurrent: Int
  )(implicit cs: ContextShift[IO]): Bookkeeper[IO] = new Bookkeeper[IO] {
    override def record[Tx <: Transaction: Recordable](
        coin: Coin,
        accountId: AccountId,
        keychainId: AccountId,
        blockHash: Option[BlockHash]
    ): IO[BatchResult] =
      keychain
        .addresses(keychainId)
        .flatMap { addresses =>
          Stream
            .emit(addresses)
            .through(Bookkeeper.fetchTransactionRecords(explorerClient(coin), blockHash))
            .through(
              Bookkeeper
                .saveTransactionRecords(
                  interpreterClient,
                  maxTxsToSavePerBatch,
                  maxConcurrent,
                  accountId
                )
            )
            .foldMonoid
            .through(Bookkeeper.markAddresses(keychain, keychainId))
        }
        .takeWhile(_.addresses.nonEmpty)
        .foldMonoid
        .compile
        .toList
        .map(Foldable[List].fold[BatchResult])
  }

  case class TransactionRecord[Tx <: Transaction: Recordable](
      tx: Tx,
      usedAddresses: List[AccountAddress]
  )

  def fetchTransactionRecords[Tx <: Transaction](
      explorer: ExplorerClient,
      blockHash: Option[BlockHash]
  )(implicit
      cs: ContextShift[IO],
      recordable: Recordable[Tx]
  ): Pipe[IO, List[AccountAddress], TransactionRecord[Tx]] =
    _.prefetch
      .flatMap { addresses =>
        recordable
          .fetch(explorer)(addresses.map(_.accountAddress).toSet, blockHash)
          .map(tx => TransactionRecord(tx, addressesUsed(addresses)(tx)))
      }

  def saveTransactionRecords[Tx <: Transaction: Recordable](
      interpreter: InterpreterClient,
      maxSize: Int,
      maxConcurrent: Int,
      accountId: AccountId
  )(implicit
      cs: ContextShift[IO],
      recordable: Recordable[Tx]
  ): Pipe[IO, TransactionRecord[Tx], BatchResult] =
    _.chunkN(maxSize)
      .parEvalMapUnordered(maxConcurrent) { trs =>
        val txs           = trs.map(_.tx).toList
        val usedAddresses = trs.map(_.usedAddresses).toList.flatten

        val maxBlock = txs.collect { case ConfirmedTransaction(_, _, _, _, _, _, _, block, _) =>
          block
        }.maxOption

        recordable
          .save(interpreter)(accountId, txs)
          .as(
            BatchResult(
              usedAddresses,
              maxBlock
            )
          )
      }

  def addressUsedBy(tx: Transaction)(accountAddress: AccountAddress): Boolean = {
    tx.inputs
      .collect { case i: DefaultInput => i.address }
      .contains(accountAddress.accountAddress) ||
    tx.outputs.map(_.address).contains(accountAddress.accountAddress)
  }

  def addressesUsed(
      accountAddresses: List[AccountAddress]
  )(tx: Transaction): List[AccountAddress] = {
    accountAddresses.filter(addressUsedBy(tx)).distinct
  }

  def markAddresses[Tx <: Transaction](
      keychain: Keychain,
      keychainId: KeychainId
  ): Pipe[IO, BatchResult, BatchResult] =
    _.evalTap(batch =>
      keychain.markAsUsed(keychainId, batch.addresses.distinct.map(_.accountAddress).toSet)
    )

  trait Recordable[Tx <: Transaction] {
    def fetch(
        explorer: ExplorerClient
    )(addresses: Set[Address], block: Option[BlockHash]): Stream[IO, Tx]

    def save(interpreter: InterpreterClient)(accountId: AccountId, txs: List[Tx]): IO[String] =
      for {
        savedTxsCount <- interpreter.saveTransactions(accountId, txs.map(_.toTransactionView))
      } yield s"$savedTxsCount new transactions saved"
  }

  implicit def confirmed(implicit
      cs: ContextShift[IO],
      t: Timer[IO]
  ): Recordable[ConfirmedTransaction] =
    new Recordable[ConfirmedTransaction] {
      override def fetch(
          explorer: ExplorerClient
      )(addresses: Set[Address], block: Option[BlockHash]): Stream[IO, ConfirmedTransaction] =
        explorer.getConfirmedTransactions(addresses.toSeq, block)
    }

  implicit def unconfirmedTransaction(implicit
      cs: ContextShift[IO],
      t: Timer[IO]
  ): Recordable[UnconfirmedTransaction] =
    new Recordable[UnconfirmedTransaction] {
      override def fetch(
          explorer: ExplorerClient
      )(addresses: Set[Address], block: Option[BlockHash]): Stream[IO, UnconfirmedTransaction] =
        explorer.getUnconfirmedTransactions(addresses)
    }

  case class BatchResult(
      addresses: List[AccountAddress],
      maxBlock: Option[Block]
  )

  object BatchResult {

    implicit def monoid[A <: Transaction]: Monoid[BatchResult] = new Monoid[BatchResult] {
      override def empty: BatchResult = BatchResult(List.empty, None)

      override def combine(x: BatchResult, y: BatchResult): BatchResult =
        BatchResult(
          addresses = (x.addresses ::: y.addresses).distinct,
          maxBlock = List(x.maxBlock, y.maxBlock).flatten.maxOption
        )
    }
  }
}
