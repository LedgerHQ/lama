package co.ledger.lama.bitcoin.interpreter.services

import cats.data.{NonEmptyList, OptionT}
import cats.effect.{ContextShift, IO}
import co.ledger.lama.bitcoin.common.models.interpreter._
import co.ledger.lama.bitcoin.interpreter.models.{OperationToSave, TransactionAmounts}
import co.ledger.lama.common.logging.IOLogging
import co.ledger.lama.common.models.{Sort, TxHash}
import doobie._
import doobie.implicits._
import fs2._

import java.util.UUID

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
        .groupAdjacentBy { case (op, _) => op.hash } // many operations by hash (RECEIVED AND SENT)
        .chunkN(5) // we have to figure out which value is more suitable here
        .flatMap { ops =>
          val txHashes = ops.map { case (txHash, _) => txHash }.toNel

          val inputsAndOutputs = Stream
            .emits(txHashes.toList)
            .flatMap(txHashes =>
              OperationQueries
                .fetchInputsWithOutputsOrderedByTxHash(accountId, sort, txHashes)
            )

          Stream
            .chunk(ops)
            .covary[ConnectionIO]
            .zip(inputsAndOutputs)
        }
        .transact(db)
        .through(makeOperation)
        .compile
        .toList

      total <- OperationQueries.countOperations(accountId, blockHeight).transact(db)

      // we get 1 more than necessary to know if there's more, then we return the correct number
      truncated = opsWithTx.size > limit

    } yield {
      val operations = opsWithTx.take(limit)
      GetOperationsResult(operations, total, truncated)
    }

  private lazy val makeOperation: Pipe[
    IO,
    (
        (TxHash, Chunk[(OperationQueries.Op, OperationQueries.Tx)]),
        (TxHash, (List[InputView], List[OutputView]))
    ),
    Operation
  ] =
    _.flatMap { case ((txHash, sentAndReceivedOperations), (txHash1, (inputs, outputs))) =>
      Stream
        .chunk(sentAndReceivedOperations)
        .takeWhile(_ => txHash == txHash1)
        .map { case (op, tx) =>
          operation(tx, op, inputs, outputs)
        }
    }

  def getOperation(
      accountId: Operation.AccountId,
      operationId: Operation.UID
  )(implicit cs: ContextShift[IO]): IO[Option[Operation]] = {

    val o = for {
      (op, tx) <- OptionT(OperationQueries.findOperation(accountId, operationId))
      (txhash, (inputs, outputs)) <- OptionT(
        OperationQueries
          .fetchInputsWithOutputsOrderedByTxHash(
            accountId.value,
            Sort.Ascending,
            NonEmptyList.one(op.hash)
          )
          .compile
          .last
      )
      if txhash == op.hash
    } yield operation(tx, op, inputs, outputs)

    o.value.transact(db)
  }

  private def operation(
      tx: OperationQueries.Tx,
      op: OperationQueries.Op,
      inputs: List[InputView],
      outputs: List[OutputView]
  ) =
    Operation(
      uid = op.uid,
      accountId = op.accountId,
      hash = op.hash.hex,
      transaction = TransactionView(
        id = tx.id,
        hash = tx.hash.hex,
        receivedAt = tx.receivedAt,
        lockTime = tx.lockTime,
        fees = tx.fees,
        inputs = inputs,
        outputs = outputs,
        block = tx.block,
        confirmations = tx.confirmations
      ),
      operationType = op.operationType,
      amount = op.amount,
      fees = op.fees,
      time = op.time,
      blockHeight = op.blockHeight
    )

  def deleteUnconfirmedTransactionView(accountId: UUID): IO[Int] =
    OperationQueries
      .deleteUnconfirmedTransactionsViews(accountId)
      .transact(db)

  def saveUnconfirmedTransactionView(
      accountId: UUID,
      transactions: List[TransactionView]
  ): IO[Int] =
    OperationQueries
      .saveUnconfirmedTransactionView(accountId, transactions)
      .transact(db)

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
      confirmedUtxos <- OperationQueries
        .fetchUTXOs(accountId, sort, Some(limit + 1), Some(offset))
        .transact(db)
        .compile
        .toList

      // Flag utxos used in the mempool
      unconfirmedInputs <- OperationQueries
        .fetchUnconfirmedTransactionsViews(accountId)
        .transact(db)
        .map(_.flatMap(_.inputs).filter(_.belongs))

      total <- OperationQueries.countUTXOs(accountId).transact(db)

    } yield {
      val utxos = confirmedUtxos.map(utxo =>
        utxo.copy(
          usedInMempool = unconfirmedInputs.exists(input =>
            input.outputHash == utxo.transactionHash && input.outputIndex == utxo.outputIndex
          )
        )
      )

      // We get 1 more than necessary to know if there's more, then we return the correct number
      GetUtxosResult(utxos.slice(0, limit), total, truncated = utxos.size > limit)
    }

  def getUnconfirmedUtxos(accountId: UUID): IO[List[Utxo]] =
    OperationQueries
      .fetchUnconfirmedTransactionsViews(accountId)
      .transact(db)
      .map { unconfirmedTxs =>
        val usedOutputs = unconfirmedTxs
          .flatMap(_.inputs)
          .filter(_.belongs)
          .map(i => (i.outputHash, i.outputIndex))

        val outputMap = unconfirmedTxs
          .flatMap(tx =>
            tx.outputs
              .collect { case o @ OutputView(_, _, _, _, _, Some(derivation)) =>
                (tx.hash, o.outputIndex) -> (tx.hash, o, tx.receivedAt, derivation)
              }
          )
          .toMap

        val unusedOutputs = outputMap.keys.toList
          .diff(usedOutputs)
          .map(outputMap)

        unusedOutputs
          .map { case (hash, output, time, derivation) =>
            Utxo(
              hash,
              output.outputIndex,
              output.value,
              output.address,
              output.scriptHex,
              output.changeType,
              derivation,
              time
            )
          }
      }

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
