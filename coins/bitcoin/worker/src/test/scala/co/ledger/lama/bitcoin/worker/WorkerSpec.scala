package co.ledger.lama.bitcoin.worker

import cats.effect.{ContextShift, IO, Timer}
import co.ledger.lama.bitcoin.common.clients.grpc.InterpreterClient
import co.ledger.lama.bitcoin.common.clients.grpc.mocks.InterpreterClientMock
import co.ledger.lama.bitcoin.common.clients.http.ExplorerClient
import co.ledger.lama.bitcoin.common.clients.http.mocks.ExplorerClientMock
import co.ledger.lama.bitcoin.common.models.explorer.Block
import co.ledger.lama.bitcoin.worker.SyncEventServiceFixture.{End, QueueInputOps, registered}
import co.ledger.lama.bitcoin.worker.services.{CursorStateService, SyncEventService}
import co.ledger.lama.common.models.Status.Registered
import co.ledger.lama.common.models.messages.{ReportMessage, WorkerMessage}
import co.ledger.lama.common.models.{AccountIdentifier, Coin, CoinFamily, WorkableEvent}
import co.ledger.lama.common.utils.IOAssertion
import fs2.concurrent.Queue
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.time.Instant
import java.util.UUID

import co.ledger.lama.bitcoin.common.models.interpreter.TransactionView

import scala.concurrent.ExecutionContext

class WorkerSpec extends AnyFlatSpec with Matchers {

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  implicit val t: Timer[IO]         = IO.timer(ExecutionContext.global)

  val accountIdentifier =
    AccountIdentifier(UUID.randomUUID().toString, CoinFamily.Bitcoin, Coin.Btc, "WorkerSpec:31")
  val accountAddresses = LazyList.from(1).map(_.toString)
  val blockchain = LazyList
    .from(0)
    .map { i =>
      val block   = Block((i + 1000).toString, i, Instant.now())
      val address = i.toString
      block -> List(address -> List(TransactionFixture.confirmed.receive(address, inBlock = block)))
    }
    .take(5)
    .toList

  val mempool = LazyList
    .from(100)
    .map { i =>
      val address = i.toString
      address -> List(TransactionFixture.receive(address))
    }
    .take(1)
    .toList

  val defaultExplorer = new ExplorerClientMock(
    blockchain.flatMap(_._2).toMap
  )
  val alreadyValidBlockCursorService: CursorStateService[IO] = (_, b) => IO.pure(b)

  def worker(
      messages: Queue[IO, Option[(AccountIdentifier, WorkableEvent[Block])]],
      interpreter: InterpreterClient = new InterpreterClientMock,
      explorer: ExplorerClient = defaultExplorer
  ) = new Worker(
    SyncEventServiceFixture.syncEventService(receiving = messages),
    KeychainFixture.keychainClient(accountAddresses),
    _ => explorer,
    interpreter,
    _ => alreadyValidBlockCursorService,
    maxTxsToSavePerBatch = 1,
    maxConcurrent = 1
  )

  "Worker" should "run on each Registered event received" in IOAssertion {

    for {
      events <- SyncEventServiceFixture.workableEvents

      runs = worker(events).run

      _ <- registered(accountIdentifier, cursor = None) >> events
      _ <- registered(accountIdentifier, cursor = None) >> events
      _ <- End >> events

      nbRuns <- runs.compile.toList.map(_.size)

    } yield {
      nbRuns shouldBe 2
    }
  }

  it should "synchronize on registered event" in IOAssertion {

    val interpreter = new InterpreterClientMock

    assert(interpreter.savedTransactions.isEmpty)

    for {
      events <- SyncEventServiceFixture.workableEvents
      runs = worker(events, interpreter).run

      _ <- registered(accountIdentifier, cursor = None) >> events
      _ <- End >> events

      _ <- runs.compile.drain

    } yield {

      val txs: List[TransactionView] = interpreter.savedTransactions(accountIdentifier.id)
      txs should have size 4
    }
  }

  it should "not import confirmed txs when the cursor is already on the last mined block" in IOAssertion {

    val lastMinedBlock = blockchain.last._1
    val interpreter    = new InterpreterClientMock
    val explorer =
      new ExplorerClientMock(blockchain.flatMap(_._2).toMap)

    assert(interpreter.savedTransactions.isEmpty)

    for {
      events <- SyncEventServiceFixture.workableEvents
      runs = worker(events, interpreter, explorer).run

      _ <- registered(accountIdentifier, cursor = Some(lastMinedBlock)) >> events
      _ <- End >> events

      _ <- runs.compile.drain

    } yield {

      interpreter.savedTransactions.get(accountIdentifier.id) shouldBe empty
      explorer.getConfirmedTransactionsCount = 0
    }
  }

  it should "try to import unconfirmed txs even if blockchain last block is synced" in {

    val lastMinedBlock = blockchain.last._1
    val explorer =
      new ExplorerClientMock(blockchain.flatMap(_._2).toMap, mempool.toMap)

    for {
      events <- SyncEventServiceFixture.workableEvents
      runs = worker(messages = events, explorer = explorer).run

      _ <- registered(accountIdentifier, cursor = Some(lastMinedBlock)) >> events
      _ <- registered(accountIdentifier, cursor = Some(lastMinedBlock)) >> events
      _ <- End >> events

      _ <- runs.compile.drain

    } yield {
      explorer.getUnConfirmedTransactionsCount = 2
    }

  }
}

object SyncEventServiceFixture {

  def workableEvents(implicit
      cs: ContextShift[IO]
  ): IO[Queue[IO, Option[(AccountIdentifier, WorkableEvent[Block])]]] =
    Queue.bounded[IO, Option[(AccountIdentifier, WorkableEvent[Block])]](5)

  def registered(
      accountId: AccountIdentifier,
      cursor: Option[Block]
  ): (AccountIdentifier, WorkableEvent[Block]) =
    accountId ->
      WorkableEvent(
        accountId.id,
        syncId = UUID.randomUUID(),
        status = Registered,
        cursor,
        error = None,
        Instant.now()
      )

  def syncEventService(
      receiving: Queue[IO, Option[(AccountIdentifier, WorkableEvent[Block])]]
  ): SyncEventService = {
    new SyncEventService {
      override def consumeWorkerMessages: fs2.Stream[IO, WorkerMessage[Block]] = {
        receiving.dequeue.unNoneTerminate
          .map { case (accountIdentifier, event) =>
            WorkerMessage(accountIdentifier, event)
          }
      }

      override def reportMessage(message: ReportMessage[Block]): IO[Unit] = IO.unit
    }
  }

  object End {
    def >>[T](queue: Queue[IO, Option[T]]): IO[Unit] = queue.enqueue1(None)
  }
  implicit class QueueInputOps[M](e: M) {
    def >>(queue: Queue[IO, Option[M]]): IO[Unit] = queue.enqueue1(Some(e))
  }

}
