package co.ledger.lama.bitcoin.worker

import cats.effect.{ContextShift, IO, Resource, Timer}
import co.ledger.lama.bitcoin.common.clients.grpc.mocks.{InterpreterClientMock, KeychainClientMock}
import co.ledger.lama.bitcoin.common.clients.http.ExplorerHttpClient
import co.ledger.lama.bitcoin.common.models.explorer.Block
import co.ledger.lama.bitcoin.worker.config.Config
import co.ledger.lama.bitcoin.worker.services.{CursorStateService, RabbitSyncEventService}
import co.ledger.lama.common.models.messages.{ReportMessage, WorkerMessage}
import co.ledger.lama.common.models._
import co.ledger.lama.common.services.Clients
import co.ledger.lama.common.utils.{IOAssertion, RabbitUtils}
import dev.profunktor.fs2rabbit.interpreter.RabbitClient
import dev.profunktor.fs2rabbit.model.{ExchangeName, ExchangeType, QueueName, RoutingKey}
import fs2.Stream
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import pureconfig.ConfigSource

import java.time.Instant
import java.util.UUID
import scala.concurrent.ExecutionContext

class WorkerIT extends AnyFlatSpecLike with Matchers {

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  implicit val t: Timer[IO]         = IO.timer(ExecutionContext.global)

  val conf: Config = ConfigSource.default.loadOrThrow[Config]

  val rabbit: Resource[IO, RabbitClient[IO]] = Clients.rabbit(conf.rabbit)

  val resources = for {
    rabbitClient <- rabbit
    httpClient   <- Clients.htt4s
  } yield (rabbitClient, httpClient)

  IOAssertion {
    setupRabbit() *>
      resources
        .use { case (rabbitClient, httpClient) =>
          val syncEventService = new RabbitSyncEventService(
            rabbitClient,
            conf.queueName(conf.workerEventsExchangeName),
            conf.lamaEventsExchangeName,
            conf.routingKey
          )

          val keychainClient = new KeychainClientMock

          val explorerClient = new ExplorerHttpClient(httpClient, conf.explorer, _)

          val interpreterClient = new InterpreterClientMock

          val cursorStateService: Coin => CursorStateService[IO] =
            c => CursorStateService(explorerClient(c), interpreterClient).getLastValidState(_, _)

          val worker = new Worker(
            syncEventService,
            keychainClient,
            explorerClient,
            interpreterClient,
            cursorStateService,
            conf.maxTxsToSavePerBatch,
            conf.maxConcurrent
          )

          val accountManager = new SimpleAccountManager(
            rabbitClient,
            conf.queueName(conf.lamaEventsExchangeName),
            conf.workerEventsExchangeName,
            conf.routingKey
          )

          val keychainId = UUID.randomUUID()

          val account = AccountIdentifier(keychainId.toString, CoinFamily.Bitcoin, Coin.Btc, "WorkerIT:77")

          val syncId = UUID.randomUUID()

          val registeredMessage =
            WorkerMessage[Block](
              account = account,
              event = WorkableEvent(
                account.id,
                syncId,
                Status.Registered,
                None,
                None,
                Instant.now()
              )
            )

          Stream
            .eval {
              accountManager.publishWorkerMessage(registeredMessage) *>
                accountManager.consumeReportMessage
            }
            .concurrently(worker.run)
            .take(1)
            .compile
            .last
            .map { reportMessage =>
              it should "have 35 used addresses for the account" in {
                keychainClient.usedAddresses.size shouldBe 35
              }

              val expectedTxsSize         = 73
              val expectedLastBlockHeight = 644553L

              it should s"have synchronized $expectedTxsSize txs with last blockHeight > $expectedLastBlockHeight" in {
                interpreterClient.savedTransactions
                  .getOrElse(
                    account.id,
                    List.empty
                  )
                  .distinctBy(_.hash) should have size expectedTxsSize

                reportMessage should not be empty
                reportMessage.get.account shouldBe account

                val event = reportMessage.get.event
                event.cursor.get.height should be > expectedLastBlockHeight
                event.cursor.get.time should be > Instant.parse("2020-08-20T13:01:16Z")
              }
            }
        }
  }

  def setupRabbit(): IO[Unit] =
    rabbit.use { client =>
      for {
        _ <- RabbitUtils.deleteBindings(
          client,
          List(
            conf.queueName(conf.workerEventsExchangeName),
            conf.queueName(conf.lamaEventsExchangeName)
          )
        )
        _ <- RabbitUtils.deleteExchanges(
          client,
          List(conf.workerEventsExchangeName, conf.lamaEventsExchangeName)
        )
        _ <- RabbitUtils.declareExchanges(
          client,
          List(
            (conf.workerEventsExchangeName, ExchangeType.Topic),
            (conf.lamaEventsExchangeName, ExchangeType.Topic)
          )
        )
        res <- RabbitUtils.declareBindings(
          client,
          List(
            (
              conf.workerEventsExchangeName,
              conf.routingKey,
              conf.queueName(conf.workerEventsExchangeName)
            ),
            (
              conf.lamaEventsExchangeName,
              conf.routingKey,
              conf.queueName(conf.lamaEventsExchangeName)
            )
          )
        )
      } yield res
    }

}

class SimpleAccountManager(
    rabbit: RabbitClient[IO],
    lamaEventsQueueName: QueueName,
    workerEventsExchangeName: ExchangeName,
    routingKey: RoutingKey
) {

  private lazy val consumer: Stream[IO, ReportMessage[Block]] =
    RabbitUtils.createAutoAckConsumer[ReportMessage[Block]](rabbit, lamaEventsQueueName)

  private lazy val publisher: Stream[IO, WorkerMessage[Block] => IO[Unit]] =
    RabbitUtils.createPublisher[WorkerMessage[Block]](rabbit, workerEventsExchangeName, routingKey)

  def consumeReportMessage: IO[ReportMessage[Block]] =
    consumer.take(1).compile.last.map(_.get)

  def publishWorkerMessage(message: WorkerMessage[Block]): IO[Unit] =
    publisher.evalMap(p => p(message)).compile.drain

}
