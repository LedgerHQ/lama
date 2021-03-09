package co.ledger.lama.manager

import cats.effect.IO
import co.ledger.lama.common.models._
import co.ledger.lama.common.utils.{IOAssertion, RabbitUtils}
import co.ledger.lama.manager.config.CoinConfig
import co.ledger.lama.common.models.messages.{ReportMessage, WorkerMessage}
import dev.profunktor.fs2rabbit.interpreter.RabbitClient
import dev.profunktor.fs2rabbit.model.ExchangeName
import doobie.implicits._
import fs2.Stream
import io.circe.{Json, JsonObject}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class AccountManagerIT extends AnyFlatSpecLike with Matchers with TestResources {

  IOAssertion {
    setup() *>
      appResources.use { case (db, redisClient, rabbitClient) =>
        val service = new AccountManager(db, conf.orchestrator.coins)

        val coinOrchestrator =
          new CoinOrchestrator(conf.orchestrator, db, rabbitClient, redisClient)

        val worker = new SimpleWorker(
          rabbitClient,
          conf.orchestrator.workerEventsExchangeName,
          conf.orchestrator.lamaEventsExchangeName,
          conf.orchestrator.coins.head
        )

        val nbEvents = 15

        def runTests(): IO[Unit] =
          for {
            // Register an account.
            registeredResult <- service.registerAccount(
              accountTest.key,
              CoinFamily.Bitcoin,
              Coin.Btc,
              None,
              None,
              "AccountManagerIT:46"
            )

            registeredAccountId = registeredResult.accountId
            registeredSyncId    = registeredResult.syncId

            messageSent1 <- worker.consumeWorkerMessage()

            // Report a successful sync event with a new cursor.
            syncedCursorJson = Json.obj("blockHeight" -> Json.fromLong(123456789)).asObject
            _ <- worker.publishReportMessage(
              ReportMessage(
                account = messageSent1.account,
                event = messageSent1.event.asReportableSuccessEvent(syncedCursorJson)
              )
            )

            messageSent2 <- worker.consumeWorkerMessage()

            // Report a failed sync event with an error message.
            syncFailedError = ReportError(code = "sync_failed", message = Some("failed to sync"))
            _ <- worker.publishReportMessage(
              ReportMessage(
                account = messageSent2.account,
                event = messageSent2.event.asReportableFailureEvent(syncFailedError)
              )
            )

            // reseed scenario
            _ <- service.resyncAccount(accountTest.id, wipe = true)

            messageSent3 <- worker.consumeWorkerMessage()

            // Report after a reseed a successful sync event
            _ <- worker.publishReportMessage(
              ReportMessage(
                account = messageSent3.account,
                event = messageSent3.event.asReportableSuccessEvent(syncedCursorJson)
              )
            )

            // Unregister an account.
            unregisteredResult <- service.unregisterAccount(accountTest.id)

            unregisteredAccountId = unregisteredResult.accountId
            unregisteredSyncId    = unregisteredResult.syncId

            messageSent4 <- worker.consumeWorkerMessage()

            // Report a failed delete event with an error message.
            deleteFailedError = ReportError(
              code = "delete_failed",
              message = Some("failed to delete data")
            )
            _ <- worker.publishReportMessage(
              ReportMessage(
                account = messageSent4.account,
                event = messageSent4.event.asReportableFailureEvent(deleteFailedError)
              )
            )

            messageSent5 <- worker.consumeWorkerMessage()

            // Report a successful delete event.
            _ <- worker.publishReportMessage(
              ReportMessage(
                account = messageSent5.account,
                event = messageSent5.event.asReportableSuccessEvent(None)
              )
            )

            // Fetch all sync events.
            syncEvents <- Queries
              .getSyncEvents(accountTest.id, Sort.Ascending)
              .take(nbEvents)
              .compile
              .toList
              .transact(db)
          } yield {
            it should "have consumed messages from worker" in {
              messageSent1 shouldBe
                WorkerMessage(
                  account = accountTest,
                  event = WorkableEvent(
                    accountTest.id,
                    registeredSyncId,
                    Status.Registered,
                    None,
                    None,
                    messageSent1.event.time
                  )
                )

              messageSent2 shouldBe
                WorkerMessage(
                  account = accountTest,
                  event = WorkableEvent(
                    accountTest.id,
                    messageSent2.event.syncId,
                    Status.Registered,
                    syncedCursorJson,
                    None,
                    messageSent2.event.time
                  )
                )

              messageSent3 shouldBe
                WorkerMessage(
                  account = accountTest,
                  event = WorkableEvent(
                    accountTest.id,
                    messageSent3.event.syncId,
                    Status.Registered,
                    None,
                    None,
                    messageSent3.event.time
                  )
                )

              messageSent4 shouldBe
                WorkerMessage(
                  account = accountTest,
                  event = WorkableEvent(
                    accountTest.id,
                    unregisteredSyncId,
                    Status.Unregistered,
                    None,
                    None,
                    messageSent4.event.time
                  )
                )

              messageSent5 shouldBe
                WorkerMessage(
                  account = accountTest,
                  event = WorkableEvent(
                    accountTest.id,
                    messageSent5.event.syncId,
                    Status.Unregistered,
                    None,
                    Some(deleteFailedError),
                    messageSent5.event.time
                  )
                )
            }

            it should s"have $nbEvents inserted events" in {
              syncEvents should have size nbEvents
            }

            it should "succeed to register an account" in {
              registeredAccountId shouldBe accountTest.id
            }

            it should "have (registered -> published -> synchronized) events for the first iteration" in {
              val eventsBatch1 = syncEvents.slice(0, 3)
              eventsBatch1 shouldBe List(
                WorkableEvent(
                  accountTest.id,
                  registeredSyncId,
                  Status.Registered,
                  None,
                  None,
                  eventsBatch1.head.time
                ),
                FlaggedEvent(
                  accountTest.id,
                  registeredSyncId,
                  Status.Published,
                  None,
                  None,
                  eventsBatch1(1).time
                ),
                ReportableEvent(
                  accountTest.id,
                  registeredSyncId,
                  Status.Synchronized,
                  syncedCursorJson,
                  None,
                  eventsBatch1(2).time
                )
              )
            }

            it should "have (registered -> published -> sync_failed) events for the next iteration" in {
              val eventsBatch2 = syncEvents.slice(3, 6)
              eventsBatch2 shouldBe List(
                WorkableEvent(
                  accountTest.id,
                  messageSent2.event.syncId,
                  Status.Registered,
                  syncedCursorJson,
                  None,
                  eventsBatch2.head.time
                ),
                FlaggedEvent(
                  accountTest.id,
                  messageSent2.event.syncId,
                  Status.Published,
                  syncedCursorJson,
                  None,
                  eventsBatch2(1).time
                ),
                ReportableEvent(
                  accountTest.id,
                  messageSent2.event.syncId,
                  Status.SyncFailed,
                  syncedCursorJson,
                  Some(syncFailedError),
                  eventsBatch2(2).time
                )
              )
            }

            it should "have (registered -> published -> synchronized) events after a reseed (force sync manual from 0)" in {
              val eventsBatch3 = syncEvents.slice(6, 9)
              eventsBatch3 shouldBe List(
                WorkableEvent(
                  accountTest.id,
                  messageSent3.event.syncId,
                  Status.Registered,
                  None,
                  None,
                  eventsBatch3.head.time
                ),
                FlaggedEvent(
                  accountTest.id,
                  messageSent3.event.syncId,
                  Status.Published,
                  None,
                  None,
                  eventsBatch3(1).time
                ),
                ReportableEvent(
                  accountTest.id,
                  messageSent3.event.syncId,
                  Status.Synchronized,
                  syncedCursorJson,
                  None,
                  eventsBatch3(2).time
                )
              )
            }

            it should "succeed to unregister an account" in {
              unregisteredAccountId shouldBe accountTest.id
            }

            it should "have (unregistered -> published -> delete_failed) events for the next iteration" in {
              val eventsBatch4 = syncEvents.slice(9, 12)
              eventsBatch4 shouldBe List(
                WorkableEvent(
                  accountTest.id,
                  messageSent4.event.syncId,
                  Status.Unregistered,
                  None,
                  None,
                  eventsBatch4.head.time
                ),
                FlaggedEvent(
                  accountTest.id,
                  messageSent4.event.syncId,
                  Status.Published,
                  None,
                  None,
                  eventsBatch4(1).time
                ),
                ReportableEvent(
                  accountTest.id,
                  messageSent4.event.syncId,
                  Status.DeleteFailed,
                  None,
                  Some(deleteFailedError),
                  eventsBatch4(2).time
                )
              )
            }

            it should "have (unregistered -> published -> deleted) events at the end" in {
              val eventsBatch5 = syncEvents.slice(12, 15)
              eventsBatch5 shouldBe List(
                WorkableEvent(
                  accountTest.id,
                  messageSent5.event.syncId,
                  Status.Unregistered,
                  None,
                  Some(deleteFailedError),
                  eventsBatch5.head.time
                ),
                FlaggedEvent(
                  accountTest.id,
                  messageSent5.event.syncId,
                  Status.Published,
                  None,
                  Some(deleteFailedError),
                  eventsBatch5(1).time
                ),
                ReportableEvent(
                  accountTest.id,
                  messageSent5.event.syncId,
                  Status.Deleted,
                  None,
                  None,
                  eventsBatch5(2).time
                )
              )
            }
          }

        coinOrchestrator
          .run(stopAtNbTick = Some(nbEvents + 1)) // run the orchestrator
          .concurrently(Stream.eval(runTests()))  // and run tests at the same time
          .timeout(5.minutes)
          .compile
          .drain
      }
  }

}

class SimpleWorker(
    rabbit: RabbitClient[IO],
    inExchangeName: ExchangeName,
    outExchangeName: ExchangeName,
    coinConf: CoinConfig
) {

  private val consumer: Stream[IO, WorkerMessage[JsonObject]] =
    RabbitUtils
      .createAutoAckConsumer[WorkerMessage[JsonObject]](rabbit, coinConf.queueName(inExchangeName))

  private val publisher: Stream[IO, ReportMessage[JsonObject] => IO[Unit]] =
    RabbitUtils
      .createPublisher[ReportMessage[JsonObject]](rabbit, outExchangeName, coinConf.routingKey)

  def consumeWorkerMessage(): IO[WorkerMessage[JsonObject]] =
    consumer.take(1).compile.last.map(_.get)

  def publishReportMessage(message: ReportMessage[JsonObject]): IO[Unit] =
    publisher.evalMap(p => p(message)).compile.drain

}
