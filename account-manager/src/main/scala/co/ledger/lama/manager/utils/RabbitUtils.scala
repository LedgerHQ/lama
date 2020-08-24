package co.ledger.lama.manager.utils

import java.nio.charset.StandardCharsets
import java.util.concurrent.Executors

import cats.data.Kleisli
import cats.effect.{Blocker, ConcurrentEffect, ContextShift, IO, Resource}
import cats.implicits._
import dev.profunktor.fs2rabbit.config.Fs2RabbitConfig
import dev.profunktor.fs2rabbit.config.declaration.DeclarationQueueConfig
import dev.profunktor.fs2rabbit.effects.MessageEncoder
import dev.profunktor.fs2rabbit.interpreter.RabbitClient
import dev.profunktor.fs2rabbit.model._
import fs2.Stream
import io.circe.{Decoder, Encoder}
import io.circe.parser._
import io.circe.syntax._

object RabbitUtils {

  def createClient(
      conf: Fs2RabbitConfig
  )(implicit ce: ConcurrentEffect[IO], cs: ContextShift[IO]): Resource[IO, RabbitClient[IO]] =
    Resource
      .make(IO(Executors.newCachedThreadPool()))(es => IO(es.shutdown()))
      .map(Blocker.liftExecutorService)
      .evalMap(RabbitClient[IO](conf, _))

  def declareExchanges(
      R: RabbitClient[IO],
      exchanges: List[(ExchangeName, ExchangeType)]
  ): IO[Unit] =
    R.createConnectionChannel.use { implicit channel =>
      exchanges
        .map {
          case (exchangeName, exchangeType) =>
            R.declareExchange(exchangeName, exchangeType)
        }
        .sequence
        .void
    }

  def declareBindings(
      R: RabbitClient[IO],
      bindings: List[(ExchangeName, RoutingKey, QueueName)]
  ): IO[Unit] =
    R.createConnectionChannel.use { implicit channel =>
      bindings
        .map {
          case (exchangeName, routingKey, queueName) =>
            R.declareQueue(DeclarationQueueConfig.default(queueName)) *>
              R.bindQueue(queueName, exchangeName, routingKey)
        }
        .sequence
        .void
    }

  def createAutoAckConsumer[A](
      R: RabbitClient[IO],
      queueName: QueueName
  )(implicit d: Decoder[A]): Stream[IO, A] =
    Stream
      .resource(R.createConnectionChannel)
      .evalMap { implicit channel =>
        R.createAutoAckConsumer[String](queueName)
      }
      .flatten
      .evalMap { message =>
        val parsed = parse(message.payload).flatMap(_.as[A])
        IO.fromEither(parsed)
      }

  def createPublisher[A](
      R: RabbitClient[IO],
      exchangeName: ExchangeName,
      routingKey: RoutingKey
  )(implicit e: Encoder[A]): Stream[IO, A => IO[Unit]] = {
    implicit val me: MessageEncoder[IO, A] =
      Kleisli[IO, A, AmqpMessage[Array[Byte]]] { s =>
        AmqpMessage(
          payload = s.asJson.noSpaces.getBytes(StandardCharsets.UTF_8),
          properties = AmqpProperties.empty
        ).pure[IO]
      }

    Stream
      .resource(R.createConnectionChannel)
      .evalMap { implicit channel =>
        R.createPublisher[A](
          exchangeName,
          routingKey
        )
      }
  }

}
