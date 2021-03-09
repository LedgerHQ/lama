package co.ledger.lama.bitcoin.api.routes

import cats.Monoid
import cats.data.NonEmptyList
import cats.effect.{ContextShift, IO}
import cats.implicits._
import co.ledger.lama.bitcoin.api.models.BalancePreset
import co.ledger.lama.bitcoin.api.models.accountManager._
import co.ledger.lama.bitcoin.api.models.transactor._
import co.ledger.lama.bitcoin.api.utils.RouterUtils._
import co.ledger.lama.bitcoin.common.clients.grpc.{
  InterpreterClient,
  KeychainClient,
  TransactorClient
}
import co.ledger.lama.common.models.implicits.defaultCirceConfig
import co.ledger.lama.bitcoin.common.models.interpreter.ChangeType
import co.ledger.lama.bitcoin.common.utils.CoinImplicits._
import co.ledger.lama.common.clients.grpc.AccountManagerClient
import co.ledger.lama.common.logging.IOLogging
import co.ledger.lama.common.utils.UuidUtils
import io.circe.Json
import io.circe.generic.extras.auto._
import io.circe.syntax._
import org.http4s.HttpRoutes
import org.http4s.circe.CirceEntityCodec._
import org.http4s.dsl.Http4sDsl

import java.time.Instant
import java.time.temporal.ChronoUnit

object AccountController extends Http4sDsl[IO] with IOLogging {

  implicit val cs: ContextShift[IO] = IO.contextShift(scala.concurrent.ExecutionContext.global)

  def transactionsRoutes(
      accountManagerClient: AccountManagerClient,
      transactorClient: TransactorClient
  ): HttpRoutes[IO] = HttpRoutes.of[IO] {

    // Create transaction
    case req @ POST -> Root / UUIDVar(
          accountId
        ) / "transactions" =>
      for {
        _                        <- log.info(s"Preparing transaction creation for account: $accountId")
        createTransactionRequest <- req.as[CreateTransactionRequest]

        account    <- accountManagerClient.getAccountInfo(accountId)
        keychainId <- UuidUtils.stringToUuidIO(account.key)

        response <- transactorClient
          .createTransaction(
            accountId,
            keychainId,
            account.coin,
            createTransactionRequest.coinSelection,
            createTransactionRequest.outputs,
            createTransactionRequest.feeLevel,
            createTransactionRequest.customFee,
            createTransactionRequest.maxUtxos
          )
          .flatMap(Ok(_))

      } yield response

    // Sign transaction (only for testing)
    case req @ POST -> Root / "_internal" / "sign" =>
      for {
        _       <- log.info(s"Signing Transaction")
        request <- req.as[GenerateSignaturesRequest]

        response <- transactorClient
          .generateSignature(
            request.rawTransaction,
            request.privKey
          )
          .flatMap(Ok(_))

      } yield response

    // Send transaction
    case req @ POST -> Root / UUIDVar(
          accountId
        ) / "transactions" / "send" =>
      for {
        _       <- log.info(s"Broadcasting transaction for account: $accountId")
        request <- req.as[BroadcastTransactionRequest]

        account    <- accountManagerClient.getAccountInfo(accountId)
        keychainId <- UuidUtils.stringToUuidIO(account.key)

        txInfo <- transactorClient
          .broadcastTransaction(
            keychainId,
            account.coin.name,
            request.rawTransaction,
            request.signatures
          )
          .flatMap(Ok(_))

        syncResult <- accountManagerClient.resyncAccount(accountId, wipe = false)
        _          <- log.info(s"Sync sent to account manager: $syncResult")

      } yield txInfo

    case req @ POST -> Root / UUIDVar(
          accountId
        ) / "recipients" =>
      for {
        coin      <- accountManagerClient.getAccountInfo(accountId).map(_.coin)
        addresses <- req.as[NonEmptyList[String]]
        result <- transactorClient
          .validateAddresses(coin, addresses.map(TransactorClient.Address))
          .map(results =>
            results.collectFold {
              case TransactorClient.Accepted(address) => ValidationResult.valid(address)
              case TransactorClient.Rejected(address, reason) =>
                ValidationResult.invalid(address, reason)
            }
          )
          .flatMap(Ok(_))

      } yield result
  }

  def routes(
      keychainClient: KeychainClient,
      accountManagerClient: AccountManagerClient,
      interpreterClient: InterpreterClient
  ): HttpRoutes[IO] =
    HttpRoutes.of[IO] {

      // Register account
      case req @ POST -> Root =>
        val ra = for {
          creationRequest <- req.as[CreationRequest]
          _               <- log.info(s"Creating keychain with arguments: $creationRequest")

          createdKeychain <- keychainClient.create(
            creationRequest.accountKey,
            creationRequest.scheme,
            creationRequest.lookaheadSize,
            creationRequest.coin.toNetwork
          )
          _ <- log.info(s"Keychain created with id: ${createdKeychain.keychainId}")
          _ <- log.info("Registering account")

          account <- accountManagerClient.registerAccount(
            createdKeychain.keychainId,
            creationRequest.coin.coinFamily,
            creationRequest.coin,
            creationRequest.syncFrequency,
            creationRequest.label,
            creationRequest.group
          )

          _ <- log.info(
            s"Account registered with id: ${account.accountId}"
          )
        } yield RegisterAccountResponse(
          account.accountId,
          account.syncId,
          createdKeychain.extendedPublicKey
        )

        ra.flatMap(Ok(_))

      // Get account info
      case GET -> Root / UUIDVar(accountId) =>
        accountManagerClient
          .getAccountInfo(accountId)
          .parProduct(interpreterClient.getBalance(accountId))
          .flatMap { case (account, balance) =>
            Ok(
              AccountWithBalance(
                account.id,
                account.coinFamily,
                account.coin,
                account.syncFrequency,
                account.lastSyncEvent,
                balance.balance,
                balance.unconfirmedBalance,
                balance.utxos,
                balance.received,
                balance.sent,
                account.label
              )
            )
          }

      // Get account events
      case GET -> Root / UUIDVar(accountId) / "events"
          :? OptionalLimitQueryParamMatcher(limit)
          +& OptionalOffsetQueryParamMatcher(offset)
          +& OptionalSortQueryParamMatcher(sort) =>
        accountManagerClient
          .getSyncEvents(accountId, limit, offset, sort)
          .flatMap(Ok(_))

      // Update account
      case req @ PUT -> Root / UUIDVar(accountId) =>
        val r = for {
          updateRequest <- req.as[UpdateRequest]

          _ <- log.info(
            s"Updating account $accountId with $updateRequest"
          )

          _ <- updateRequest match {
            case UpdateLabel(label) => accountManagerClient.updateLabel(accountId, label)
            case UpdateSyncFrequency(syncFrequency) =>
              accountManagerClient.updateSyncFrequency(accountId, syncFrequency)
            case UpdateSyncFrequencyAndLabel(syncFrequency, label) =>
              accountManagerClient.updateAccount(accountId, syncFrequency, label)
          }
        } yield ()
        r.flatMap(_ => Ok())

      // List accounts
      case GET -> Root
          :? OptionalLimitQueryParamMatcher(limit)
          +& OptionalOffsetQueryParamMatcher(offset) =>
        val t = for {

          // Get Account Info
          accountsResult <- accountManagerClient.getAccounts(None, limit, offset)
          accountsWithIds = accountsResult.accounts.map(account => account.id -> account)

          // Get Balance
          accountsWithBalances <- accountsWithIds.parTraverse { case (accountId, account) =>
            interpreterClient.getBalance(accountId).map(balance => account -> balance)
          }
        } yield (accountsWithBalances, accountsResult.total)

        t.flatMap { case (accountsWithBalances, total) =>
          val accountsInfos = accountsWithBalances.map { case (account, balance) =>
            AccountWithBalance(
              account.id,
              account.coinFamily,
              account.coin,
              account.syncFrequency,
              account.lastSyncEvent,
              balance.balance,
              balance.unconfirmedBalance,
              balance.utxos,
              balance.received,
              balance.sent,
              account.label
            )
          }

          Ok(
            Json.obj(
              "accounts" -> Json.fromValues(accountsInfos.map(_.asJson)),
              "total"    -> Json.fromInt(total)
            )
          )
        }

      // List account operations
      case GET -> Root / UUIDVar(
            accountId
          ) / "operations" :? OptionalBlockHeightQueryParamMatcher(blockHeight)
          +& OptionalLimitQueryParamMatcher(limit)
          +& OptionalOffsetQueryParamMatcher(offset)
          +& OptionalSortQueryParamMatcher(sort) =>
        log.info(s"Fetching operations for account: $accountId") *>
          interpreterClient
            .getOperations(
              accountId = accountId,
              blockHeight = blockHeight.getOrElse(0L),
              limit = limit.getOrElse(0),
              offset = offset.getOrElse(0),
              sort = sort
            )
            .flatMap(Ok(_))

      // Get operation info
      case GET -> Root / UUIDVar(
            accountId
          ) / "operations" / uid =>
        log.info(s"Fetching operations for account: $accountId") *>
          interpreterClient
            .getOperation(
              accountId,
              operationId = uid
            )
            .flatMap {
              case Some(operation) => Ok(operation)
              case None            => NotFound(s"No operation in account identified by $uid ")
            }

      // List account UTXOs
      case GET -> Root / UUIDVar(
            accountId
          ) / "utxos" :? OptionalLimitQueryParamMatcher(limit)
          +& OptionalOffsetQueryParamMatcher(offset)
          +& OptionalSortQueryParamMatcher(sort) =>
        log.info(s"Fetching UTXOs for account: $accountId") *>
          interpreterClient
            .getUtxos(
              accountId = accountId,
              limit = limit.getOrElse(0),
              offset = offset.getOrElse(0),
              sort = sort
            )
            .flatMap(Ok(_))

      // Get account balances
      case GET -> Root / UUIDVar(
            accountId
          ) / "balances" :? OptionalStartInstantQueryParamMatcher(start)
          +& OptionalEndInstantQueryParamMatcher(end)
          +& OptionalIntervalQueryParamMatcher(interval) =>
        log.info(s"Fetching balances history for account: $accountId") *>
          interpreterClient
            .getBalanceHistory(
              accountId,
              start,
              end,
              interval
            )
            .flatMap(Ok(_))

      // Get account balances with preset
      case GET -> Root / UUIDVar(
            accountId
          ) / "balances" / BalancePreset(preset) =>
        log.info(s"Fetching balances history for account: $accountId") *>
          interpreterClient
            .getBalanceHistory(
              accountId,
              Some(Instant.now().minus(preset.durationInSeconds, ChronoUnit.SECONDS)),
              Some(Instant.now()),
              Some(preset.interval)
            )
            .flatMap(Ok(_))

      // List observable account addresses
      case GET -> Root / UUIDVar(
            accountId
          ) / "addresses" :? OptionalFromIndexQueryParamMatcher(from)
          +& OptionalToIndexQueryParamMatcher(to)
          +& OptionalChangeTypeParamMatcher(change) =>
        for {
          account    <- accountManagerClient.getAccountInfo(accountId)
          keychainId <- UuidUtils.stringToUuidIO(account.key)

          response <- keychainClient
            .getAddresses(
              keychainId,
              from.getOrElse(0),
              to.getOrElse(0),
              change
            )
            .flatMap(Ok(_))

        } yield response

      // List fresh account addresses
      case GET -> Root / UUIDVar(
            accountId
          ) / "addresses" / "fresh" :? OptionalChangeTypeParamMatcher(change) =>
        for {
          account      <- accountManagerClient.getAccountInfo(accountId)
          keychainId   <- UuidUtils.stringToUuidIO(account.key)
          keychainInfo <- keychainClient.getKeychainInfo(keychainId)

          response <- keychainClient
            .getFreshAddresses(
              keychainId,
              change.getOrElse(ChangeType.External),
              keychainInfo.lookaheadSize
            )
            .flatMap(Ok(_))

        } yield response

      // Resync account
      case GET -> Root / UUIDVar(accountId) / "resync"
          :? OptionalWipeQueryParamMatcher(wipe) =>
        for {
          _       <- log.info(s"Fetching account informations for id: $accountId")
          account <- accountManagerClient.getAccountInfo(accountId)

          isWipe = wipe.getOrElse(false)

          _ <- log.info(s"Resyncing account $accountId - wipe=$isWipe")

          _ <-
            if (isWipe) {
              for {
                _          <- log.info("Resetting keychain")
                keychainId <- UuidUtils.stringToUuidIO(account.key)
                res        <- keychainClient.resetKeychain(keychainId)
                _          <- log.info("Removing interpreter data")
                _          <- interpreterClient.removeDataFromCursor(account.id, None)
              } yield res
            } else IO.unit

          res <- accountManagerClient.resyncAccount(accountId, isWipe).flatMap(Ok(_))
        } yield res

      // Unregister account
      case DELETE -> Root / UUIDVar(accountId) =>
        for {

          _       <- log.info(s"Fetching account informations for id: $accountId")
          account <- accountManagerClient.getAccountInfo(accountId)

          _          <- log.info("Deleting keychain")
          keychainId <- UuidUtils.stringToUuidIO(account.key)
          _ <- keychainClient
            .deleteKeychain(keychainId)
            .map(_ => log.info("Keychain deleted"))
            .handleErrorWith(_ =>
              log.info("An error occurred while deleting the keychain, moving on")
            )

          _ <- log.info("Unregistering account")
          _ <- accountManagerClient.unregisterAccount(account.id)
          _ <- log.info("Account unregistered")

          res <- Ok()

        } yield res
    }

}

case class ValidationResult(valid: List[String], invalid: Map[String, String])
object ValidationResult {

  def valid(address: TransactorClient.Address): ValidationResult =
    ValidationResult(valid = List(address.value), invalid = Map.empty)
  def invalid(address: TransactorClient.Address, reason: String): ValidationResult =
    ValidationResult(valid = List.empty, invalid = Map(address.value -> reason))

  implicit val monoid: Monoid[ValidationResult] = new Monoid[ValidationResult] {
    override def empty: ValidationResult = ValidationResult(List.empty, Map.empty)

    override def combine(x: ValidationResult, y: ValidationResult): ValidationResult =
      ValidationResult(
        valid = x.valid ::: y.valid,
        invalid = (x.invalid.toList ::: y.invalid.toList).toMap
      )
  }

}
