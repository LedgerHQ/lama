package co.ledger.lama.bitcoin.interpreter

import co.ledger.lama.bitcoin.common.models.interpreter._
import co.ledger.lama.bitcoin.interpreter.models.OperationToSave
import co.ledger.lama.bitcoin.interpreter.services.OperationQueries.Op
import co.ledger.lama.common.models.TxHash
import co.ledger.lama.common.utils.IOAssertion
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import java.util.UUID

class QueriesIT extends AnyFlatSpecLike with Matchers with TestResources {

  val block: BlockView = BlockView(
    "00000000000000000008c76a28e115319fb747eb29a7e0794526d0fe47608379",
    570153,
    Instant.parse("2019-04-04T10:03:22Z")
  )

  val accountId: UUID = UUID.fromString("b723c553-3a9a-4130-8883-ee2f6c2f9201")

  val outputs = List(
    OutputView(0, 50000, "1DtwACvd338XtHBFYJRVKRLxviD7YtYADa", "script", None, None),
    OutputView(1, 9434, "1LK8UbiRwUzC8KFEbMKvgbvriM9zLMce3C", "script", None, None)
  )
  val inputs = List(
    InputView(
      "0f38e5f1b12078495a9e80c6e0d77af3d674cfe6096bb6e7909993a53b6e8386",
      0,
      0,
      80000,
      "1LD1pARePgXXyZA1J3EyvRtB82vxENs5wQ",
      "script",
      List(),
      4294967295L,
      None
    )
  )
  val transactionToInsert: TransactionView =
    TransactionView(
      "txId",
      "a8a935c6bc2bd8b3a7c20f107a9eb5f10a315ce27de9d72f3f4e27ac9ec1eb1f",
      Instant.parse("2019-04-04T10:03:22Z"),
      0,
      20566,
      inputs,
      outputs,
      Some(block),
      1
    )

  "transaction saved in db" should "not be returned and populated by fetch before Interpreter.compute" in IOAssertion {
    setup() *>
      appResources.use { db =>
        for {
          _ <- QueryUtils.saveTx(db, transactionToInsert, accountId)
          _ <- QueryUtils.saveTx(db, transactionToInsert, accountId) // check upsert
          inputsWithOutputs <- QueryUtils.fetchInputAndOutputs(
            db,
            accountId,
            TxHash(transactionToInsert.hash)
          )

          account = Operation.AccountId(accountId)

          opWithTx <- QueryUtils.fetchOpAndTx(
            db,
            account,
            Operation.uid(account, Operation.TxId(transactionToInsert.hash), opToSave.operationType)
          )
        } yield {

          opWithTx shouldBe None
          inputsWithOutputs should not be empty

          val Some((inputs, outputs)) = inputsWithOutputs

          inputs should have size 1
          inputs.head.value shouldBe 80000

          outputs should have size 2
          outputs.filter(_.outputIndex == 0).head.value shouldBe 50000
        }
      }
  }

  val opToSave: OperationToSave = OperationToSave(
    Operation.uid(
      Operation.AccountId(accountId),
      Operation.TxId(transactionToInsert.id),
      OperationType.Send
    ),
    accountId,
    transactionToInsert.hash,
    OperationType.Send,
    transactionToInsert.inputs.collect { case i: InputView =>
      i.value
    }.sum,
    transactionToInsert.fees,
    block.time,
    Some(block.hash),
    Some(block.height)
  )

  "operation saved in db" should "be fetched" in IOAssertion {
    setup() *>
      appResources.use { db =>
        for {
          _   <- QueryUtils.saveTx(db, transactionToInsert, accountId)
          _   <- QueryUtils.saveOp(db, opToSave)
          ops <- QueryUtils.fetchOps(db, accountId)
        } yield {
          ops.map(_._1) should contain only
            Op(
              opToSave.uid,
              opToSave.accountId,
              TxHash(opToSave.hash),
              opToSave.operationType,
              opToSave.value,
              opToSave.fees,
              opToSave.time,
              opToSave.blockHeight
            )
        }
      }
  }

}
