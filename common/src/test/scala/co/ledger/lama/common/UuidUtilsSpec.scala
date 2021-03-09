package co.ledger.lama.common

import java.util.UUID

import co.ledger.lama.common.models.{AccountIdentifier, Coin, CoinFamily}
import co.ledger.lama.common.utils.UuidUtils
import org.scalacheck.Gen
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class UuidUtilsSpec extends AnyFunSuite with Matchers {

  test("uuid to bytes") {
    val uuids = Gen.listOfN(1000, Gen.uuid).sample.get
    uuids.foreach { uuid =>
      val bytes = UuidUtils.uuidToBytes(uuid)
      assert(UuidUtils.bytesToUuid(bytes).contains(uuid))
    }
  }

  test("account identifier to uuid") {
    assert(
      AccountIdentifier("xpub", CoinFamily.Bitcoin, Coin.Btc, "UuidUtilsSpec:23").id ==
        UUID.fromString("4159f144-4e96-3cfa-8495-f26b22a99459")
    )
  }

}
