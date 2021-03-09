package co.ledger.lama.common.models

import cats.Eq

case class TxHash(hex: String) extends AnyVal

object TxHash {
  implicit val eq: Eq[TxHash]             = Eq.by(_.hex)
  implicit val ordering: Ordering[TxHash] = Ordering.by(_.hex)
}
