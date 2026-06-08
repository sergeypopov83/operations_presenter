package sergeypopov83.chariot.processing.operations

import sergeypopov83.chariot.processing.operations.service.{MoneyAmount, Operation}
import zio.test.{Gen, Sized}

import java.time.Instant
import java.util.UUID
import scala.math.BigDecimal.RoundingMode
import scala.util.Random

object Generators {

  def datesPeriod(min: Instant, max: Instant): Gen[Any, Instant] = Gen.instant(min, max)
  // pre generate accounts and le lists
  val legalEntities: List[UUID] = (1 to 10).map(_ => UUID.randomUUID()).toList
  val accountsId: List[UUID] = (1 to 10).map(_ => UUID.randomUUID()).toList

  def moneyValuesGenerator: Gen[Any, BigDecimal] = Gen.bigDecimal(BigDecimal(1), BigDecimal(+10000000)).
    map(_.setScale(2, RoundingMode.HALF_EVEN)
    )

  def accountTransactions(pivotInstant: Instant): Gen[Sized, Operation] = for {
    ci <- Generators.datesPeriod(pivotInstant.minusMillis(10000000), pivotInstant.plusMillis(10000000))
    amnt <- Generators.moneyValuesGenerator
    badTrxn <- zio.schema.DeriveGen.gen[Operation]
    int <- Gen.const( Random.between(0, 9))
    leId = legalEntities(int)
    accId = accountsId(int)
    trxn = badTrxn.copy(amount = MoneyAmount(amnt, MoneyAmount.EUR), createdAt = ci, legalEntityId = leId.toString,
      accountId = accId.toString)
  } yield {
    trxn
  }
}