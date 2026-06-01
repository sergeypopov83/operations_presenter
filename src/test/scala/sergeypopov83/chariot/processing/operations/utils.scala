package sergeypopov83.chariot.processing.operations

import sergeypopov83.chariot.processing.operations.service.{MoneyAmount, Operation}
import zio.ZIO
import zio.test.{Gen, Sized}

import java.time.Instant
import java.util.UUID
import scala.math.BigDecimal.RoundingMode

object Generators {

  def datesPeriod(min: Instant, max: Instant): Gen[Any, Instant] = Gen.instant(min, max)

  val legalEntities: ZIO[Any, Nothing, List[UUID]] = Gen.uuid.runCollectN(10)

  val randomEntity: Gen[Any, UUID] = Gen.fromRandom { random => random.nextIntBounded(10) }.mapZIO(i =>
    legalEntities.map(l => l(i))
  )

  val accountsId: ZIO[Any, Nothing, List[UUID]] = Gen.uuid.runCollectN(10)

  val randomAccountId: Gen[Any, UUID] = Gen.fromRandom { random => random.nextIntBounded(10) }.mapZIO(i =>
    accountsId.map(a => a(i))
  )

  def moneyValuesGenerator: Gen[Any, BigDecimal] = Gen.bigDecimal(BigDecimal(1), BigDecimal(+10000000)).
    map(_.setScale(2, RoundingMode.HALF_EVEN)
    )

  def accountTransactions(pivotInstant: Instant): Gen[Sized, Operation] = for {
    ci <- Generators.datesPeriod(pivotInstant.minusMillis(10000000), pivotInstant.plusMillis(10000000))
    amnt <- Generators.moneyValuesGenerator
    badTrxn <- zio.schema.DeriveGen.gen[Operation]
    leId <- randomEntity
    accountsId <- randomAccountId
    trxn = badTrxn.copy(amount = MoneyAmount(amnt, MoneyAmount.EUR), createdAt = ci, legalEntityId = leId.toString,
      accountId = accountsId.toString)
  } yield {
    trxn
  }
}