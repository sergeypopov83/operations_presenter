package sergeypopov83.chariot.processing.operations.repository.mongodb

import org.bson.BsonString
import sergeypopov83.chariot.processing.operations.Generators
import sergeypopov83.chariot.processing.operations.service.{MoneyAmount, Operation}
import zio.test.*
import zio.{Ref, ZIO, ZLayer}

import java.time.{Clock, Instant}

object OperationsRepositorySuite extends ZIOSpecDefault {

  override def spec: Spec[TestEnvironment, Any] = {
    val pivotInstant = Instant.ofEpochMilli(1769526627888L)
    (suiteAll("OperationsRepository") {

      test("Generate and save 100 events into mongo") {
        for {
          mrepo <- ZIO.service[OperationsRepository.Service]
          ops <- ZIO.service[Ref[List[Operation]]]
          listOps <- ops.get
          result <- mrepo.saveManyOperations(listOps)
          ids = result.getInsertedIds.values()
        } yield {
          assertTrue(listOps.forall(o => ids.contains(BsonString(o.operationId))))
        }
      }

      test("Take 50 last inserted operations") {
        for {
          mrepo <- ZIO.service[OperationsRepository.Service]
          ops <- ZIO.service[Ref[List[Operation]]]
          r <- mrepo.lastNOperations(50)
          listOps <- ops.get
          lastOps = listOps.sortBy(_.createdAt.toEpochMilli * -1).take(50)
        } yield {
          assertTrue(r.zip(lastOps).forall((o1, o2) => o1.createdAt.getEpochSecond == o2.createdAt.getEpochSecond))
        }
      }
      test("Sum and count of operations should be correct") {
        for {
          mrepo <- ZIO.service[OperationsRepository.Service]
          ops <- ZIO.service[Ref[List[Operation]]]
          listOps <- ops.get
          testOperation = listOps.head
          result <- mrepo.operationsPeriodStatisticsByLegalEntity(testOperation.legalEntityId,
            Option(testOperation.createdAt.minusSeconds(100000000)), Option(testOperation.createdAt.plusSeconds(100000000)))
          testAmount = listOps.filter(o => o.legalEntityId == testOperation.legalEntityId).foldLeft(MoneyAmount.ZEROEURO){(a,i) =>
            MoneyAmount(a.amount + i.amount.amount, a.currency)
          }
        } yield {
          assertTrue(testAmount == result.get.totalAmount)
        }
      }

      test("Take 10 operations that have greatest amount") {
        for {
          mrepo <- ZIO.service[OperationsRepository.Service]
          r <- mrepo.topNOperationsByAmount(10)
          ops <- ZIO.service[Ref[List[Operation]]]
          listOps <- ops.get
          maxAmts = listOps.sortBy(_.amount.amount * -1).take(10)
        } yield {
          assertTrue(r.zip(maxAmts).forall( (o1, o2) => o1.amount.amount == o2.amount.amount))
        }
      }
    } @@ TestAspect.beforeAll{
      for {
        trxns <- Generators.accountTransactions(pivotInstant).runCollectN(2000)
        ref <- ZIO.service[Ref[List[Operation]]]
        _ <- ref.set(trxns)
      } yield ()
    } @@ TestAspect.afterAll {
      for {repo <- ZIO.service[OperationsRepositoryLive]
           ops <- ZIO.service[Ref[List[Operation]]]
           l <- ops.get
           _ <- repo.dropAll(l).orDie
           } yield ()
    } @@ TestAspect.sequential).provideSomeShared[Sized](ZLayer(testScope), ZLayer {
      Ref.make(List[Operation]())
    }, ZLayer.succeed(Clock.systemUTC()), mockMongoDatabase, OperationsRepositoryLive.live)
  }
}

