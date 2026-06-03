package sergeypopov83.chariot.processing.operations.repository.mongodb

import org.bson.BsonString
import sergeypopov83.chariot.processing.operations.Generators
import sergeypopov83.chariot.processing.operations.service.Operation
import zio.test.*
import zio.{Ref, ZIO, ZLayer}

import java.time.Instant

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
          _ <- ZIO.logInfo(s"IDS  $ids")
        } yield {
          assertTrue(listOps.forall(o => ids.contains(BsonString(o.operationId))))
        }
      }

      test("Take 50 last innserted operations") {
        for {
          mrepo <- ZIO.service[OperationsRepository.Service]
        } yield {
          assertTrue(true == false)
        }
      }

      test("Take 10 operations that have greatest amount") {
        for {
          mrepo <- ZIO.service[OperationsRepository.Service]
        } yield {
          assertTrue(false)
        }
      }
    } @@ TestAspect.beforeAll{
      for {
        trxns <- Generators.accountTransactions(pivotInstant).runCollectN(1000)
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
    },
      mockMongoDatabase, OperationsRepositoryLive.live)
  }
}

