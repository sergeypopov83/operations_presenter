package sergeypopov83.chariot.processing.operations.repository.mongodb

import sergeypopov83.chariot.processing.operations.Generators
import sergeypopov83.chariot.processing.operations.service.Operation
import zio.test.*
import zio.{Ref, ZIO, ZLayer}

import java.time.Instant

class OperationsRepositorySuite extends ZIOSpecDefault {

  override def spec: Spec[TestEnvironment, Any] = {
    val zRef = Ref.make(List[Operation]())
    val pivotInstant = Instant.ofEpochMilli(1769526627888L)

    val fillRef: ZIO[Sized, Nothing, Unit] = for {
      trxns <- Generators.accountTransactions(pivotInstant).runCollectN(1000)
      ref <- zRef
      _ <- ref.update(_ => trxns)
    } yield ()

    (suiteAll("OperationsRepository") {
      test("Generate and save 100 events into mongo") {
        for {
          mrepo <- ZIO.service[OperationsRepository.Service]
        } yield
          assertTrue(true)
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
    } @@ TestAspect.beforeAll(fillRef) @@ TestAspect.afterAll {
      for {repo <- ZIO.service[OperationsRepositoryLive]
           ops <- zRef
           l <- ops.get
           _ <- repo.dropAll(l).orDie
           } yield ()
    } @@ TestAspect.sequential).provideSomeAuto(ZLayer(testScope), mockMongoDatabase, OperationsRepositoryLive.live)
  }

}
