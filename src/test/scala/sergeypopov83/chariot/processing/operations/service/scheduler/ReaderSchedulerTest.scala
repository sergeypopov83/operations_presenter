package sergeypopov83.chariot.processing.operations.service.scheduler

import zio.Clock.currentTime
import zio.test.*
import zio.test.Assertion.*
import zio.{Duration, Ref, Schedule, Scope, ZIO, ZLayer, given}

import java.util.concurrent.TimeUnit

object ReaderSchedulerTest extends ZIOSpecDefault {
  override def spec: Spec[TestEnvironment & Scope, Any] = {
    suiteAll("ReaderScheduler") {
      test("scheduleWithAction should repeat the effect according to the policy") {
        for {
          ref <- Ref.make(0)
          job <- ReaderScheduler.scheduleWithAction(ref.update(_ + 1) *> TestClock.adjust(101.millis)).timeout(Duration.fromMillis(100)).fork
          _ <- TestClock.adjust(100.millis)
          count <- ref.get
        } yield assertTrue(count == 1)
      }
      
      test("scheduleWithAction should log warning on failure and continue according to policy") {
        for {
          ref <- Ref.make(0)
          job <- ReaderScheduler
            .scheduleWithAction(for {
              v <- ref.get
              mils <- currentTime(TimeUnit.MILLISECONDS)
              _ <- ZIO.logInfo(s"Current time $mils")
              r <- ZIO.whenCase(v){
                case v if v > 2 =>  ref.update(_ + 1) *> ZIO.fail(new Exception("test exception"))
                case _ => ref.update(_ + 1) *> ZIO.logInfo(s"Current time $mils and v is $v")
              }
            } yield r
            ).fork
          mover <- TestClock.adjust(1.millis).forever.fork // time mover
          _ <- ZIO.sleep(149.millis)
          count <- ref.get
        } yield assert(count)(equalTo(3))
      }
    }
  }
}