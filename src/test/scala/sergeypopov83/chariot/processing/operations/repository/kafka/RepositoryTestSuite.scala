package sergeypopov83.chariot.processing.operations.repository.kafka

import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.RecordMetadata
import org.testcontainers.kafka.KafkaContainer
import sergeypopov83.chariot.processing.operations.repository.kafka.TestContainers.kafkaAdminClient
import zio.*
import zio.Clock.currentTime
import zio.kafka.consumer.Consumer.OffsetRetrieval
import zio.kafka.consumer.{Consumer, ConsumerSettings, Subscription}
import zio.kafka.serde.Serde
import zio.test.*
import zio.test.TestAspect.{afterAll, beforeAll}

import java.lang.System as JSystem
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.SeqHasAsJava

object RepositoryTestSuite extends ZIOSpecDefault {

  private val topic = "test_topic"

  override def spec: Spec[TestEnvironment, Any] =
    suiteAll("KafkaRepository Integration Tests") {
      test("produce and consume messages using consume() method") {
        ZIO.scoped {
          for {
            container <- ZIO.service[KafkaContainer]
            repo = new KafkaRepository(KafkaConfiguration(pwd = "", user = "", saslMechanismEnabled = false, bootstrapServers = List(container.getBootstrapServers)))
            ref <- Ref.make(Nil)
            job <- repo.consume(topic)(
              kr => ref.update(_ :+ kr.key())
            ).fork
            _ <- ZIO.sleep(3.seconds)
            producer <- repo.producer
            results <- ZIO.foreach(1L to 3L) { key =>
              repo.produceRecord(producer, topic, key.toString, s"msg-$key")
            }
            _ <- ZIO.sleep(5.seconds)
            consumedKeys <- ref.get
          } yield assertTrue(consumedKeys.size == 3) &&
            assertTrue(consumedKeys.contains(1L)) &&
            assertTrue(consumedKeys.contains(2L)) &&
            assertTrue(consumedKeys.contains(3L))
        }
      } @@ TestAspect.withLiveClock
    }.provideShared(TestContainers.live)
}