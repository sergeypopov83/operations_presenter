package sergeypopov83.chariot.processing.operations.repository.kafka

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG
import org.testcontainers.kafka.{ConfluentKafkaContainer, KafkaContainer}
import org.testcontainers.utility.DockerImageName
import sergeypopov83.chariot.processing.operations.repository.kafka.KafkaConfiguration.BOOTSTRAP_SERVERS
import zio.{Task, ZIO, ZLayer}

import java.util.UUID
import scala.jdk.CollectionConverters.MapHasAsJava

object TestContainers {

  val kafkaContainer: KafkaContainer = new KafkaContainer(DockerImageName.parse("apache/kafka"))

  val live: ZLayer[Any, Throwable, KafkaContainer] =
    ZLayer.scoped {
      ZIO.acquireRelease(
        ZIO.attempt {
          val c = kafkaContainer
          c.start()
          c
        }
      )(container => ZIO.attempt(container.stop()).ignoreLogged)
    }
  
  def initContainer(): Task[KafkaContainer] = ZIO.attemptBlocking {
    kafkaContainer.start()
    System.setProperty("KAFKA_LISTENER_GROUP", UUID.randomUUID().toString)
    System.setProperty(BOOTSTRAP_SERVERS, kafkaContainer.getBootstrapServers)
    System.setProperty(KafkaConfiguration.USERNAME, "")
    System.setProperty(KafkaConfiguration.PWD, "")
    kafkaContainer
  }

  /**
   * The singleton container is started only once when the base class is loaded. The container can
   * then be used by all inheriting test classes. At the end of the test suite the Ryuk container
   * that is started by TestContainers core will take care of stopping the singleton container
   */
  def kafkaAdminClient(kafkaContainer: ConfluentKafkaContainer): AdminClient = {
    AdminClient.create(
      Map(
        BOOTSTRAP_SERVERS_CONFIG -> kafkaContainer.getBootstrapServers
      ).asJava
    )
  }
}