package sergeypopov83.chariot.processing.operations.repository.kafka

import sergeypopov83.chariot.processing.operations.utils.configurationEnvSource
import zio.{Config, IO}

case class KafkaConfiguration(
                               pwd: String,
                               user: String,
                               saslMechanismEnabled: Boolean,
                               bootstrapServers: List[String]
                             )

object KafkaConfiguration {

  val BOOTSTRAP_SERVERS = "BOOTSTRAP_SERVERS"
  val USERNAME: String = "KAFKA_USERNAME"
  val PWD: String = "KAFKA_PASSWORD"

  // topics
  val operations_topic = "operations"

  private val configDescription = (Config.string(KafkaConfiguration.BOOTSTRAP_SERVERS)
    ++ Config.string(KafkaConfiguration.PWD)
    ++ Config.string(KafkaConfiguration.USERNAME)
    )
    .map((serversStr, pwd, user) => KafkaConfiguration(bootstrapServers = serversStr.split(",").toList, pwd = pwd, user = user, saslMechanismEnabled = true))

  private def loadMongoConfig(): IO[Config.Error, KafkaConfiguration] = {
    configurationEnvSource.load(configDescription)
  }


}