package sergeypopov83.chariot.processing.operations.repository.kafka

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.security.auth.SecurityProtocol
import zio.kafka.consumer.{Consumer, ConsumerSettings, Subscription}
import org.apache.kafka.common.serialization.{ByteArraySerializer, LongSerializer}
import zio.kafka.producer.{Producer, ProducerSettings}
import zio.kafka.serde.Serde
import zio.{Scope, Task, URIO, ZIO, given}

import scala.jdk.CollectionConverters.MapHasAsJava
import java.util.UUID

class KafkaRepository(config: KafkaConfiguration) {

  val producer: ZIO[Scope, Throwable, Producer] = {
    val settings = if config.saslMechanismEnabled then
      ProducerSettings(
        config.bootstrapServers //List("89.169.46.86:9092")
      ).withProperty(
        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name
      ).withProperty(
        SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512"
      ).withProperty(
        "sasl.jaas.config", s"org.apache.kafka.common.security.scram.ScramLoginModule required username=${config.user} password=\"${config.pwd}\" ;"
      )
    else ProducerSettings(
      config.bootstrapServers //List("89.169.46.86:9092")
    )
    ZIO.acquireRelease(Producer.make(settings))(p => p.flush.orDie)
  }

  def kafkaProducer(): ZIO[Any, Nothing, KafkaProducer[java.lang.Long, Array[Byte]]] = {
    val cfg = Map(
      CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> config.bootstrapServers.mkString(","), //List("89.169.46.86:9092")
      CommonClientConfigs.SECURITY_PROTOCOL_CONFIG -> SecurityProtocol.PLAINTEXT.name
    )

    val p = new KafkaProducer(cfg.asJava, LongSerializer(), ByteArraySerializer())
    ZIO.succeed(p)
  }

  private def consumer(topic: String): ZIO[Scope, Throwable, Consumer] = {
    val settings = if config.saslMechanismEnabled then
      ConsumerSettings(
        config.bootstrapServers //List("89.169.46.86:9092")
      ).withProperty(
          CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name
        ).withProperty(CommonClientConfigs.GROUP_ID_CONFIG, UUID.randomUUID())
        .withProperty(
          SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512"
        ).withProperty(
          "sasl.jaas.config", s"org.apache.kafka.common.security.scram.ScramLoginModule required username=${config.user} password=\"${config.pwd}\" ;"
        ).withProperty("auto.offset.reset", "earliest")
    else
      ConsumerSettings(
        config.bootstrapServers
      )

    Consumer.make(settings.withGroupId(s"$topic.operation-presenter-group"))
  }

  def consume(topic: String)(f: (rec: ConsumerRecord[Long, Array[Byte]]) => URIO[Scope, Unit]): ZIO[Scope, Throwable, Unit] = 
    consumer(topic).flatMap(c => c.consumeWith(
    subscription = Subscription.topics(topic),
    keyDeserializer = Serde.long,
    valueDeserializer = Serde.byteArray,
  ) {
    r => f(r)
  })

  def produceRecord(producer: Producer, topic: String, key: Long, value: Array[Byte]): Task[RecordMetadata] =
    producer.produce[Any, Long, Array[Byte]](
      topic = topic,
      key = key,
      value = value,
      keySerializer = Serde.long,
      valueSerializer = Serde.byteArray
    ).tap( r => 
      producer.flush
    )

  def produceKafkaRecord(producer: KafkaProducer[java.lang.Long, Array[Byte]], topic: String, key: Long, value: Array[Byte]): Task[RecordMetadata] =
    val r = new ProducerRecord(topic, key.asInstanceOf[java.lang.Long], value)
    ZIO.fromFutureJava(producer.send(r))
}
