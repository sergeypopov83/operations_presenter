package sergeypopov83.chariot.processing.operations.service

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.consumer.ConsumerRecord
import sergeypopov83.chariot.processing.operations.repository.kafka.KafkaRepository
import sergeypopov83.chariot.processing.operations.repository.mongodb.MongoRepository
import sergeypopov83.chariot.processing.operations.service.OutboxEvent.JacksonConverter
import sergeypopov83.chariot.processing.operations.service.grpc.SponsorbankService
import zio.{Scope, Task, ZIO}

import java.util.UUID

class OperationsEnricher(val operationsTopic: String,
                         val objectMapper: ObjectMapper,
                         val kafka: KafkaRepository,
                         val mongo: MongoRepository,
                         val sponsorbankService: SponsorbankService) {

  def startConsuming(): ZIO[Scope, Throwable, Unit] = {
    kafka.consume(operationsTopic)(r =>
      ZIO.scope.flatMap { _ =>
        (for {
          ev <- ZIO.attempt(kafkaRecordConverter[String, String](r))
          transfer <- sponsorbankService.findTransferData(UUID.fromString(ev.operationId))
          oe <- storeEvent(ev)
          _ <- ZIO.logInfo(s"Record $ev stored into dynamo")
        } yield ()).foldZIO(th => ZIO.logError(s"Save failed with $th"), p => ZIO.succeed(p))
      })
  }

  def storeEvent(outboxEvent: OutboxEvent): Task[Unit] =  ???

  def kafkaRecordConverter[K, V](r: ConsumerRecord[K, V])(using conv: (r: ConsumerRecord[K, V]) => JacksonConverter): OutboxEvent =
    conv(r)(objectMapper)

}
