package sergeypopov83.chariot.processing.operations.service

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.consumer.ConsumerRecord
import sergeypopov83.chariot.processing.operations.repository.kafka.KafkaRepository
import sergeypopov83.chariot.processing.operations.repository.mongodb.OperationsRepository
import sergeypopov83.chariot.processing.operations.service.OutboxEvent.JacksonConverter
import sergeypopov83.chariot.processing.operations.service.grpc.{SponsorbankService, toDomain}
import sergeypopov83.chariot.processing.sponsorbank.grpc.TransfersService.GetTransferIntoByTransferIdResponse
import zio.{Scope, Task, ZIO}

import java.util.UUID

class OperationsEnricher(val operationsTopic: String,
                         val objectMapper: ObjectMapper,
                         val kafka: KafkaRepository,
                         val mongo: OperationsRepository.Service,
                         val sponsorbankService: SponsorbankService) {

  def startConsuming(): ZIO[Scope, Throwable, Unit] = {
    kafka.consume(operationsTopic)(r =>
      ZIO.scope.flatMap { _ =>
        (for {
          ev <- ZIO.attempt(kafkaRecordConverter[String, String](r))
          transfer <- sponsorbankService.findTransferData(UUID.fromString(ev.operationId))
          oe <- storeTransfer(mapToPersistentRecord(ev, transfer))
          _ <- ZIO.logInfo(s"Record $ev stored into mongo")
        } yield ()).foldZIO(th => ZIO.logError(s"Save failed with $th"), p => ZIO.succeed(p))
      })
  }

  private def mapToPersistentRecord(ev: OutboxEvent, transfer: GetTransferIntoByTransferIdResponse): Operation =
    Operation(
      operationId = ev.operationId,
      operationType = transfer.transferType,
      status = transfer.status,
      createdAt = transfer.createdAt.getOrElse(throw RuntimeException("Created at has to be present")).asJavaInstant,
      amount = transfer.getAmount.toDomain,
      accountId = transfer.accountId,
      legalEntityId = transfer.leId
    )

  def storeTransfer(operation: Operation): Task[Unit] = mongo.saveOperation(operation).foldZIO(
    f => ZIO.logError(s"Insert of the operation $operation has failed").map(
      // rethrow in order to fail the whole effect including Kafka read
      throw f
    ), s => ZIO.logInfo("Has successfully")
  )
  

  def kafkaRecordConverter[K, V](r: ConsumerRecord[K, V])(using conv: (r: ConsumerRecord[K, V]) => JacksonConverter): OutboxEvent =
    conv(r)(objectMapper)

}
