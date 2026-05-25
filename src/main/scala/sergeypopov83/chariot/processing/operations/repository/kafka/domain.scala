package sergeypopov83.chariot.processing.operations.repository.kafka

import java.time.Instant
import java.util.UUID

case class OutboxEvent(
                        // UUID7
                        eventId: UUID,
                        msgType: String,
                        createdAt: Instant,
                        processed: Boolean,
                        operationId: String,
                        idempotencyKey: String,
                        partitionKey: Long,
                        processedAt: Option[Instant],
                      )