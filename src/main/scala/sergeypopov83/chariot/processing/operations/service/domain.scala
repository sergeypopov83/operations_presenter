package sergeypopov83.chariot.processing.operations.service

import com.fasterxml.jackson.core.{JsonGenerator, JsonParser}
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, JsonNode, ObjectMapper, SerializerProvider}
import org.apache.kafka.clients.consumer.ConsumerRecord

import java.math.{MathContext, RoundingMode}
import java.time.Instant
import java.util.{Currency, UUID}


case class OutboxEvent(
                        // UUID7
                        eventId: UUID,
                        msgType: String,
                        createdAt: Instant,
                        processed: Boolean,
                        operationId: String,
                        idempotencyKey: String,
                        partitionKey: Long,
                        processedAt: Instant
                      )

object OutboxEvent {

  type JacksonConverter = (mapper: ObjectMapper) => OutboxEvent

  private def convert(rec: ConsumerRecord[String, String]): JacksonConverter = (mapper: ObjectMapper) => {
    mapper.readValue[OutboxEvent](rec.value(), classOf[OutboxEvent])
  }
  
  given recordToEvent: Conversion[ConsumerRecord[String, String], JacksonConverter] = convert

}


case class Operation(
                      operationId: String,
                      operationType: String,
                      status: String,
                      createdAt: Instant,
                      amount: MoneyAmount,
                      accountId: String,
                      legalEntityId: String
                    )

case class MoneyAmount(amount: BigDecimal, currency: Currency)

object MoneyAmount:
  val EUR: Currency = Currency.getInstance("EUR")
  val USD: Currency = Currency.getInstance("USD")

  private val defaultPrecision = 20
  private val defaultRoundingMode = RoundingMode.HALF_EVEN
  private val scalaDefaultRoundingMode = scala.math.BigDecimal.RoundingMode.HALF_EVEN

  val ZEROEURO: MoneyAmount = MoneyAmount(BigDecimal(0, MathContext(defaultPrecision, defaultRoundingMode)).setScale(EUR.scale), EUR)
  val ZEROUSD: MoneyAmount = MoneyAmount(BigDecimal(0, MathContext(defaultPrecision, defaultRoundingMode)).setScale(USD.scale), USD)

  def apply(amount: BigDecimal, currency: Currency): MoneyAmount = {
    val amnt = amount(MathContext(defaultPrecision, defaultRoundingMode)).setScale(currency.scale)
    new MoneyAmount(amnt, currency)
  }

  def apply(amount: Long, currency: Currency): MoneyAmount =
    new MoneyAmount(BigDecimal(amount, MathContext(defaultPrecision, defaultRoundingMode)).setScale(currency.scale), currency)


  class MoneyAmountSerializer(t: Class[MoneyAmount]) extends StdSerializer[MoneyAmount](t):

    def this() = {
      this(null)
    }

    @Override
    def serialize(money: MoneyAmount,
                  jsonGenerator: JsonGenerator,
                  serializer: SerializerProvider): Unit = {
      jsonGenerator.writeStartObject();
      jsonGenerator.writeStringField("amount", money.amount.toString())
      jsonGenerator.writeStringField("currency", money.currency.getCurrencyCode)
      jsonGenerator.writeEndObject();
    }
  end MoneyAmountSerializer

  class MoneyAmountDeserializer(t: Class[MoneyAmount]) extends StdDeserializer[MoneyAmount](t):

    def this() =
      this(null)

    @Override
    def deserialize(parser: JsonParser, deserializer: DeserializationContext): MoneyAmount =
      val codec = parser.getCodec
      val node: JsonNode = codec.readTree(parser)
      // try catch block
      // better MAthContext
      val amount = BigDecimal(node.get("amount").asText())
      val currency = Currency.getInstance(node.get("currency").asText())
      MoneyAmount(amount, currency)
  end MoneyAmountDeserializer
end MoneyAmount

extension (c: Currency)
  def scale: Int = c.getNumericCodeAsString match {
    case "978" => 2
    case "840" => 2
    case _ => throw new RuntimeException(s"Unknown currency code ${c.getNumericCodeAsString}")
  }