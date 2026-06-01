package sergeypopov83.chariot.processing.operations.service.grpc

import com.google.`type`.money.Money
import com.google.protobuf.timestamp.Timestamp
import sergeypopov83.chariot.processing.operations.service.MoneyAmount

import java.time.Instant
import java.util.{Currency, Date}

extension (money: Money)
  def toDomain: MoneyAmount =
    val amt = money.units + money.nanos.toDouble / 1000000000
    MoneyAmount(amt, Currency.getInstance(money.currencyCode))
end extension

extension (inst: Instant)
  def toGrpc: Timestamp =
    Timestamp.of(inst.getEpochSecond, inst.getNano)

extension (date: Date)
  def toGrpc: Timestamp =
    date.toInstant.toGrpc