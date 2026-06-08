package sergeypopov83.chariot.processing.operations.repository.mongodb

import com.mongodb.client.model.{TimeSeriesGranularity, TimeSeriesOptions}
import com.mongodb.client.result.{DeleteResult, InsertManyResult, InsertOneResult}
import com.mongodb.{ExplainVerbosity, MongoClientSettings}
import mongo4cats.bson.BsonValue
import mongo4cats.bson.BsonValue.BDateTime
import mongo4cats.bson.BsonValue.BDateTime.given
import mongo4cats.models.collection.IndexOptions
import mongo4cats.models.database.CreateCollectionOptions
import mongo4cats.operations.*
import mongo4cats.zio.{ZMongoCollection, ZMongoDatabase}
import org.bson.codecs.BsonValueCodec
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.bson.codecs.configuration.{CodecProvider, CodecRegistry}
import org.bson.types.ObjectId
import sergeypopov83.chariot.processing.operations.repository.mongodb.OperationsRepository.{OperationMongo, OperationsStatistics, TmpAggregateResult, codecRegistry}
import sergeypopov83.chariot.processing.operations.service.{MoneyAmount, Operation}
import zio.bson.{BsonCodec, zioBsonCodecProvider}
import zio.metrics.Metric
import zio.schema.codec.BsonSchemaCodec
import zio.schema.{Schema, derived}
import zio.{Task, ZIO, ZLayer}

import java.time.temporal.ChronoUnit
import java.time.{Clock, Instant}

object OperationsRepository {
  trait Service {
    def saveOperation(operation: Operation): Task[InsertOneResult]

    def saveManyOperations(operation: Seq[Operation]): Task[InsertManyResult]

    def lastNOperations(opsCount: Int): Task[List[Operation]]

    def topNOperationsByAmount(opsCount: Int): Task[List[Operation]]

    def dropAll(list: List[Operation]): Task[DeleteResult]

    def operationsPeriodStatisticsByLegalEntity(le: String, stratOpt: Option[Instant], endOpt: Option[Instant]): Task[Option[OperationsStatistics]]

  }

  case class OperationMongoMeta(
                                 accountId: String,
                                 legalEntityId: String,
                               )

  case class OperationMongo(
                             _id: String,
                             meta: OperationMongoMeta,
                             operationId: String,
                             operationType: String,
                             status: String,
                             createdAt: Instant,
                             amount: BigDecimal,
                           ) derives Schema

  case class OperationsStatistics(
                                   legalEntity: String,
                                   periodStart: Instant,
                                   periodEnd: Instant,
                                   count: Long,
                                   totalAmount: MoneyAmount
                                 )

  case class TmpAggregateResult(
                                 _id: String,
                                 count: Long,
                                 totalAmount: BigDecimal,
                               ) derives Schema

  given objectIdSchema: Schema[ObjectId] =
    Schema[String].transform(
      str => new ObjectId(str),
      id => id.toString
    )

  // this is bson codec to store and retrieve data from mongo
  given bsonCodec: BsonCodec[OperationMongo] = BsonSchemaCodec.bsonCodec(OperationMongo.derived$Schema)
  given tmpCodec: BsonCodec[TmpAggregateResult] = BsonSchemaCodec.bsonCodec(TmpAggregateResult.derived$Schema)

  val operationCodecProvider: CodecProvider = zioBsonCodecProvider[OperationMongo]
  val tmpAggregateResultCodecProvider: CodecProvider = zioBsonCodecProvider[TmpAggregateResult]

  val codecRegistry: CodecRegistry = fromRegistries(
    MongoClientSettings.getDefaultCodecRegistry,
    fromProviders(operationCodecProvider, tmpAggregateResultCodecProvider)
  )

  def fromOperations(trxn: Operation): OperationMongo = OperationMongo(
    _id = trxn.operationId,
    meta = OperationMongoMeta(trxn.accountId, trxn.legalEntityId),
    operationType = trxn.operationType,
    status = trxn.status,
    createdAt = trxn.createdAt,
    amount = trxn.amount.amount,
    operationId = trxn.operationId
  )

  def toOperations(trxn: OperationMongo): Operation = Operation.apply(
    accountId = trxn.meta.accountId,
    legalEntityId = trxn.meta.legalEntityId,
    operationId = trxn.operationId.toString,
    operationType = trxn.operationType,
    status = trxn.status,
    createdAt = trxn.createdAt,
    amount = MoneyAmount(trxn.amount, MoneyAmount.EUR)
  )

}

object OperationsRepositoryLive {

  // Metrics (optional, but great for observability)
  private val operationsRetrievedCounter = Metric.counterInt("operations_retrieved_total")
  private val timeIndexName = "operations_time_index"

  def makeService: ZIO[ZMongoDatabase & Clock, Throwable, OperationsRepositoryLive] = for {
    db <- ZIO.service[ZMongoDatabase]
    operCollection <- makeTimeSeriesCollection
    clock <- ZIO.service[Clock]
  } yield new OperationsRepositoryLive(db, operCollection, clock)

  def makeTimeSeriesCollection: ZIO[ZMongoDatabase, Throwable, ZMongoCollection[OperationMongo]] = for {
    db <- ZIO.service[ZMongoDatabase]
    list <- db.listCollectionNames
    _ <- ZIO.ifZIO(ZIO.succeed(!list.toList.contains(MongoConfiguration.DEFAULT_COLLECTION))).apply(db.createCollection(MongoConfiguration.DEFAULT_COLLECTION, CreateCollectionOptions()
      .timeSeriesOptions(
        TimeSeriesOptions("createdAt").metaField("meta").granularity(TimeSeriesGranularity.MINUTES)
      )), ZIO.succeed(()))
    col <- db.getCollection[OperationMongo](MongoConfiguration.DEFAULT_COLLECTION, codecRegistry)
    indexes <- col.listIndexes
    _ <- ZIO.ifZIO(ZIO.attempt(!indexes.toList.exists(idx => idx.getString("name").contains(timeIndexName)))).apply(
      col.createIndex(Index.descending("createdAt"), new IndexOptions().name(timeIndexName)), ZIO.succeed(())
    )
  } yield col

  def removeCollection(db: ZMongoDatabase): ZIO[Any, Nothing, Unit] = {
    ZIO.succeed(db).flatMap { d =>
      db.getCollection(MongoConfiguration.DEFAULT_COLLECTION)
    }.flatMap { cc =>
      cc.drop
    }.orDie
  }

  // ZMongoDatabase -> OperationsRepository
  val live: ZLayer[ZMongoDatabase & Clock, Throwable, OperationsRepositoryLive] =
    ZLayer.fromZIO {
      makeService
    }
}

class OperationsRepositoryLive(mongoDatabase: ZMongoDatabase,
                               operCollection: ZMongoCollection[OperationMongo],
                               clock: Clock) extends OperationsRepository.Service {

  import OperationsRepository.{fromOperations, toOperations}

  override def saveOperation(operation: Operation): Task[InsertOneResult] = operCollection.insertOne(fromOperations(operation))

  override def saveManyOperations(operations: Seq[Operation]): Task[InsertManyResult] = operCollection.insertMany(operations.map(o => fromOperations(o)))

  override def lastNOperations(opsCount: Int): Task[List[Operation]] = {
    val aggrQuery = Aggregate.sort(Sort.desc("createdAt"))
      .limit(opsCount)
    val q = operCollection.aggregate[OperationMongo](aggrQuery)
    q.explain(ExplainVerbosity.QUERY_PLANNER).flatMap(expl =>
      ZIO.logInfo(s"LAST N PERFORMANCE STATS $expl").flatMap(_ =>
        q.all.map(ol => ol.map(o => toOperations(o)).toList)
      )
    )
  }

  override def topNOperationsByAmount(opsCount: Int): Task[List[Operation]] = {
    val aggrQuery = Aggregate.sort(Sort.desc("amount"))
      .limit(opsCount)
    val q = operCollection.aggregate[OperationMongo](aggrQuery)
    q.explain(ExplainVerbosity.QUERY_PLANNER).flatMap(expl =>
      ZIO.logInfo(s"PERFORMANCE STATS $expl").flatMap(_ =>
        q.all.map(ol => ol.map(o => toOperations(o)).toList)
      )
    )
  }

  override def dropAll(list: List[Operation]): Task[DeleteResult] = operCollection.deleteMany(
    Filter.in("_id", list.map(i => i.operationId))
  )

  override def operationsPeriodStatisticsByLegalEntity(le: String,
                                                       stratOpt: Option[Instant],
                                                       endOpt: Option[Instant]): Task[Option[OperationsStatistics]] = {
    val now = clock.instant()
    val start = stratOpt.getOrElse(now.truncatedTo(ChronoUnit.DAYS))
    val end = endOpt.getOrElse(now.plus(10, ChronoUnit.DAYS))
    val matchAggr = Aggregate.matchBy(Filter.and(
      Filter.eq("meta.legalEntityId", le),
      Filter.gte("createdAt", start),
      Filter.lte("createdAt", end)
    )).project(Projection().include(Seq("meta", "amount", "createdAt"))).group(
      id = "$meta.legalEntityId", // SQL equivalent: GROUP BY department
      fieldAccumulators = Accumulator.sum("totalAmount", "$amount").sum("count", 1)
    )
    operCollection.aggregate[TmpAggregateResult](matchAggr).first.map(tmp =>
      tmp.map(tmpItem => OperationsStatistics(
        legalEntity = tmpItem._id, periodStart = start, periodEnd = end, count = tmpItem.count,
        totalAmount = MoneyAmount(tmpItem.totalAmount, MoneyAmount.EUR))
      ))
  }


}