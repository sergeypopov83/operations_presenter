package sergeypopov83.chariot.processing.operations.repository.mongodb

import com.mongodb.MongoClientSettings
import com.mongodb.client.model.{TimeSeriesGranularity, TimeSeriesOptions}
import com.mongodb.client.result.{DeleteResult, InsertManyResult, InsertOneResult}
import mongo4cats.models.collection.IndexOptions
import mongo4cats.models.database.CreateCollectionOptions
import mongo4cats.operations.{Filter, Index}
import mongo4cats.zio.{ZMongoCollection, ZMongoDatabase}
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.bson.codecs.configuration.{CodecProvider, CodecRegistry}
import org.bson.types.ObjectId
import sergeypopov83.chariot.processing.operations.repository.mongodb.OperationsRepository.{OperationMongo, codecRegistry}
import sergeypopov83.chariot.processing.operations.service.{MoneyAmount, Operation}
import zio.bson.{BsonCodec, zioBsonCodecProvider}
import zio.metrics.Metric
import zio.schema.codec.BsonSchemaCodec
import zio.schema.{Schema, derived}
import zio.{Task, ZIO, ZLayer}

import java.time.Instant

object OperationsRepository {
  trait Service {
    def saveOperation(operation: Operation): Task[InsertOneResult]

    def saveManyOperations(operation: Seq[Operation]): Task[InsertManyResult]
    
    def lastNOperations(opsCount: Int): Task[List[Operation]]

    def topNOperationsByAmount(opsCount: Int): Task[List[Operation]]
    
    def dropAll(list: List[Operation]): Task[DeleteResult] 
      
  }

  case class OperationMongoMeta(
                                 accountId: String,
                                 legalEntityId: String,
                               )

  case class OperationMongo(
                             meta: OperationMongoMeta,
                             operationId: ObjectId,
                             operationType: String,
                             status: String,
                             createdAt: Instant,
                             amount: BigDecimal,
                           ) derives Schema

  given objectIdSchema: Schema[ObjectId] =
    Schema[String].transform(
      str => new ObjectId(str),
      id => id.toString
    )
  
  // this is bson codec to store and retrieve data from mongo
  given bsonCodec: BsonCodec[OperationMongo] = BsonSchemaCodec.bsonCodec(OperationMongo.derived$Schema)

  val myDataCodecProvider: CodecProvider = zioBsonCodecProvider[OperationMongo]
  val codecRegistry: CodecRegistry = fromRegistries(
    MongoClientSettings.getDefaultCodecRegistry,
    fromProviders(myDataCodecProvider)
  )

  def fromOperations(trxn: Operation): OperationMongo = OperationMongo(
    meta = OperationMongoMeta(trxn.accountId, trxn.legalEntityId),
    operationType = trxn.operationType,
    status = trxn.status,
    createdAt = trxn.createdAt,
    amount = trxn.amount.amount,
    operationId = ObjectId(trxn.operationId)
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

  def makeService: ZIO[ZMongoDatabase, Throwable, OperationsRepositoryLive] = for {
    db <- ZIO.service[ZMongoDatabase]
    operCollection <- makeTimeSeriesCollection
  } yield new OperationsRepositoryLive(db, operCollection)

  def makeTimeSeriesCollection: ZIO[ZMongoDatabase, Throwable, ZMongoCollection[Operation]] = for {
    db <- ZIO.service[ZMongoDatabase]
    list <- db.listCollectionNames
    _ <- ZIO.ifZIO(ZIO.succeed(!list.toList.contains(MongoConfiguration.DEFAULT_COLLECTION))).apply(db.createCollection(MongoConfiguration.DEFAULT_COLLECTION, CreateCollectionOptions()
      .timeSeriesOptions(
        TimeSeriesOptions("createdAt").metaField("createdAt").granularity(TimeSeriesGranularity.MINUTES)
      )), ZIO.succeed(()))
    col <- db.getCollection[Operation](MongoConfiguration.DEFAULT_COLLECTION, codecRegistry)
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
  val live: ZLayer[ZMongoDatabase, Throwable, OperationsRepositoryLive] =
    ZLayer.fromZIO {
      makeService
    }
}

class OperationsRepositoryLive(mongoDatabase: ZMongoDatabase, operCollection: ZMongoCollection[Operation]) extends OperationsRepository.Service {

  override def saveOperation(operation: Operation): Task[InsertOneResult] = operCollection.insertOne(operation)

  override def saveManyOperations(operation: Seq[Operation]): Task[InsertManyResult] = operCollection.insertMany(operation)

  override def lastNOperations(opsCount: Int): Task[List[Operation]] = ???

  override def topNOperationsByAmount(opsCount: Int): Task[List[Operation]] = ???

  override def dropAll(list: List[Operation]): Task[DeleteResult] = operCollection.deleteMany(
    Filter.in("_id", list.map(_.operationId))
  )
    

}