package sergeypopov83.chariot.processing.operations.repository.mongodb

import mongo4cats.models.client.{ConnectionString, MongoClientSettings}
import mongo4cats.zio.{ZMongoClient, ZMongoDatabase}
import sergeypopov83.chariot.processing.operations.utils.configurationEnvSource
import zio.{Config, IO, Scope, URLayer, ZIO, ZLayer}

import java.net.URLEncoder

case class MongoConfiguration(
                               host: String,
                               pwd: String,
                               user: String
                             )

object MongoConfiguration {
  // env parameter names
  val USERNAME = "MONGO_USER"
  val HOST = "MONGO_HOST"
  val PASSWORD = "MONGO_MY_USER_PASSWORD"
  val DEFAULT_DB = "operations"

  // ZIO configuration
  private val configDescription = (Config.string(MongoConfiguration.HOST)
    ++ Config.string(MongoConfiguration.PASSWORD)
    ++ Config.string(MongoConfiguration.USERNAME)
    )
    .map((host, pwd, user) => MongoConfiguration(host = host, pwd = pwd, user = user))

  private def loadMongoConfig(): IO[Config.Error, MongoConfiguration] = {
    configurationEnvSource.load(configDescription)
  }

  def live: ZLayer[Any, Config.Error, MongoConfiguration] = ZLayer.fromZIO(loadMongoConfig())

  val liveDb: URLayer[Scope, ZMongoDatabase] = ZLayer.fromZIO {
    (for {
      mc <- loadMongoConfig()
      // By providing custom MongoClientSettings object
      pwd = URLEncoder.encode(mc.pwd, "UTF-8")
      user = URLEncoder.encode(mc.user, "UTF-8")
      cs = ConnectionString(s"mongodb://$user:$pwd@${mc.host}/$DEFAULT_DB?authSource=admin&directConnection=true")
      settings = MongoClientSettings.builder()
        .applyConnectionString(cs)
        .build()
      client <- ZMongoClient.create(settings)
      database <- client.getDatabase(DEFAULT_DB)
      logger <- ZIO.logInfo(s"MONGO DATABASE $database")
    } yield database).orDie
  }
}