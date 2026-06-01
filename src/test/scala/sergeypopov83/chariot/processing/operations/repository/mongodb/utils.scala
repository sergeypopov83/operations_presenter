package sergeypopov83.chariot.processing.operations.repository.mongodb

import mongo4cats.zio.ZMongoDatabase
import zio.test.TestSystem
import zio.{Scope, UIO, ZIO, ZLayer}

val setEnvZIO: ZIO[Any, Nothing, Unit] = {
  TestSystem.putEnv(MongoConfiguration.USERNAME, "gen_user").flatMap { _ =>
    TestSystem.putEnv(MongoConfiguration.PASSWORD, "0zQgv:Hz8&*qJK")
  }.flatMap { _ =>
    TestSystem.putEnv(MongoConfiguration.HOST, "194.87.43.35:27017")
  }
}

def testScope: UIO[Scope.Closeable] = Scope.make
// Connection to the locally deployed mongo
val mockMongoDatabase: ZLayer[Scope, Nothing, ZMongoDatabase] = {
  ZLayer.fromZIO(setEnvZIO) >>> MongoConfiguration.liveDb
}