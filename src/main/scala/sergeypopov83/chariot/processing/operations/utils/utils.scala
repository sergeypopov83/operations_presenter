package sergeypopov83.chariot.processing.operations.utils

import zio.config.magnolia.deriveConfig
import zio.{Config, ConfigProvider, IO, ULayer, ZIO, ZLayer}

type AuthKey = String

final case class GRPC(port: String)

final case class ServerConfiguration(grpc: GRPC)

val serverDescription = deriveConfig[ServerConfiguration]

val configurationEnvSource = ConfigProvider.fromEnv()

def myConfig(): IO[Config.Error, ServerConfiguration] =
  configurationEnvSource.load(serverDescription).orElse(
    for {
      _ <- ZIO.logInfo("Fallback on default values")
    } yield {
      ServerConfiguration(GRPC("9090"))
    }
  )

def serverConfigurationLive: ULayer[ServerConfiguration] = ZLayer {
  myConfig().orDie
}