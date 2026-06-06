package sergeypopov83.chariot.processing.operations.utils

import zio.{Config, ConfigProvider, IO, ULayer, ZIO, ZLayer}

type AuthKey = String

final case class GRPC(port: String)

final case class TelemetryConfig(host: String)

final case class ServerConfiguration(grpc: GRPC, telemetry: TelemetryConfig)

val grpcPortConfig =
  Config.string("GRPC_PORT").withDefault("9090").map(GRPC(_))

val telemetryHostConfig =
  Config.string("TELEMETRY_HOST").withDefault("0.0.0.0").map(TelemetryConfig(_))

val serverDescription =
  (grpcPortConfig ++ telemetryHostConfig).map((g, t) => ServerConfiguration(g, t))

val configurationEnvSource = ConfigProvider.fromEnv()

def myConfig(): IO[Config.Error, ServerConfiguration] =
  configurationEnvSource.load(serverDescription)

def serverConfigurationLive: ULayer[ServerConfiguration] = ZLayer {
  myConfig().orDie
}