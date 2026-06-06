package sergeypopov83.chariot.processing.operations.utils

import zio.ZIO
import zio.test.*

object UtilsSuite extends ZIOSpecDefault {

  override def spec: Spec[Environment, Any] =
    suite("myConfig()")(
      test("loads config from environment when present") {
        for {
          _ <- TestSystem.putEnv("GRPC_PORT", "8080")
          _ <- TestSystem.putEnv("TELEMETRY_HOST", "127.0.0.1")
          result <- myConfig()
          _ <- ZIO.logInfo("Testing with env: " + result)
        } yield assertTrue(result == ServerConfiguration(GRPC("8080"), TelemetryConfig("127.0.0.1")))
      },
      test("loads config partially from environment when present") {
        for {
          _ <- TestSystem.putEnv("TELEMETRY_HOST", "127.0.0.1")
          result <- myConfig()
          _ <- ZIO.logInfo("Testing with env: " + result)
        } yield assertTrue(result == ServerConfiguration(GRPC("9090"), TelemetryConfig("127.0.0.1")))
      },
      test("falls back to defaults when env vars are missing") {
        for {
          result <- myConfig()
        } yield assertTrue(result == ServerConfiguration(GRPC("9090"), TelemetryConfig("0.0.0.0")))
      }
    )
}