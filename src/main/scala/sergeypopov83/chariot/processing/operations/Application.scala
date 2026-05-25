package sergeypopov83.chariot.processing.operations


import io.grpc.netty.NettyServerBuilder
import scalapb.zio_grpc.{ScopedServer, ServiceList}
import sergeypopov83.chariot.processing.operations.grpc.{GRPCServerDefinition, OperationsGrpcServiceImpl}
import sergeypopov83.chariot.processing.operations.utils.{OpenTelemetryLayer, serverConfigurationLive}
import sttp.tapir.server.tracing.opentelemetry.OpenTelemetryTracingConfig.Defaults.instrumentationScopeName
import zio.Clock.ClockLive
import zio.internal.LoomSupport
import zio.logging.backend.SLF4J
import zio.metrics.jvm.DefaultJvmMetrics
import zio.telemetry.opentelemetry.OpenTelemetry as ZOpenTelemetry
import zio.{Clock, Duration, Runtime, Scope, URLayer, ZIO, ZIOAppArgs, ZIOAppDefault, ZLayer}

import java.time.Clock as JClock

object Application extends ZIOAppDefault:

  val logger: URLayer[Any, Unit] = Runtime.removeDefaultLoggers >>> SLF4J.slf4j
  val systemClock: URLayer[Any, Clock & JClock] = ZLayer.succeed {
    ClockLive
  } <*> ZLayer.succeed {
    JClock.systemUTC()
  }

  private def servers = ZIO.acquireRelease(
    for {_ <- ZIO.logInfo("Start app ...")
         grpc <- ZIO.service[NettyServerBuilder]
         } yield {
      grpc
    }
  )(r =>
    ZIO.logInfo("Releasing resource (3s) ...") *> ZIO.sleep(Duration.fromSeconds(3))
  )

  private type ApplicationOutType = OperationsGrpcServiceImpl
  private val live: ZLayer[Scope, Any, ApplicationOutType] = ZLayer {
    for {
      s <- ZIO.succeed(OperationsGrpcServiceImpl())
    } yield {
      s
    }
  }

  override val bootstrap: ZLayer[Any, LoomSupport.LoomNotAvailableException, Unit] =
    Runtime.enableLoomBasedExecutor

  override def run: ZIO[ZIOAppArgs & Scope, Any, Any] = ZIO.scope.flatMap(s => {
    ZIO.scopedWith { scope =>
      for {
        grpc <- servers
        operationsGrpc <- ZIO.service[OperationsGrpcServiceImpl]
        server <- ScopedServer
          .fromServiceList(grpc, ServiceList.add(operationsGrpc))
        _ <- server.awaitTermination
        _ <- ZIO.logInfo("Finishing the service")
      } yield ()
    }.provide(live,
      serverConfigurationLive,
      GRPCServerDefinition.live,
      OpenTelemetryLayer.live,
      //      ZOpenTelemetry.tracing(instrumentationScopeName),
      ZOpenTelemetry.metrics(instrumentationScopeName),
      ZOpenTelemetry.logging(instrumentationScopeName),
      //        ZOpenTelemetry.baggage(),
      ZOpenTelemetry.zioMetrics,
      ZOpenTelemetry.contextZIO,
      DefaultJvmMetrics.liveV2.unit,
      ZLayer.succeed(s))
  })

end Application
