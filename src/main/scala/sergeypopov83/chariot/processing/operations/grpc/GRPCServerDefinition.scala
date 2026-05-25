package sergeypopov83.chariot.processing.operations.grpc

import io.grpc.ServerBuilder
import io.grpc.netty.NettyServerBuilder
import io.grpc.protobuf.services.ProtoReflectionService
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcTelemetry
import sergeypopov83.chariot.processing.operations.utils.ServerConfiguration
import zio.{ZIO, ZLayer}

object GRPCServerDefinition {
  /*
   * @param conf : ServerConfiguration - description of the grpc service connection 
   * @return
   */
  def live: ZLayer[OpenTelemetry & ServerConfiguration, Nothing, NettyServerBuilder] = ZLayer {
    for {
      _ <- ZIO.logInfo("Build GRPC ")
      cfg <- ZIO.service[ServerConfiguration]
      telemetry <- ZIO.service[OpenTelemetry]
    } yield {
      val b = ServerBuilder
        .forPort(cfg.grpc.port.toInt)
      b.addService(ProtoReflectionService.newInstance())
      val grpcTelemetry = GrpcTelemetry.create(telemetry)
      b.intercept(grpcTelemetry.createServerInterceptor())
      b.asInstanceOf[NettyServerBuilder]
    }
  }
}