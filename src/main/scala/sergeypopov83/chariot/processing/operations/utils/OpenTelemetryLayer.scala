package sergeypopov83.chariot.processing.operations.utils

import io.opentelemetry.api.common.Attributes
import io.opentelemetry.context.propagation.ContextPropagators
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter
import io.opentelemetry.extension.trace.propagation.B3Propagator
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.metrics.`export`.PeriodicMetricReader
import io.opentelemetry.sdk.resources.Resource
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.`export`.BatchSpanProcessor
import io.opentelemetry.semconv.ServiceAttributes
import zio.*

object OpenTelemetryLayer {

  private val resourceName = "OperationsPresenter"

  private def httpTracerProvider(resourceName: String): Task[SdkTracerProvider] = ZIO.attempt(SdkTracerProvider.builder()
    .addSpanProcessor(BatchSpanProcessor.builder(OtlpHttpSpanExporter.builder()
      .setEndpoint("http://127.0.0.1:4318/v1/traces").build()
    ).build())
    .setResource(
      io.opentelemetry.sdk.resources.Resource.getDefault.merge(
        io.opentelemetry.sdk.resources.Resource.create(
          Attributes.builder()
            .put(ServiceAttributes.SERVICE_NAME, resourceName)
            .put("service.version", "1.0.0")
            .build()
        )
      )
    )
    .build())

  /**
   * Exports metrics in GRPC
   */
  private def httpMeterProvider(resourceName: String): RIO[Scope, SdkMeterProvider] =
    for {
      metricExporter <- ZIO.succeed(OtlpHttpMetricExporter.builder()
        .setEndpoint("http://127.0.0.1:4318/v1/metrics")
        .build())
      metricReader <-
        ZIO.fromAutoCloseable(
          ZIO.succeed(PeriodicMetricReader.builder(metricExporter)
            .setInterval(durationInt(5).second).build())
        )
      meterProvider <-
        ZIO.fromAutoCloseable(
          ZIO.succeed(
            SdkMeterProvider
              .builder()
              .registerMetricReader(metricReader)
              .setResource(Resource.create(Attributes.of(ServiceAttributes.SERVICE_NAME, resourceName)))
              .build()
          )
        )
    } yield {
      meterProvider
    }

  /*
     val sdk = OpenTelemetrySdk.builder()
        val metricExporter = OtlpGrpcMetricExporter.getDefaultBuilder.build
        val metricReader = PeriodicMetricReader.builder(metricExporter)
          .setInterval(Duration.ofSeconds(30))
          .build
   */
  val live: ZLayer[Any, Throwable, OpenTelemetrySdk] = ZLayer.scoped {
    ZIO.acquireRelease {
      for {
        meterProvider <- httpMeterProvider(resourceName)
        tracerProvider <- httpTracerProvider(resourceName)
        openTelemetrySdk <- ZIO.fromAutoCloseable(
          ZIO.succeed(
            OpenTelemetrySdk
              .builder()
              .setTracerProvider(tracerProvider)
              .setMeterProvider(meterProvider)
              //                 .setLoggerProvider(loggerProvider)
              //Would enable trace propagators only after traces are enabled
              .setPropagators(ContextPropagators.create(B3Propagator.injectingSingleHeader()))
              .build()
          )
        )
      } yield {
        // Also enable JVM metrics
        openTelemetrySdk
      }
    } { sdk =>
      ZIO.logInfo("Shutting down OpenTelemetry SDK...") *>
        ZIO.succeed(sdk.close()) // This shuts down tracer/meter providers
    }
  }
}

