import sbt.*

object Dependencies {

  lazy val grpc: Seq[ModuleID] = Seq(
    "com.thesamet.scalapb.zio-grpc" %% "zio-grpc-codegen" % "0.6.3",
    "io.grpc" % "grpc-netty" % "1.81.0",
    "com.google.api.grpc" % "proto-google-common-protos" % "2.71.0" % "protobuf",
    "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion,
    "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf",
    "com.thesamet.scalapb.common-protos" %% "proto-google-common-protos-scalapb_0.11" % "2.9.6-0",
    "com.thesamet.scalapb.common-protos" %% "proto-google-common-protos-scalapb_0.11" % "2.9.6-0" % "protobuf",
  )

  lazy val logging = Seq(
    "ch.qos.logback.contrib" % "logback-json-classic" % "0.1.5",
    "ch.qos.logback.contrib" % "logback-jackson" % "0.1.5"
  )

  lazy val configDeps: Seq[ModuleID] = Seq(
    "com.typesafe" % "config" % "1.4.8",
  )

  lazy val zio: Seq[ModuleID] = Seq(
    "dev.zio" %% "zio-logging" % "2.5.3",
    "dev.zio" %% "zio-logging-slf4j2" % "2.5.3",
    "dev.zio" %% "zio-config" % "4.0.7",
    "dev.zio" %% "zio-config-magnolia" % "4.0.7",
    "dev.zio" %% "zio-config-typesafe" % "4.0.7",
    "dev.zio" %% "zio-opentelemetry" % "3.1.16",
    "dev.zio" %% "zio-schema" % "1.8.5",
    "dev.zio" %% "zio-schema-bson" % "1.8.5",
    "dev.zio" %% "zio-kafka" % "3.5.0",
    "io.github.kirill5k" %% "mongo4cats-zio" % "0.7.17",
    // Last two are needed for automatic derivation
    "dev.zio" %% "zio-schema-zio-test" % "1.8.5" % Test,
    "dev.zio" %% "zio-test" % "2.1.26" % Test,
    "dev.zio" %% "zio-test-sbt" % "2.1.26" % Test,
  )

  lazy val otel: Seq[ModuleID] = Seq(
    // JVM Telemetry
    "io.opentelemetry" % "opentelemetry-extension-trace-propagators" % "1.62.0",
    "io.opentelemetry" % "opentelemetry-api" % "1.62.0",
    "io.opentelemetry" % "opentelemetry-sdk" % "1.62.0",
    "io.opentelemetry" % "opentelemetry-context" % "1.62.0",
    "io.opentelemetry" % "opentelemetry-exporter-otlp" % "1.62.0",
    "io.opentelemetry" % "opentelemetry-exporter-logging-otlp" % "1.62.0",
    "com.softwaremill.sttp.tapir" %% "tapir-opentelemetry-tracing" % "1.13.19",

    // OpenTelemetry Instrumentation
    "io.opentelemetry.instrumentation" % "opentelemetry-grpc-1.6" % "2.28.1-alpha",
  )

  lazy val tests: Seq[ModuleID] = Seq(
    "org.testcontainers" % "testcontainers-kafka" % "2.0.5" % Test,
    "org.testcontainers" % "testcontainers-mongodb" % "2.0.5" % Test,
    "org.testcontainers" % "testcontainers" % "2.0.5" % Test,
    "org.scalameta" %% "munit" % "1.3.0" % Test
  )

}

