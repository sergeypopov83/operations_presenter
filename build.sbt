import Dependencies.*
import sbt.Keys.libraryDependencies
import sbtassembly.AssemblyKeys.assembly

val scala3Version = "3.8.3"

lazy val root = project
  .in(file("."))
  .settings(
    name := "operationspresenter",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,

    libraryDependencies += "org.scalameta" %% "munit" % "1.3.2" % Test,
    libraryDependencies ++= grpc,
    libraryDependencies ++= logging,
    libraryDependencies ++= tests,
    libraryDependencies ++= otel,
    libraryDependencies ++= zio,
    libraryDependencies ++= misc,

    // assembly
    assembly / mainClass := Some("sergeypopov83.chariot.processing.operations.Application"),
    assembly / assemblyMergeStrategy := {
      case x if x.endsWith("module-info.class") => MergeStrategy.discard
      case x if x.endsWith("native-image.properties") => MergeStrategy.first
      case x if x.endsWith("io.netty.versions.properties") => MergeStrategy.first
      case x if x.endsWith("native-image/reflect-config.json") => MergeStrategy.first
      case x if x.endsWith("OSGI-INF/MANIFEST.MF") => MergeStrategy.first
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    }
  )

Compile / PB.targets := Seq(
  scalapb.gen(grpc = true) -> (Compile / sourceManaged).value / "scalapb",
  scalapb.zio_grpc.ZioCodeGenerator -> (Compile / sourceManaged).value / "scalapb"
)

Compile / PB.protoSources := Seq((ThisBuild / baseDirectory).value / "src" / "main" / "resources" / "protos")

//sergeypopov83.chariot.processing.sponsorbank.utils.LocalFunctionalTest
Test / testOptions += Tests.Argument("-l", "sergeypopov83.chariot.processing.sponsorbank.utils.LocalFunctionalTest")


