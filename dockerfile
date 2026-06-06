FROM eclipse-temurin:25
MAINTAINER sergeypopov83
COPY target/scala-3.8.3/operationspresenter-assembly-0.1.0-SNAPSHOT.jar app.jar
ENTRYPOINT ["java","-jar","/app.jar"]