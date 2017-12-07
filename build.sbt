crossScalaVersions := Seq("2.12.4", "2.11.12")

scalaVersion in Global := crossScalaVersions.value.head

organization := "com.hypertino"

name := "hyperbus-t-kafka"

version := "0.2-SNAPSHOT"

libraryDependencies ++= Seq(
  "com.hypertino"               %% "hyperbus"                     % "0.5-SNAPSHOT",
  "com.hypertino"               %% "expression-parser"            % "0.2.1",
  "org.apache.kafka"            % "kafka-clients"                 % "0.10.2.0",
  "io.monix"                    %% "monix-kafka-10"               % "0.14",
  "org.scalamock"               %% "scalamock-scalatest-support"  % "3.5.0" % "test",
  "ch.qos.logback"              % "logback-classic"               % "1.2.3" % "test",
  compilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)
)

resolvers ++= Seq(
  Resolver.sonatypeRepo("public")
)