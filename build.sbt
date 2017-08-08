crossScalaVersions := Seq("2.12.1", "2.11.8")

scalaVersion in Global := "2.11.8"

organization := "com.hypertino"

name := "hyperbus-t-kafka"

version := "0.2-SNAPSHOT"

libraryDependencies ++= Seq(
  "com.hypertino"   %% "hyperbus"        % "0.2-SNAPSHOT",
  "org.apache.kafka" % "kafka-clients" % "0.10.2.0",
  "io.monix" %% "monix-kafka-10" % "0.14",
  "org.scalamock"   %% "scalamock-scalatest-support" % "3.5.0" % "test",
  compilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)
)

resolvers ++= Seq(
  Resolver.sonatypeRepo("public")
)