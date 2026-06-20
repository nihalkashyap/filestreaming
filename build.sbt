import Dependencies.*

val PekkoVersion = "1.6.0"

ThisBuild / scalaVersion     := "2.13.18"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val root = (project in file("."))
  .settings(
    name := "FileStreaming",
    libraryDependencies ++= Seq(
      munit % Test,
      "org.apache.pekko" %% "pekko-stream-typed" % PekkoVersion,
      "ch.qos.logback" % "logback-classic" % "1.5.34"
    )
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
