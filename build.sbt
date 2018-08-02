name := "akka-quorum"

version := "1.0"

scalaVersion := "2.12.6"

lazy val akkaVersion = "2.5.14"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "com.github.scopt" %% "scopt" % "3.7.0",
  "com.github.nscala-time" %% "nscala-time" % "2.20.0"
)
