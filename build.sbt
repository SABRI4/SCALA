name := "CarrefourIntelligent"
version := "0.1"
scalaVersion := "2.13.12" // Version stable pour Akka

val AkkaVersion = "2.6.21" // Version compatible avec votre étude

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor-typed" % AkkaVersion,
  "com.typesafe.akka" %% "akka-actor-testkit-typed" % AkkaVersion % Test,
  "org.scalatest" %% "scalatest" % "3.2.15" % Test
)