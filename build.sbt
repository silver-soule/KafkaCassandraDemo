name := "kafkamainassignment"

version := "1.0"

scalaVersion := "2.12.2"

libraryDependencies ++= Seq(
  "com.datastax.cassandra" % "cassandra-driver-extras" % "3.0.0",
  "com.typesafe" % "config"% "1.3.1",
  "org.twitter4j" % "twitter4j-core" % "4.0.6",
  "org.apache.kafka" % "kafka-clients" % "0.11.0.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.8.2",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.8.9",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.9",
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.12" % "2.8.9"
)
