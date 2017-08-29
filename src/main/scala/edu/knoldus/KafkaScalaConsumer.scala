package edu.knoldus

import java.util.logging.Logger
import java.util.{Collections, Properties}
import edu.knoldus.Models.UserToHashTag
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import scala.collection.JavaConverters._
import scala.concurrent.duration._


class KafkaScalaConsumer(cassandraProvider: CassandraProvider, runtime:Long,pollTime:Long) {
  private val logger = Logger.getLogger(s"${this.getClass.getName}")

  def props(): Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "edu.knoldus.serialization.UserToHashTagDeserializer")
    props.put("group.id", "consumer-group-1")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("auto.offset.reset", "earliest")
    props.put("session.timeout.ms", "30000")
    props.put("-consumer-timeout-ms","30000")
    props
  }

  def generateConsumer(subscribeTo: String, props: Properties): KafkaConsumer[Nothing, UserToHashTag] = {
    val consumer: KafkaConsumer[Nothing, UserToHashTag] = new KafkaConsumer[Nothing, UserToHashTag](props)
    consumer.subscribe(Collections.singletonList(subscribeTo))
    consumer
  }

  def storeHashTags(topic: String): Unit = {
    val consumer = generateConsumer(topic, this.props())
    val cassandraConn = cassandraProvider.cassandraConn
    val deadline = runtime.seconds.fromNow
    try {
      while(deadline.hasTimeLeft) {
        val records: ConsumerRecords[Nothing, UserToHashTag] = consumer.poll(pollTime)
        cassandraConn.execute(s"CREATE TABLE IF NOT EXISTS hashtags (name text, hashtag text, PRIMARY KEY((name),hashtag))")
        for (record <- records.asScala) {
          logger.info(record.value().toString)
          cassandraConn.execute(s"INSERT INTO hashtags(name,hashtag) VALUES ('${record.value().userName}','${record.value().data}')")
        }
      }
    }
    catch {
      case ex: Throwable => logger.warning(s"$ex")
    }
    finally {
      cassandraConn.close()
    }
  }
}

object KafkaScalaConsumer extends App with CassandraProvider {
  val topic = "feed"
  val kafkaScalaConsumer = new KafkaScalaConsumer(new {} with CassandraProvider {},10,100)
  kafkaScalaConsumer.storeHashTags(topic)
  logger.info("bye")

}
