package edu.knoldus

import java.util.{Collections, Properties}
import edu.knoldus.Models.UserToHashTag
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import scala.collection.JavaConverters._

object KafkaScalaConsumer extends App with CassandraProvider {

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "edu.knoldus.serialization.UserToHashTagDeserializer")
  props.put("group.id", "consumer-group-1")
  props.put("enable.auto.commit", "true")
  props.put("auto.commit.interval.ms", "1000")
  props.put("auto.offset.reset", "earliest")
  props.put("session.timeout.ms", "30000")
  val topic = "feed"

  val consumer: KafkaConsumer[Nothing, UserToHashTag] = new KafkaConsumer[Nothing, UserToHashTag](props)

  consumer.subscribe(Collections.singletonList(topic))

  while (true) {
    val records: ConsumerRecords[Nothing, UserToHashTag] = consumer.poll(100)
    cassandraConn.execute(s"CREATE TABLE IF NOT EXISTS hashtags (name text, hashtag text, PRIMARY KEY((name),hashtag))")
    for (record <- records.asScala) {
      try {
        logger.info(record.value().toString)
        cassandraConn.execute(s"INSERT INTO hashtags(name,hashtag) VALUES ('${record.value().userName}','${record.value().data}')")
      }
    }
    // kafka wont read all record together, dont close the connection!!
    //cassandraConn.close()
  }
}
