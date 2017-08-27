package edu.knoldus

import java.util.Properties
import java.util.logging.Logger
import edu.knoldus.Models.{UserToHashTag, UserToHashTagList}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import twitter4j.TwitterFactory
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}


class KafkaScalaProducer() {
  val logger = Logger.getLogger(this.getClass.toString)

  def getProps(): Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("client.id", "ScalaProducerExample")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "edu.knoldus.serialization.UserToHashTagSerializer")
    props.put("acks", "0")
    props.put("retries", "0")
    props.put("batch.size", "16384")
    props.put("linger.ms", "1")
    props.put("buffer.memory", "33554432")
    props
  }

  def sendHashTagDataToStream(data: Future[UserToHashTagList], producer: KafkaProducer[Nothing, UserToHashTag], topic: String): Unit = {
    data.onComplete {
      case Success(userNameToHashTags) =>
        val userName = userNameToHashTags.userName
        userNameToHashTags.data.foreach {
          hashtag =>
            val record: ProducerRecord[Nothing, UserToHashTag] = new ProducerRecord(topic, UserToHashTag(userName, hashtag))
            producer.send(record)
        }
      case Failure(ex) => logger.warning(ex.toString)
    }
  }
}

object KafkaScalaProducer extends App {
  val SLEEPTIME = 10000
  val log = Logger.getLogger(this.getClass.toString)
  if (args.length == 1) {
    val topic = "feed"
    log.info(s"Sending Records in Kafka Topic [$topic]")
    val tweetFetcher = new TweetFetcher(new TwitterFactory())
    val kafkaScalaProducer = new KafkaScalaProducer()
    val producer: KafkaProducer[Nothing, UserToHashTag] = new KafkaProducer[Nothing, UserToHashTag](kafkaScalaProducer.getProps())
    val tweets = tweetFetcher.getUniqueHashTags(args(0).trim)
    kafkaScalaProducer.sendHashTagDataToStream(tweets, producer, topic)
    Thread.sleep(SLEEPTIME)
    producer.close()
  }
  else {
    log.info(s"please give a valid screen name")
  }
}
