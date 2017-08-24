package edu.knoldus.serialization

/**
  * Created by Neelaksh on 22/8/17.
  */

import java.io.ByteArrayOutputStream
import java.util.logging.Logger

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import edu.knoldus.Models.UserToHashTag
import org.apache.kafka.common.serialization.Deserializer

class UserToHashTagDeserializer extends Deserializer[UserToHashTag] {
  val logger = Logger.getLogger("des")
  def close(): Unit = {
  }

  def configure(arg0: java.util.Map[String, _], arg1: Boolean): Unit = {
  }

  def deserialize(arg0: String, arg1: Array[Byte]): UserToHashTag = {
    val mapper = new ObjectMapper with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    var userToHashTag: UserToHashTag = null
    try {
      userToHashTag = mapper.readValue(arg1,classOf[UserToHashTag])
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
    userToHashTag
  }
}