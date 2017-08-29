package edu.knoldus.serialization

/**
  * Created by Neelaksh on 22/8/17.
  */

import java.util.logging.Logger
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import edu.knoldus.Models.UserToHashTag
import org.apache.kafka.common.serialization.Serializer

class UserToHashTagSerializer extends Serializer[UserToHashTag] {

  def configure(map: java.util.Map[String, _], b: Boolean): Unit = {
  }

  def serialize(arg0: String, arg1: UserToHashTag): Array[Byte] = {
    val logger = Logger.getLogger("def")
    val objectMapper = new ObjectMapper
    objectMapper.registerModule(DefaultScalaModule)
    try {
      val retVal = objectMapper.writeValueAsString(arg1).getBytes
      logger.info(s"$retVal")
      retVal
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
        new Array[Byte](0)
    }
  }

  def close(): Unit = {
  }
}
