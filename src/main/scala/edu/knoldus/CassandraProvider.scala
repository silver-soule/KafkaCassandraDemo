package edu.knoldus

/**
  * Created by Neelaksh on 22/8/17.
  */

import java.util.logging.Logger
import com.datastax.driver.core.{Cluster, Session}
import com.typesafe.config.ConfigFactory

trait CassandraProvider {
  protected val logger: Logger = Logger.getLogger(this.getClass.toString)
  private val config = ConfigFactory.load()
  private val cassandraKeySpace = config.getString("cassandra.keyspace")
  private val cassandraHostname = config.getString("cassandra.contact.points")
  val cassandraConn: Session = {
    val cluster = new Cluster.Builder().withClusterName("Test Cluster").
      addContactPoints(cassandraHostname).build
    val session = cluster.connect
    logger.info(s"creating keyspace")
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS  $cassandraKeySpace WITH REPLICATION = " +
      s"{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
    session.execute(s"USE $cassandraKeySpace")
    session
  }
}
