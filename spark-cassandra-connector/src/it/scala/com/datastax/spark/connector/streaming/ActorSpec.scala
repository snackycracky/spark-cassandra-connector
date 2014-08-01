package com.datastax.spark.connector.streaming

import akka.actor.{ActorRef, ActorSystem, PoisonPill}
import akka.testkit.TestKit
import com.datastax.spark.connector.util.{CassandraServer, SparkServer}
import org.apache.spark.SparkEnv
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

abstract class ActorSpec(val ssc: StreamingContext, _system: ActorSystem) extends TestKit(_system) with StreamingSpec
with CassandraServer {
  def this() = this({SparkServer.restart(); new StreamingContext(SparkServer.sc, Milliseconds(300))}, SparkEnv.get.actorSystem)

  useCassandraConfig("cassandra-default.yaml.template")

  after {
    // Spark Context is shared among all integration test so we don't want to stop it here
    ssc.stop(stopSparkContext = false)
  }

  def schedule[T](to: ActorRef, lifeTime: FiniteDuration, delay: FiniteDuration, interval: FiniteDuration)(f: => T)(implicit system: ActorSystem, context: ExecutionContext) {
    val task = system.scheduler.schedule(delay, interval) {
      to ! f
    }

    system.scheduler.scheduleOnce(lifeTime) {
      task.cancel()
      to ! PoisonPill
    }
  }

}
