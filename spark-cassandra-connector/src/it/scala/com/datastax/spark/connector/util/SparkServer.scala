package com.datastax.spark.connector.util

import org.apache.spark.{SparkEnv, SparkConf, SparkContext}

import scala.util.Try

trait SparkServer {
  val conf = SparkServer.conf
  val sc = SparkServer.createSparkContext
}

object SparkServer {
  val conf = new SparkConf(true)
    .set("spark.cassandra.connection.host", CassandraServer.cassandraHost.getHostAddress)
    .set("spark.cleaner.ttl", "3600")   // required for Spark Streaming
  val sparkMasterUrl = Option(System.getenv("IT_TEST_SPARK_MASTER")).getOrElse("local[4]")



  var sc: SparkContext = null
  def restart() {
    createSparkContext
  }

  def createSparkContext = {
    Try {sc.stop()}
    sc = new SparkContext(sparkMasterUrl, "Integration Test", conf)
    sc
  }
  def actorSystem = SparkEnv.get.actorSystem
}
