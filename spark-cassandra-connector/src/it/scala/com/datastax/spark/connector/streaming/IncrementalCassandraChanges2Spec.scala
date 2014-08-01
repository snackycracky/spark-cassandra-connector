package com.datastax.spark.connector.streaming

import java.util.UUID

import akka.actor._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.util.SparkServer
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Random

class IncrementalCassandraChanges2Spec extends ActorSpec {
  val KS = "incremental_cassandra_changes_spec"

  override val actorName = UUID.randomUUID().toString

  before {
    CassandraConnector(SparkServer.conf).withSessionDo { session =>
      session.execute(s"DROP KEYSPACE IF EXISTS $KS")
      session.execute(s"CREATE KEYSPACE $KS WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
      session.execute(s"CREATE TABLE $KS.dict (word TEXT PRIMARY KEY, translation TEXT)")
      session.execute(s"CREATE TABLE $KS.events (id TIMESTAMP PRIMARY KEY, word TEXT, translation TEXT)")
    }
  }

  "cassandraRDD" must {
    "be refreshed with every access even if cached and then upersisted" in {
      val stream = ssc.actorStream[(Long, String)](Props(new SparkStreamingActor {
        def receive = {
          case e: String => pushBlock((System.currentTimeMillis(), e))
        }
      }), actorName, StorageLevel.MEMORY_AND_DISK)

      val dictRDD = ssc.cassandraTable[(String, String)](KS, "dict").select("word", "translation").cache()
      // do something on dictRDD
      dictRDD.count()
      dictRDD.unpersist()

      stream
        .map(_.swap)
        .transform(rdd => rdd.leftOuterJoin(dictRDD))
        .map { case (word, (id, translation)) => (id, word, translation)}
        .saveToCassandra(KS, "events", Seq("id", "word", "translation"))

      ssc.start()
      val future = system.actorSelection(s"$system/user/Supervisor0/$actorName").resolveOne()
      awaitCond(future.isCompleted)

      val startTime = System.currentTimeMillis()

      // schedule sending random fruit every 100ms during 5 seconds
      val words = Seq("apple", "orange", "peach", "pineapple", "strawberry")
      for (actor <- future) {
        schedule(actor, 5 seconds, 0 seconds, 100 milliseconds) {
          words(Random.nextInt(words.length))
        }
        watch(actor)
      }

      // after 3 seconds add the dictionary entry for each fruit
      system.scheduler.scheduleOnce(3 seconds) {
        CassandraConnector(SparkServer.conf) withSessionDo { session =>
          session.execute(s"INSERT INTO $KS.dict (word, translation) VALUES (?, ?)", "apple", "pink")
          session.execute(s"INSERT INTO $KS.dict (word, translation) VALUES (?, ?)", "orange", "orange")
          session.execute(s"INSERT INTO $KS.dict (word, translation) VALUES (?, ?)", "peach", "green")
          session.execute(s"INSERT INTO $KS.dict (word, translation) VALUES (?, ?)", "pineapple", "yellow")
          session.execute(s"INSERT INTO $KS.dict (word, translation) VALUES (?, ?)", "strawberry", "red")
        }
      }

      // test what happened after 7 seconds
      val rows = expectMsgPF(7 seconds) {
        case Terminated(_) => {
          ssc.stop(stopSparkContext = false)
          CassandraConnector(SparkServer.conf) withSessionDo { session =>
            session.execute(s"SELECT id, word, translation FROM $KS.events").all()
              .map(CassandraRow.fromJavaDriverRow(_, Array("id", "word", "translation")))
              .map(row => (row.getLong("id"), row.getString("word"), row.getStringOption("translation")))
              .sortBy(_._1)

          }
        }
      }

      rows.foreach {
        case (id, word, translation) => println(f"${(id - startTime)/1000d}%-1.2f $word%-11s => $translation%s")
      }

      rows.size should be > 2
      rows.head._3 should be (None)
      rows.last._3 should not be None
    }
  }

}






