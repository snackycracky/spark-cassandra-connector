package com.datastax.spark.connector


import java.net.InetAddress

import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.rdd.partitioner.ReplicaPartitioner
import com.datastax.spark.connector.rdd.reader._
import com.datastax.spark.connector.rdd.{CassandraJoinRDD, SpannedRDD, ValidRDDType}
import com.datastax.spark.connector.writer._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag


/** Provides Cassandra-specific methods on `RDD` */
class RDDFunctions[T](rdd: RDD[T]) extends WritableToCassandra[T] with Serializable {

  override val sparkContext: SparkContext = rdd.sparkContext

  /**
   * Saves the data from `RDD` to a Cassandra table. Uses the specified column names.
   * @see [[com.datastax.spark.connector.writer.WritableToCassandra]]
   */
  def saveToCassandra(keyspaceName: String,
                      tableName: String,
                      columns: ColumnSelector = AllColumns,
                      writeConf: WriteConf = WriteConf.fromSparkConf(sparkContext.getConf))
                     (implicit connector: CassandraConnector = CassandraConnector(sparkContext.getConf),
                      rwf: RowWriterFactory[T]): Unit = {
    val writer = TableWriter(connector, keyspaceName, tableName, columns, writeConf)
    rdd.sparkContext.runJob(rdd, writer.write _)
  }

  /** Applies a function to each item, and groups consecutive items having the same value together.
    * Contrary to `groupBy`, items from the same group must be already next to each other in the
    * original collection. Works locally on each partition, so items from different
    * partitions will never be placed in the same group. */
  def spanBy[U](f: (T) => U): RDD[(U, Iterable[T])] =
    new SpannedRDD[U, T](rdd, f)

  /**
   * Uses the data from `RDD` to get data from a Cassandra table. This function is slightly more liberal than
   * saveToCassandra as you are not required to specify the entire Primary Key. You must specify
   * the entire partition key but clustering columns are optional. If clustering columns are included they must be in
   * an order valid for Cassandra.
   */
  def joinWithCassandraTable[R](keyspaceName: String, tableName: String)
                           (implicit connector: CassandraConnector = CassandraConnector(sparkContext.getConf),
                            newType: ClassTag[R], rrf: RowReaderFactory[R], ev: ValidRDDType[R],
                            currentType: ClassTag[T], rwf: RowWriterFactory[T]): CassandraJoinRDD[T, R] = {
    new CassandraJoinRDD[T, R](rdd, keyspaceName, tableName, connector)
  }


  /**
   * Return a new ShuffledRDD that is made by taking the current RDD and repartitioning it
   * with the Replica Partitioner.
   **/
  def repartitionByCassandraReplica(keyspaceName: String, tableName: String, partitionsPerReplicaSet: Int = 10)
                                   (implicit connector: CassandraConnector = CassandraConnector(sparkContext.getConf),
                                    currentType: ClassTag[T], rwf: RowWriterFactory[T]) = {
    val part = new ReplicaPartitioner(partitionsPerReplicaSet, connector)
    val repart = rdd.keyByCassandraReplica(keyspaceName, tableName).partitionBy(part)
    val output = repart.mapPartitions(_.map(_._2), preservesPartitioning = true)
    output
  }

  def keyByCassandraReplica(keyspaceName: String, tableName: String)
                           (implicit connector: CassandraConnector = CassandraConnector(sparkContext.getConf),
                            currentType: ClassTag[T], rwf: RowWriterFactory[T]): RDD[(Set[InetAddress], T)] = {
    val converter = ReplicaMapper[T](connector, keyspaceName, tableName)
    rdd.mapPartitions(primaryKey =>
      converter.keyByReplicas(primaryKey)
    )
  }


}
