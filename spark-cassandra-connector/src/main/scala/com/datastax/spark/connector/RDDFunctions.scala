package com.datastax.spark.connector


import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.rdd.{CassandraPartitionKeyRDD, CassandraRDD, ValidRDDType, SpannedRDD}
import com.datastax.spark.connector.writer._
import com.datastax.spark.connector.rdd.reader._

import org.apache.spark.SparkContext
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
    * partitions will never be placed in the same group.*/
  def spanBy[U](f: (T) => U): RDD[(U, Iterable[T])] =
    new SpannedRDD[U, T](rdd, f)

  /**
   * Uses the data from `RDD` to get data from a Cassandra table. This function is slightly more liberal than
   * saveToCassandra as you are not required to specify the entire Primary Key. You must specify
   * the entire partition key but clustering columns are optional. If clustering columns are included they must be in
   * an order valid for Cassandra.
   *
   * The flag repartition (default: true) tells Spark whether or not it should shuffle and repartition the data
   * so that the queries are preformed on a coordinator for the data they request. When preforming inner-joins between
   * Cassandra tables this is not neccessary so set this to false so no data shuffling occurs.
   */
  def fetchFromCassandra[R](keyspaceName: String, tableName: String, repartition: Boolean = true, partitionsPerReplicaSet:Int = 10)
                           (implicit connector: CassandraConnector = CassandraConnector(sparkContext.getConf),
                            newType: ClassTag[R], rrf: RowReaderFactory[R], ev: ValidRDDType[R],
                            currentType: ClassTag[T], rwf: RowWriterFactory[T]): CassandraRDD[R] = {
    val cassRdd = new CassandraPartitionKeyRDD[T, R](rdd, keyspaceName, tableName, connector)
    if (repartition) {
      // Todo See if we can automatically determine whether or not we should repartition (prev.class == CassandraRDD and T matches keys of Keyspace,Table)
      cassRdd.partitionByReplica(partitionsPerReplicaSet)
    } else {
      cassRdd
    }
  }


}
