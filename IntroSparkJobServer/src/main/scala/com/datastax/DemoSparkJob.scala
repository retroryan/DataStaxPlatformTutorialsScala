/**
 *  Copyright DataStax, Inc.
 *
 *
 *   THIS IS FROM THE YET RELEASED DSE Spark Job Server Demos by Russ S. and copied here for testing
 *
 *   Please see the DataStax Enterprise license files for details.
 **/

/**
 *
 *   A demonstration jar which provides access to several standard Spark C*
 *   functions via an http interface using the Spark Job Server. This application
 *   lets the user submit the following jobs.
 *
 * 1. Copy a table to another Keyspace
 * 2. Count all the elements in a table
 * 3. Create a dummy table with 1000 partition keys
 **/
package com.datastax

//Avoid namespace collision with org.apache.spark in DSE Build
import _root_.spark.jobserver._

import com.datastax.driver.core.DataType
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.types._

import org.apache.spark.SparkContext
import com.typesafe.config.Config

import scala.util.Try


case class DemoRow ( customer: String, order: String, amount: Int )


object DemoSparkJob extends SparkJob with NamedRddSupport {

  /**
   * The main entry point for launching the Spark Job Server application. In this case we use
   * this as a switch which calls the correct subprogram based on the action provided in the
   * HTTP request.
   */
  override def runJob(sc: SparkContext, jobConfig: Config): Any = {
    jobConfig.getString("action").toLowerCase match {
      case "create" => demoCreate(sc, jobConfig)
      case "cache" => demoCache(sc, jobConfig)
      case "uncache" => demoUncache(sc, jobConfig)
      case "sum" => demoSum(sc, jobConfig)
      case _ => throw  new IllegalArgumentException("Invalid Action")
    }
  }


  /**
   * Validates that the passed configuration is valid for
   * the create function. Checks whether the keyspace to
   * have the table created in exists and that we have a
   * valid number of rows if given.
   */
  def demoCreateValidate(sc: SparkContext, jobConfig: Config): SparkJobValidation = {

    try {

      val keyspaceName = jobConfig.getString("keyspace")
      val tableName = jobConfig.getString("table")
      val rowCount = Try(jobConfig.getInt("rows")).getOrElse(1000)

      val metadata = CassandraConnector(sc.getConf).withClusterDo(_.getMetadata)
      val keyspace = Option(metadata.getKeyspace(keyspaceName))

      keyspace match {
        case Some(ks) => {
          if (Option(ks.getTable(tableName)).isEmpty)
            SparkJobValid
          else
            SparkJobInvalid(s"Table $tableName already exists")
        }
        case _ => SparkJobInvalid(s"Can't create a Table in Non-Existent Keyspace $keyspaceName")
      }
    } catch {
      case e: Exception => SparkJobInvalid(e.getMessage)
    }
  }

  /**
   * Paralleizes a collection of rows based on the Job Configuration and saves it to a new table
   * using the saveAsCassandraTableEx method. This will be done in a distributed manner on the spark
   * executors.
   */
  def demoCreate(sc: SparkContext, jobConfig: Config): String = {

    val keyspaceName = jobConfig.getString("keyspace")
    val tableName = jobConfig.getString("table")

    val newTable = TableDef(
      keyspaceName,
      tableName,
      Seq(ColumnDef("customer",PartitionKeyColumn, TextType)),
      Seq(ColumnDef("order", ClusteringColumn(0), TextType)),
      Seq(ColumnDef("amount", RegularColumn, IntType))
    )

    val parts = Try(jobConfig.getInt("rows")).getOrElse(1000)

    sc.parallelize(1 to parts)
      .flatMap( x => for (y <- 1 to 100) yield (x,y))
      .map{ case (id,order_id) =>
      DemoRow(s"customer_$id",s"order_$order_id", order_id)}
      .saveAsCassandraTableEx(newTable)
    s"$keyspaceName.$tableName Created with ${parts * 100} records"
  }

  /**
   * Checks to see of there exists a Cassandra Table which exists and has
   * all the columns required to form a DemoRow object.
   */
  def demoCacheValidate(sc: SparkContext, jobConfig: Config): SparkJobValidation = {
    try {
      val requiredColumns = Seq(
        ("customer", DataType.text()),
        ("order", DataType.text()),
        ("amount", DataType.cint())
      )
      val requiredColumnNames = requiredColumns.map(_._1)

      val keyspaceName = jobConfig.getString("keyspace")
      val tableName = jobConfig.getString("table")
      val rddName = jobConfig.getString("rdd")

      val metadata = CassandraConnector(sc.getConf).withClusterDo(_.getMetadata)
      val keyspace = Option(metadata.getKeyspace(keyspaceName))

      keyspace match {
        case Some(ks) => {
          Option(ks.getTable(tableName)) match {
            case Some(table) => {
              import scala.collection.JavaConversions._
              val tableColNames = table.getColumns.map(_.getName)
              val missingColumns = requiredColumnNames.toSet -- tableColNames.toSet
              if (missingColumns.isEmpty) {
                val incorrectTypes = for (
                  (colName, colType) <- requiredColumns
                  if table.getColumn(colName).getType != colType)
                  yield (colName,colType)
                if (incorrectTypes.isEmpty)
                  SparkJobValid
                else
                  SparkJobInvalid(s"Requested table $keyspaceName.$tableName had incorrect types for $incorrectTypes")
              } else {
                SparkJobInvalid(s"Requested table $keyspaceName.$tableName had missing columns $missingColumns")
              }
            }
            case _ => SparkJobInvalid (s"Table $tableName does not exist")
          }
        }
        case _ => SparkJobInvalid(s"Can't cache a Table in a Non-Existent Keyspace $keyspaceName")
      }
    } catch {
      case e: Exception => SparkJobInvalid(e.getMessage)
    }
  }

  /**
   * Adds a Cassandra Table as a NamedRDD to be persisted between SparkJobserver Jobs.
   */
  def demoCache(sc: SparkContext, jobConfig: Config): String =
  {
    this.namedRdds.update(
      jobConfig.getString("rdd"),
      sc.cassandraTable[DemoRow](
        jobConfig.getString("keyspace"),
        jobConfig.getString("table")
      )
    )
    s"Now Cached: ${this.namedRdds.getNames().mkString(",")}"
  }

  /**
   * Checks that a NamedRDD exists so that it can be removed.
   */
  def demoUnCacheValidate(sc: SparkContext, jobConfig: Config): SparkJobValidation = {
    try {
      val rddName = jobConfig.getString("rdd")
      this.namedRdds.get(rddName) match {
        case Some(_) => SparkJobValid
        case _ => SparkJobInvalid(s"Can't uncache non-cached RDD $rddName")
      }
    } catch {
      case e: Exception => SparkJobInvalid(e.getMessage)
    }
  }

  /**
   * Removes a NamedRDD
   */
  def demoUncache(sc: SparkContext, jobConfig: Config): String = {
    this.namedRdds.destroy(jobConfig.getString("rdd"))
    s"Now Cached: ${this.namedRdds.getNames().mkString(",")}"
  }

  /**
   * Check that the NamedRDD actually exists and is of the proper type.
   */
  def demoSumValidate(sc: SparkContext, jobConfig: Config): SparkJobValidation = {
    try {
      val rddName = jobConfig.getString("rdd")
      val rdd = this.namedRdds.get[DemoRow](rddName)
      if (rdd.isDefined) SparkJobValid else SparkJobInvalid(s"Missing named RDD $rddName")
    } catch {
      case e: Exception => SparkJobInvalid(e.getMessage)
    }
  }

  /** Accesses a named RDD of type DemoRow and sums the "amount" field **/
  def demoSum(sc: SparkContext, jobConfig: Config): String = {
    val rddName = jobConfig.getString("rdd")
    val rdd = this.namedRdds.get[DemoRow](rddName).get
    s"Summed amount field of $rddName is ${rdd.map(_.amount).sum()}"
  }

  /**
   * Validate method from the Spark Job which lets us control whether or not a
   * given configuration request is valid.
   * @param sc
   * @param config
   * @return
   */
  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    Try(config.getString("action").toLowerCase).getOrElse("NotSet") match {
      case "create" => demoCreateValidate(sc, config)
      case "cache" => demoCacheValidate(sc, config)
      case "uncache" => demoUnCacheValidate(sc, config)
      case "sum" => demoSumValidate(sc, config)
      case "NotSet" => SparkJobInvalid("action was not set")
      case x => SparkJobInvalid(s"Invalid Action: $x")
    }
  }
}