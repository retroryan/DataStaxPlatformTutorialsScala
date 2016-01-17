package demo

import java.util.Date

import _root_.spark.jobserver._
import com.datastax.spark.connector._
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.sql.hive.HiveContext

case class Person(id: Int, name: String, birthDate: Date)

case class Order(id: Int, name: String, birthDate: Date)


object JobServerBasicReadWrite extends SparkJob with NamedRddSupport {

  val PERSON_RDD_CACHE = "PERSON_RDD_CACHE"
  val personKS = "test"
  val personTable = "people"

  type PERSON_PAIR = (Int, Person)

  override def runJob(sc: SparkContext, jobConfig: Config): Any = {

    jobConfig.getString("action").toLowerCase match {
      case "create" => createPeople(sc, jobConfig)
      case "load" => loadPeople(sc, jobConfig)
      case "display" => displayPerson(sc, jobConfig)
      case "test_sql" => testSQL(sc, jobConfig)
      case "test_hc" => testHiveQL(sc, jobConfig)
      case _ => throw new IllegalArgumentException("Invalid Action")
    }
  }

  def testSQL(sc: SparkContext, jobConfig: Config) = {
    val cc = new CassandraSQLContext(sc)
    val peopleDF: DataFrame = cc.sql("select * from test.people")
    println(s"PEOPLE DF: ${peopleDF.show()}")
  }

  def testHiveQL(sc: SparkContext, jobConfig: Config) = {
    val hc = new  HiveContext(sc)
    val peopleDF: DataFrame = hc.sql("select * from test.people")
    println(s"PEOPLE DF: ${peopleDF.show()}")
  }

  def createPeople(sc: SparkContext, jobConfig: Config) = {

    val people = List(Person(1, "John", new Date()),
      Person(2, "Anna", new Date()),
      Person(3, "Matilda", new Date()),
      Person(4, "Jake", new Date()))

    val peopleRDD = sc.parallelize(people).saveToCassandra(personKS, personTable, SomeColumns("id", "name", "birth_date"))

  }

  def loadPeople(sc: SparkContext, jobConfig: Config) = {

    val personPairRDD: RDD[PERSON_PAIR] = sc.cassandraTable[Person](personKS, personTable).map(p => (p.id, p))

    this.namedRdds.update(
      PERSON_RDD_CACHE,
      personPairRDD)

    println(s"OUT:: Now Cached: ${this.namedRdds.getNames().mkString(",")}")
    s"RETURN Now Cached: ${this.namedRdds.getNames().mkString(",")}"
  }

  def displayPerson(sc: SparkContext, jobConfig: Config) = {

    val peopleRDD: RDD[PERSON_PAIR] = this.namedRdds.get[PERSON_PAIR](PERSON_RDD_CACHE).get
    //scala only pattern matches on stable identifiers - i.e. upper case variable names or you have to use `` in the match
    val PERSON_ID = jobConfig.getInt("person_id")

    val peopleStrRDD = peopleRDD.collect {
      case (PERSON_ID, person) => person.toString
    }

    println(s"OUT::  ${peopleStrRDD.collect().mkString("::")}")
    s"RETURN STR: ${peopleStrRDD.collect().mkString("::")}"
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    SparkJobValid
  }
}
