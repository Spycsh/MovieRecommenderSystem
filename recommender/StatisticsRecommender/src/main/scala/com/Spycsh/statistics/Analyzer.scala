package com.Spycsh.statistics

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions.asScalaBuffer
import scala.util.matching.Regex

// Here is just an draft analyzer to analyze the basic information of the data set
// not used for production
object Analyzer {
  def main(args: Array[String]): Unit = {
//    val pattern:Regex = "(\\d+)\\D+(\\d+)".r
//    val iterator = pattern.findAllMatchIn("11giugi33")
//    val res = iterator.map(x => (x.group(1), x.group(2))).toList
//    val pattern:Regex = "(\\d+),(.+\\d+\\)\"?),(.+)".r
//val pattern:Regex = "(\\d+),(.+),(.+)".r
//    val str = "92535,Louis C.K.: Live at the Beacon Theater (2011),Comedy"
//    val iterator = pattern.findAllMatchIn("11,\"American President, The (1995)\",Comedy|Drama|Romance")
//    val iterator = pattern.findAllMatchIn(str)
//    val res = iterator.map(x => (x.group(1), x.group(2), x.group(3))).toList
//    println(res(0)._2)
        val MONGODB_MOVIE_COLLECTION = "Movie"
//    val MONGODB_RATING_COLLECTION = "Rating"
//
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender2021",
      "mongo.db" -> "recommender2021"
    )
//
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("analyzer")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
//
    import spark.implicits._
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
//
    val movieDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .toDF()
      .as[Movie]

    //
    val dup = Array("genres")

    import spark.implicits._
    println(movieDF.flatMap(x => x.genres.split("\\|")).dropDuplicates().collectAsList())
//    movieDF.flatmap(x => x.genres)
  }
}
