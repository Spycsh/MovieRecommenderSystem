package com.Spycsh.streaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Analyzer {
  val MAX_USER_RATING_NUM = 20
  val MAX_SIM_MOVIES_NUM = 20
  val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_MOVIE_RECS_COLLECTION = "MovieRecs"
  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender2021",
      "mongo.db" -> "recommender2021",
      "kafka.topic" -> "recommender2021"
    )

    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StreamingRecommender")

    // create a SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    // get streaming context
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(2)) // batch duration

    import spark.implicits._
    println("fuck55")
    println(config("mongo.uri"))
    println(config("mongo.db"))
    println(MongoConfig(config("mongo.uri"), config("mongo.db")))
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
    println("66")
    // broadcast the movie similarity matrix
    // it is better to handle larger data with broadcasting
    // because instead of giving every task on a worker a copy of the big data
    // only one copy is received by the executor on a worker
    // 1. collect
    val simMovieMatrix = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_RECS_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRecs]
      .rdd
      .map { movieRecs =>
        (movieRecs.mid, movieRecs.recs.map(x => (x.mid, x.score)).toMap)
      }.collectAsMap()
    println("77")
  }
}