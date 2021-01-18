package com.Spycsh.streaming

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import kafka.Kafka
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis


object ConnHelper extends Serializable{
  lazy val jedis = new Jedis("localhost")
  lazy val mongoClient = MongoClient( MongoClientURI("mongodb://localhost:27017/recommender") )
}

case class MongoConfig(uri:String, db:String)

// define a basic recommendation object
case class Recommendation( mid: Int, score: Double )

// define the recommendation list for users
case class UserRecs( uid: Int, recs: Seq[Recommendation] )

// define the similar movie list based on LFM movie feature vectors
case class MovieRecs( mid: Int, recs: Seq[Recommendation] )

object StreamingRecommender {
  val MAX_USER_RATING_NUM = 20
  val MAX_SIM_MOVIES_NUM = 20
  val MONGODB_STREAM_RECS_COLLECTION = "SteamRecs"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_MOVIE_RECS_COLLECTION = "MovieRecs"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender",
      "kafka.topic" -> "recommender"
    )

    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StreamingRecommender")

    // create a SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    // get streaming context
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(2)) // batch duration

    import spark.implicits._
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

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
      .map{ movieRecs =>
        (movieRecs.mid, movieRecs.recs.map(x=>(x.mid,x.score)).toMap )
      }.collectAsMap()
    // 2. broadcast
    val simMovieMatrixBroadCast = sc.broadcast(simMovieMatrix)

    // kafka parameters for connection
    val kafkaParam = Map(
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommender",
      "auto.offset.reset" -> "latest"
    )
    // DStream
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String]( Array(config("kafka.topic")), kafkaParam )
    )

    // transfer UID|MID|SCORE|TIMESTAMP into rating stream
    val ratingStream = kafkaStream.map{
      msg =>
        val attr = msg.value().split("\\|")
        ( attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt )
    }

    // continue to do streaming
    // core algorithms
    ratingStream.foreachRDD {
      rdds =>
        rdds.foreach {
          case (uid, mid, score, timestamp) => {
            println("rating data coming! >>>>>>>>>>>>>>")

            // 1. get the latest K times of rating from redis, and save as Array[(mid, score)]
            val userRecentlyRatings = getUserRecentlyRating(MAX_USER_RATING_NUM, uid, ConnHelper.jedis)

            // 2. from similarity matrix, extract N most similar movies as the candidate list, Array[mid]
            val candidateMovies = getTopSimMovies(MAX_SIM_MOVIES_NUM, mid, uid, simMovieMatrixBroadCast.value)

            // 3. for every candidate movie, calculate the current user's recommendation list, Array[(mid, score)]
            val streamRecs = computeMovieScores(candidateMovies, userRecentlyRatings, simMovieMatrixBroadCast.value)

            // 4. save the recommendation data into mongodb
            saveDataToMongoDB(uid, streamRecs)
          }
        }
    }

    // start to receive and handle data
    ssc.start()

    println(">>>>>>>>>>>>> streaming started!")

    ssc.awaitTermination()
  }







}
