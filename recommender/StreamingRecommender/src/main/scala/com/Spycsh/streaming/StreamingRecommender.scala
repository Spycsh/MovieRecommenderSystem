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
  val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"
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

            // 3. for every candidate movie, calculate the score and sort as current user's recommendation list, Array[(mid, score)]
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

  import scala.collection.JavaConversions._

  def getUserRecentlyRating(num: Int, uid: Int, jedis: Jedis): Array[(Int, Double)] = {
    // read data from redis, save the user rating data into a queue of uid:UID,
    jedis.lrange("uid:" + uid, 0, num-1)
      .map{
        item =>
          val attr = item.split("\\:")
          ( attr(0).trim.toInt, attr(1).trim.toDouble )

      }
      .toArray
  }

  def getTopSimMovies(num: Int, mid: Int, uid: Int, simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
                     (implicit mongoConfig: MongoConfig): Array[Int] ={
    //  1. get all the similar movies from the similarity matrix
    val allSimMovies = simMovies(mid).toArray

    //  2. get the movies that the user has already seen
    val ratingExist = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)
      .find( MongoDBObject("uid" -> uid))
      .toArray
      .map{
        item => item.get("mid").toString.toInt
      }

    // 3. filter out the movies that have been seen, and get the output list
    allSimMovies.filter(x=> ! ratingExist.contains(x._1))
      .sortWith(_._2>_._2)
      .take(num)
      .map(x=>x._1)
  }

  def computeMovieScores(candidateMovies: Array[Int],
                         userRecentlyRatings: Array[(Int, Double)],
                         simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]]): Array[(Int, Double)] = {
    // define an ArrayBuffer, which is used to save the basic score of every candidate movie
    val scores = scala.collection.mutable.ArrayBuffer[(Int, Double)]()
    // define a HashMap and save the enhancing and attenuation factor
    val increMap = scala.collection.mutable.HashMap[Int, Int]()
    val decreMap = scala.collection.mutable.HashMap[Int, Int]()

    for( candidateMovie <- candidateMovies; userRecentlyRating <- userRecentlyRatings){
      // get the similarity of the candidate movies and the recently rating movie
      val simScore = getMoviesSimScore(candidateMovie, userRecentlyRating._1, simMovies)

      if(simScore > 0.7) {
        // calculate the basic score of the candidate movie
        scores += ((candidateMovie, simScore * userRecentlyRating._2))
        if (userRecentlyRating._2 > 3) {
          increMap(candidateMovie) = increMap.getOrDefault(candidateMovie, 0) + 1
        } else {
          decreMap(candidateMovie) = decreMap.getOrDefault(candidateMovie, 0) + 1
        }
      }
    }

    // based on the formula, groupby mid of the candidate movie, compute the recommendation rating
    scores.groupBy(_._1).map {
      // data structure after groupBy: Map( mid -> ArrayBuffer[(mid, score)])
      // !!! CORE FORMULA HERE !!! //
      case (mid, scoreList) =>
        (mid, scoreList.map(_._2).sum / scoreList.length + log(increMap.getOrDefault(mid, 1)) - log(decreMap.getOrDefault(mid, 1)))
    }.toArray.sortWith(_._2>_._2)

  }

  // get the similarity of the two movies
  def getMoviesSimScore(mid1: Int, mid2: Int, simMovies: scala.collection.Map[Int,
    scala.collection.immutable.Map[Int, Double]]): Double ={

    simMovies.get(mid1) match {
      case Some(sims) => sims.get(mid2) match {
        case Some(score) => score
        case None => 0.0  // if no rating of movie id, return 0.0
      }
      case None => 0.0  // if no record of movie id, return 0.0
    }
  }

  // get the log of one number, base 10
  def log(m:Int):Double = {
    val N = 10
    math.log(m) / math.log(N)
  }

  def saveDataToMongoDB(uid: Int, streamRecs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig): Unit ={
    // define the connection to StreamRecs table
    val streamRecsCollection = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_STREAM_RECS_COLLECTION)

    // if there is already data that corresponding to uid, delete
    streamRecsCollection.findAndRemove(MongoDBObject("uid" -> uid))
    // save the data in streamRecs into the table
    streamRecsCollection.insert(MongoDBObject( "uid"->uid,
    "recs"->streamRecs.map(x=>MongoDBObject("mid"->x._1, "score"->x._2))))
  }

}
