package com.Spycsh.recommender

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.matching.Regex

// define case class
/**
 *
 * mid: 1
 * title: Toy Story (1995)
 * genres: Adventure|Animation|Children|Comedy|Fantasy
 */
case class Movie(mid: Int, title: String, genres: String)

/**
 * see ratings.csv
 * uid: 1  (user id)
 * mid: 31
 * score: 2.5
 * timestamp: 1260759144 10-bit
 *
 * Notice that signed Integer is +-31bit  1024=2**10
 * 2**31=2147483648 > max of timestamp in the data set
 * so timestamp can be treated as Int type
 */
case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)

/**
 * uid: 15
 * mid: 1955
 * tag: dentist
 * timestamp: int
 */
case class Tag(uid: Int, mid: Int, tag: String, timestamp: Int)

// mongo case class
/**
 *
 * @param uri mongodb connection
 * @param db database
 */
case class MongoConfig(uri: String, db:String)

object DataLoader {
  // define the invariants
  val MOVIE_DATA_PATH = ".\\recommender\\DataLoader\\src\\main\\resources\\ml-latest-small\\movies.csv"
  val RATING_DATA_PATH = ".\\recommender\\DataLoader\\src\\main\\resources\\ml-latest-small\\ratings.csv"
  val TAG_DATA_PATH = ".\\recommender\\DataLoader\\src\\main\\resources\\ml-latest-small\\tags.csv"

  // table names
  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_TAG_COLLECTION = "Tag"

  def main(args:Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender2021",
      "mongo.db" -> "recommender2021"
    )

    // establish a sparkConf
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")

    // establish a spark session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    // load data
    val movieRDD = spark.sparkContext.textFile(MOVIE_DATA_PATH)
    // convert to dataframe
    // get first line
    val firstLineOfMovieRDD = movieRDD.first()
    // there are some titles that has ',' inside, so regex is needed
    // to extract the mid before the first ',' , title
    // and genres after the last ','
    val pattern:Regex = "(\\d+),(.+),(.+)".r
    val movieDF = movieRDD.filter(item => !item.equals(firstLineOfMovieRDD)).map(item=>{
      val findIterator = pattern.findAllMatchIn(item)
      val res = findIterator.map(x => (x.group(1), x.group(2), x.group(3))).toList
      Movie(res(0)._1.toInt,res(0)._2.trim,res(0)._3.trim)
    }).toDF()


    val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)
    //  convert to dataframe
    val firstLineOfRatingRDD = ratingRDD.first()
    val ratingDF = ratingRDD.filter(item => !item.equals(firstLineOfRatingRDD)).map(item=>{
      val attr = item.split(",")
      Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }).toDF()


    val tagRDD = spark.sparkContext.textFile(TAG_DATA_PATH)
    // convert to dataframe
    val firstLineOfTagRDD = tagRDD.first()
    val tagDF = tagRDD.filter(item => !item.equals(firstLineOfTagRDD)).map(item=>{
      val attr = item.split(",")
      Tag(attr(0).toInt, attr(1).toInt, attr(2).trim, attr(3).toInt)
    }).toDF()

    // declare an implicit config
    implicit val mongoConfig =
      MongoConfig(config("mongo.uri"), config("mongo.db"))

    // store data into mongoDB
    storeDataInMongoDB(movieDF, ratingDF, tagDF)

    spark.stop()

  }

  def storeDataInMongoDB(movieDF:DataFrame, ratingDF:DataFrame, tagDF:DataFrame)
                        (implicit  mongoConfig: MongoConfig): Unit = {
    // create a connection to MongoDB
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    // if there is a corresponding database in MongoDB, delete it
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).dropCollection()

    // write to MongoDB
    movieDF
      .write
      .option("uri",mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // write to MongoDB
    ratingDF
      .write
      .option("uri",mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // write to MongoDB
    tagDF
      .write
      .option("uri",mongoConfig.uri)
      .option("collection", MONGODB_TAG_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // create index for tables 1 ascending -1 descending
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    //close MongoDB connection
    mongoClient.close()
  }



}
