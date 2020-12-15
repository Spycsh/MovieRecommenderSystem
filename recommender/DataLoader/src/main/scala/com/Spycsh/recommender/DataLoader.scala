package com.Spycsh.recommender

import java.net.InetAddress

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

// data structure

// define case class
/**
 * let us see movies.csv
 * mid: 1
 * name: Toy Story (1995)
 * descri:
 * timelong: 81 minutes
 * issue: March 20, 2001
 * shoot: 1995
 * language: English
 * genres: Adventure|Animation|Children|Comedy|Fantasy
 * actors: Tom Hanks|Tim Allen|Don Rickles|Jim Varney|Wallace Shawn|John Ratzenberger|Annie Potts|John Morris|Erik von Detten|Laurie Metcalf|R. Lee Ermey|Sarah Freeman|Penn Jillette|Tom Hanks|Tim Allen|Don Rickles|Jim Varney|Wallace Shawn
 * directors: John Lasseter
 */
case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                  shoot: String, language: String, genres: String, actors: String,
                  directors: String)

/**
 * see ratings.csv
 * uid: 1  (user id)
 * mid: 31
 * score: 2.5
 * timestamp: 1260759144 10-bit
 *
 * Notice that signed Integer is +-31bit  1024=2**10
 * 2**31 > 1250759200
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

// mongo and es to case class
/**
 *
 * @param uri mongodb connection
 * @param db database
 */
case class MongoConfig(uri: String, db:String)

/**
 *
 * @param httpHosts       http hosts list, split by comma port: 9200 for users' HTTP request
 * @param transportHosts  hosts transport list, port: 9300 for cluster access each other
 * @param index           index to operate on
 * @param clustername     name of cluster, default elasticsearch use ./elasticsearch in command and enter localhost:9200 on web
 */
case class ESConfig(httpHosts: String, transportHosts:String, index:String, clustername:String)

object DataLoader {
  // define the invariants
  val MOVIE_DATA_PATH = "C:\\Users\\Spycsh\\Desktop\\MovieRecommenderSystem\\recommender\\DataLoader\\src\\main\\resources\\movies.csv"
  val RATING_DATA_PATH = "C:\\Users\\Spycsh\\Desktop\\MovieRecommenderSystem\\recommender\\DataLoader\\src\\main\\resources\\ratings.csv"
  val TAG_DATA_PATH = "C:\\Users\\Spycsh\\Desktop\\MovieRecommenderSystem\\recommender\\DataLoader\\src\\main\\resources\\tags.csv"

  // table names
  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_TAG_COLLECTION = "Tag"
  val ES_MOVIE_INDEX = "Movie"

  def main(args:Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender",
      "es.httpHosts" -> "localhost:9200",
      "es.transportHosts" -> "localhost:9300",
      "es.index" -> "recommender",
      "es.cluster.name" -> "elasticsearch"
    )

    // establish a sparkConf
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")

    // establish a spark session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    // load data
    val movieRDD = spark.sparkContext.textFile(MOVIE_DATA_PATH)
    // convert to dataframe
    val movieDF = movieRDD.map(item=>{
      val attr = item.split("\\^")
      Movie(attr(0).toInt,attr(1).trim,attr(2).trim,attr(3).trim,attr(4).trim,
        attr(5).trim,attr(6).trim,attr(7).trim,attr(8).trim,attr(9).trim)
    }).toDF()


    val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)
    //  convert to dataframe
    val ratingDF = ratingRDD.map(item=>{
      val attr = item.split(",")
      Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }).toDF()


    val tagRDD = spark.sparkContext.textFile(TAG_DATA_PATH)
    // convert to dataframe
    val tagDF = tagRDD.map(item=>{
      val attr = item.split(",")
      Tag(attr(0).toInt, attr(1).toInt, attr(2).trim, attr(3).toInt)
    }).toDF()

    // declare an implicit config
    implicit val mongoConfig =
      MongoConfig(config("mongo.uri"), config("mongo.db"))

    // store data into mongoDB
    storeDataInMongoDB(movieDF, ratingDF, tagDF)


    import org.apache.spark.sql.functions._
    // groupBy=>聚合agg
    // one mid map to one list of tags where each tag is split by |
    val newTag = tagDF.groupBy($"mid")
      .agg( concat_ws( "|", collect_set($"tag") ).as("tags") )
      .select("mid", "tags")

    // left join the newTag and the movie
    val movieWithTagsDF = movieDF.join(newTag, Seq("mid"), "left")

    implicit val esConfig = ESConfig(config("es.httpHosts"), config("es.transportHosts"), config("es.index"), config("es.cluster.name"))

    // store data in es
    storeDataInES(movieWithTagsDF)

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

  def storeDataInES(movieDF:DataFrame)(implicit eSConfig: ESConfig):Unit={
    // create a config
    val settings:Settings = Settings.builder().put("cluster.name", eSConfig.clustername).build()

    // create a ES client
    val esClient = new PreBuiltTransportClient(settings)

    // add the transport hosts to esClient
    // multiple arbitrary character host: multiple numbers port
    // split by ,
    val REGEX_HOST_PORT = "(.+):(\\d+)".r
    eSConfig.transportHosts.split(",").foreach{
      case REGEX_HOST_PORT(host: String, port: String) => {
        esClient.addTransportAddress(new InetSocketTransportAddress( InetAddress.getByName(host), port.toInt ))
      }
    }
    //    esClient.addTransportAddress(new InetSocketTransportAddress( InetAddress.getByName("localhost"), 9300 ))

    // clear existing data
    if( esClient.admin().indices().exists( new IndicesExistsRequest(eSConfig.index) )
      .actionGet()
      .isExists
    ){
      esClient.admin().indices().delete( new DeleteIndexRequest(eSConfig.index) )
    }

    // create es index recommender
    esClient.admin().indices().create( new CreateIndexRequest(eSConfig.index) )

    movieDF.write
      .option("es.nodes", eSConfig.httpHosts)
      .option("es.http.timeout", "100m")  // 100 minutes
      .option("es.mapping.id", "mid") // primary key
      .mode("overwrite")
      .format("org.elasticsearch.spark.sql")
      .save(eSConfig.index + "/" + ES_MOVIE_INDEX)
  }



}
