package com.Spycsh.statistics

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Movie(mid: Int, title: String, genres: String)

case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int )

case class MongoConfig(uri:String, db:String)

case class Recommendation( mid: Int, score: Double )

case class GenresRecommendation( genres: String, recs: Seq[Recommendation] )

object StatisticsRecommender {
  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"

  // after finding out hot movies, the data should be write to the database
  // with following table names
  // 1. history hottest table
  val RATE_MORE_MOVIES = "RateMoreMovies"
  // 2. recently hot movies
  val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies"
  // 3. average rate of movies
  val AVERAGE_MOVIES = "AverageMovies"
  // 4. top movies for different genres
  val GENRES_TOP_MOVIES = "GenresTopMovies"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender2021",
      "mongo.db" -> "recommender2021"
    )

    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StatisticsRecommeder")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    val ratingDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    val movieDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .toDF()

    // create the temp view with name "rating"
    ratingDF.createOrReplaceTempView("ratings")

    // 1. hottest movie statistics, select the mid with most ratings (count)
    val rateMoreMoviesDF = spark.sql("select mid, count(mid) as count from ratings group by mid order by count desc")
    // write to the mongodb table
    storeDFInMongoDB(rateMoreMoviesDF, RATE_MORE_MOVIES)

    // 2. RECENTLY hottest movies statistics, following "yyyyMM" to select the recently rating data and count
    val simpleDateFormat = new SimpleDateFormat("yyyyMM");
    // register udf(user defined function) and turn the timestamp into year month pattern
    // ORIGINAL TIMESTAMP IS based on second, so we need to multiply 1000 and turn it to long
    // 1260759144000 => 201605
    // to get the millisecond as input
    // and finally turn it to year month format and then turn to int
    spark.udf.register("changeDate", (x:Int)=>simpleDateFormat.format(new Date(x*1000L)).toInt)
    // use the changeDate
    val ratingOfYearMonth = spark.sql("select mid, score, changeDate(timestamp) as yearmonth from ratings")
    ratingOfYearMonth.createOrReplaceTempView("ratingOfMonth")
    // search the mid, count, yearmonth for every movie in each month from ratingOFMonth
    val rateMoreRecentlyMoviesDF = spark.sql("select mid, count(mid) as count, yearmonth from ratingOfMonth group by yearmonth, mid order by yearmonth desc, count desc")
    // store into mongodb
    storeDFInMongoDB(rateMoreRecentlyMoviesDF, RATE_MORE_RECENTLY_MOVIES)

    // 3. Top Movies, The movies with highest average rate
    val averageMoviesDF = spark.sql("select mid, avg(score) as avg from ratings group by mid order by avg desc")
    storeDFInMongoDB(averageMoviesDF, AVERAGE_MOVIES)

    // 4. Each genre Top movie
    // all genre types

//    val genres = List("Action","Adventure","Animation","Comedy","Crime","Documentary","Drama","Family","Fantasy","Foreign","History","Horror","Music","Mystery"
//      ,"Romance","Science","Tv","Thriller","War","Western")
    val genres:List[String] = movieDF.as[Movie].flatMap(x => x.genres.split("\\|")).dropDuplicates().collect().toList
    // put average rating into movie table, add one column, inner join
    val movieWithScore = movieDF.join(averageMoviesDF, "mid")
    // firstly we do Cartesian product to genres and movieWithScore
    // we surely need to convert genres to rdd
    val genresRDD = spark.sparkContext.makeRDD(genres)
    val genresTopMoviesDF = genresRDD.cartesian(movieWithScore.rdd)
      .filter{
        // filter out the records movie genre do not match
        case (genre, movieRow) => movieRow.getAs[String]("genres").toLowerCase.contains( genre.toLowerCase )
      }
      .map{
            // genre, Recommendation( mid: Int, score: Double )
        case (genre, movieRow) => (genre, (movieRow.getAs[Int]("mid"), movieRow.getAs[Double]("avg")))
      }
      .groupByKey()
      .map{
//        GenresRecommendation(genres: String, recs: Seq[Recommendation])
            // item RDD, must sort by the second element
        case (genre, items) => GenresRecommendation( genre, items.toList.sortWith(_._2>_._2).take(10).map( item=> Recommendation(item._1, item._2)) )
      }
      .toDF()

    storeDFInMongoDB(genresTopMoviesDF, GENRES_TOP_MOVIES)

    spark.stop()

  }

  def storeDFInMongoDB(df: DataFrame, collection_name: String)(implicit mongoConfig: MongoConfig): Unit ={
    df.write
      .option("uri", mongoConfig.uri)
      .option("collection", collection_name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }

}
