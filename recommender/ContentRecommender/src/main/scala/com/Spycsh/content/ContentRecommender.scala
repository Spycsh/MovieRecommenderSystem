package com.Spycsh.content

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix

case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String, directors: String)

case class MongoConfig(uri:String, db:String)

case class Recommendation(mid:Int, score:Double)

case class MovieRecs(mid:Int, recs:Seq[Recommendation])

object ContentRecommender {
  val MONGODB_MOVIE_COLLECTION = "Movie"

  val CONTENT_MOVIE_RECS = "ContentMovieRecs"

  def main(args: Array[String]):Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OfflineRecommender")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // load data and preprocess
    val movieTagsDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .map(
        // extract mid, name, genres as the content features, space as the deliminator
        x => (x.mid, x.name, x.genres.map(c=> if(c=='|') ' ' else c))

      )
      .toDF("mid", "name", "genres")
      .cache() // load to memory

    // ***** use TF-IDF to extract the feature vector ****

    // create a deliminator, default space to deliminate
    val tokenizer = new Tokenizer().setInputCol("genres").setOutputCol("words")
    // transform the features into new row of words
    val wordsData = tokenizer.transform(movieTagsDF)

    // use HashingTF to convert a word sequence into the corresponding frequency
    // set number features <=> set hash buckets
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(50)
    val featurizedData = hashingTF.transform(wordsData)

    // [horror]|     (50,[26],[1.0])
    // the 26th feature of the 50 features
    featurizedData.show(truncate = false)

    // use IDF model get the word in all documents (rows)
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    // train the idf model to get the IDF(inverse document frequency )
    val idfModel = idf.fit(featurizedData)
    // process the original data with the model to obtain tf-idf of every word as a feature vector
    val rescaledData = idfModel.transform(featurizedData)

    rescaledData.show(truncate = false)


    // **** Same as StatisticsRecommender.scala **** //
    val movieFeatures = rescaledData.map(
      row => ( row.getAs[Int]("mid"), row.getAs[SparseVector]("features").toArray )
    )
      .rdd
      .map(
        x => ( x._1, new DoubleMatrix(x._2) )
      )
    movieFeatures.collect().foreach(println)

    // cartesian product of every two of the movies
    val movieRecs = movieFeatures.cartesian(movieFeatures)
      .filter{
        // filter out the record that match itself
        case (a, b) => a._1 != b._1
      }
      .map{
        case (a, b) => {
          val simScore = this.consinSim(a._2, b._2)
          ( a._1, ( b._1, simScore ) )
        }
      }
      .filter(_._2._2 > 0.6)    // filter out the records whose similarity > 0.6
      .groupByKey()
      .map{
        case (mid, items) => MovieRecs( mid, items.toList.sortWith(_._2 > _._2).map(x => Recommendation(x._1, x._2)) )
      }
      .toDF()
    movieRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", CONTENT_MOVIE_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    spark.stop()
  }

  def consinSim(movie1: DoubleMatrix, movie2: DoubleMatrix):Double ={
    movie1.dot(movie2) / ( movie1.norm2() * movie2.norm2() )
  }
}
