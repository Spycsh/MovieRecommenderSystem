package com.Spycsh.offline

import breeze.numerics.sqrt
import com.Spycsh.offline.OfflineRecommender.MONGODB_RATING_COLLECTION
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

// get best (rank, iterations, lambda), in order to get min RMSE(root-mean-square error)
object ALSTrainer {
  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )

    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OfflineRecommender")

    // create a SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    // No implicits found... solution
    import spark.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // load rating table from database
    val ratingRDD = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map( rating => Rating( rating.uid, rating.mid, rating.score ) )    // translate to rdd, and remove timestamps
      .cache()

    //randomly generate datasets for training and testing
    val splits = ratingRDD.randomSplit(Array(0.8, 0.2))
    val trainingRDD = splits(0)
    val testRDD = splits(1)

    //get parameters, using RMSE (the third element) to choose the best params
    adjustALSParam(trainingRDD, testRDD)

    spark.close()
  }

  def adjustALSParam(trainData: RDD[Rating], testData: RDD[Rating]): Unit ={
    val result = for( rank <- Array(50, 100, 200, 300); lambda <- Array( 0.01, 0.1, 1 ))
      yield {
        val model = ALS.train(trainData, rank, 5, lambda)

        //calculate current RMSE, return double
        val rmse = getRMSE( model, testData )
        ( rank, lambda, rmse )
      }
    // console output
    // (200,0.1,0.9171834194767383)
    println(result.minBy(_._3))
  }

  def getRMSE(model: MatrixFactorizationModel, data: RDD[Rating]): Double = {
    // calculate predict score
    val userProducts = data.map(item => (item.user, item.product))
    val predictRating = model.predict(userProducts)

    //use uid and mid as foreign keys, inner join the real scores and predict scores
    //use tuple (user, product) for a predict value and a real value
    val observed = data.map( item => ( (item.user, item.product), item.rating ) )
    val predict = predictRating.map( item => ( (item.user, item.product), item.rating ) )
    // inner join, get(uid, mid),(actual, predict)
    // calculate RMSE
    sqrt(
      observed.join(predict).map{
        case ( (uid, mid), (actual, pre) ) =>
          val err = actual - pre
          err * err
      }.mean()
    )
  }
}
