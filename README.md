# RecommenderSystem

## Modules introduction

Here gives brief introduction of different modules

### DataLoader

Dataset source: [MovieLens](https://grouplens.org/datasets/movielens/)

preprocess the `movies.csv` and `ratings.csv` and store in MongoDB

### StatisticsRecommender
Recommend movies based directly on statistics, and
use Spark Core + Spark SQL to implement the statistics recommender to find:

- hottest movie (with most ratings)
- Recently hottest movies (group by month, then by ratings, DESC)
- Top Movies (with highest average rating)
- Each genre top movie (cross table)

### OfflineRecommender
Recommend based on Collaborative filtering, and
use Spark Core + Spark MLlib and ALS to implement offline recommender

- from latent features of users, recommend a list of movies for a user (use ALS algorithm)
- from the similarity of movies, recommend a list of similar movies for a movie (use cosine similarity)

### StreamingRecommender
Recommend in real-time, by collecting one single rating behavior of user in real-time send to Kafka, 
and process, compute the real-time recommendation list to update the MongoDB

* get the latest K times of rating from redis
* from similarity matrix, extract N most similar movies as the candidate list
* for every candidate movie, calculate the score and sort as current user's recommendation list


Notes:

```
jdk 1.8

run dataloader

###########To see mongo#################
=> mongo
> show dbs
> use recommender
> show tables
> db.Movie.find().count()   

###################
// test statisticRecommender
// execute scala file
// in mongo
> show tables          // exist 4 more tables
> db.AverageMovies.findOne()
> db.GenresTopMovies.find().pretty()

############To access redis##############
https://www.runoob.com/redis/redis-install.html

In one cmd
=> redis-server.exe redis.windows.conf

In another cmd
=> redis-cli.exe -h 127.0.0.1 -p 6379
127.0.0.1:6379> set myKey abc
127.0.0.1:6379> get myKey
127.0.0.1:6379> RPUSH mylist "one"
127.0.0.1:6379> lrange mylist 0 1

#############
1) For streaming recommender, you should firstly open mongo, redis server, and redis-cli as above

In redis-cli, to simulate a user's rate of an item (stand for he has watched it), enter
127.0.0.1:6379> lpush uid:666 1079:4.0 9642:5.0 1213:4.5 2009:2.5 555:4.0 373:1.5 8628:5.0
127.0.0.1:6379> keys *
there should be a key-value pair about the uid:2 user, you can see the records with
127.0.0.1:6379> lrange uid:666 0 -1

2) Then you should open the zookeeper, Kafka server and the Kafka producer
In one cmd, open the zookeeper
=> C:\kafka\bin\windows>zookeeper-server-start.bat ..\..\config\zookeeper.properties
then open the Kafka
=> C:\kafka\bin\windows>kafka-server-start.bat ..\..\config\server.properties
you can use jps to see the on-going java processes
=> C:\Users\Spycsh>jps
  11808 StreamingRecommender
  14100
  21844 NailgunRunner
  11928 Jps
  15560 QuorumPeerMain
  20860 Kafka

then open the Kafka producer, you should assign the topic "recommender" as in the code
=> C:\kafka\bin\windows>kafka-console-producer.bat --broker-list localhost:9092 --topic recommender

3) Then you can run the StreamingRecommender.scala
In the Kafka producer cmd you can:
transfer one record to test in the format "UID|MID|SCORE|TIMESTAMP" into the rating stream
Notice that if you want to see results in 4) this record should have the uid that already map to a record in redis

> 666|4709|4.5|1564476435

Then you will find one line rating data coming! >>>>>>>>>>>>>> in the console of the program

4) See the results
Then in the cmd you start your mongo service
> use recommender
you will find a new database called StreamRecs
and you can show the final recommended movies with
> db.StreamRecs.find().pretty()
Such output will come out
> db.StreamRecs.find().pretty()
{
        "_id" : ObjectId("61740f6040f62e2e38e7f51b"),
        "uid" : 666,
        "recs" : [
                {
                        "mid" : 150401,
                        "score" : 4.101390021148716
                },
                {
                        "mid" : 4492,
                        "score" : 3.640862619666599
                },
                {
                        "mid" : 47950,
                        "score" : 3.6367343763541253
                },
                {
                        "mid" : 2099,
                        "score" : 3.6132755054875774
                },
                {
                        "mid" : 7132,
                        "score" : 3.262721880408015
                },
                {
                        "mid" : 69604,
                        "score" : 3.182491800795923
                },
                {
                        "mid" : 6314,
                        "score" : 3.1748072413685127
                },
                {
                        "mid" : 5915,
                        "score" : 2.2652324373263535
                }
        ]
}

Then you realize that you save the recommended movies list for a user related to his real-time rating.
```


