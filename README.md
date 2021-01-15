# RecommenderSystem

- [x] create Maven project, specify parent-children dependencies

- [x] establish the connection to Mongo database

- [x] recommender based on statistics

- [x] offline recommender

- [ ] streaming recommender

- [ ] content recommender

## Modules introduction

Here gives brief introduction of different modules

### DataLoader

Tables:
1. Movie: (mid, name, decri, timelong, shoot, issue, language, genres, director, actors)
2. Rating: (uid, mid, score, timestamp)
3. User: (uid, username, password, first, genres, timestamp) 

preprocess from `movies.csv`, `ratings.csv`, `tags.csv` and store in MongoDB

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
Recommend in real-time, and
use Flume-ng to collect one single rating behavior of user and in real-time send to Kafka
use Kafka as the cache component and receive the data collection requests from Flume to push the data 
to the spark streaming recommender, and process to update the MongoDB




