# RecommenderSystem

- [x] create Maven project, specify parent-children dependencies

- [x] establish the connection to Mongo database

- [x] recommender based on statistics

- [x] offline recommender

- [x] streaming recommender

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
Recommend in real-time, by collecting one single rating behavior of user in real-time send to Kafka, 
and process, compute the real-time recommendation list to update the MongoDB

* get the latest K times of rating from redis
* from similarity matrix, extract N most similar movies as the candidate list
* for every candidate movie, calculate the score and sort as current user's recommendation list

$Score_q = \frac{\sigma_r(sim(q,r) * R_r)}{sim_sum} + log(max(incount, 1)) - log(max(recount, 1))$

q: the candidate movie
r: the movie the user has rated (data from Kafka stream)
sim(q, r): similarity of the rated movie and candidate movie
log(max(incount, 1)): log of the max of the positive rate score(from 3 to 5) and 1, which means if no rating then this term is log(1) = 0
log(max(recount, 1)): log of the max of the negative rate score(from 1 to 3) and 1, which means if no rating then this term is log(1) = 0

The two log terms is to indicate that although there is a high basic score of the movie q based on similarity, if the user has a low score, 
it should be prevented from being recommended.




