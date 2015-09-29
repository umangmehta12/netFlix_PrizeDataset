
create external table movie_reviews(movie_id INT,customer_id double,rating float, date TIMESTAMP) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '${hiveconf:reviewfile}';


create external table movie_details(movie_id INT,year_of_release String,movie_name String) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '${hiveconf:moviefile}';

select * from (select  m.year_of_release, m.movie_id,avg.avg_rating,m.movie_name,RANK() OVER (PARTITION BY m.year_of_release ORDER BY avg.avg_rating desc) ranking from (select movie_id,sum(rating)/count(rating) avg_rating from movie_reviews group by movie_id order by movie_id,avg_rating) avg,movie_details m where m.movie_id=avg.movie_id) ratings where ranking <=5;