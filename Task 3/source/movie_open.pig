REGISTER file:/home/hadoop/lib/pig/piggybank.jar
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();
------------------------------------------------------------------------------------------------------------

-- Load required data from inputs
review_records = LOAD '$INPUT1' USING CSVLoader AS (MovieID:int, CustomerID:int, Rating:int, ReviewDate:chararray);
movie_title_records = LOAD '$INPUT2' USING CSVLoader AS (MovieID:int, ReleaseYear:int, MovieTitle:chararray);

-- Generate movie records
movie_records = FOREACH review_records GENERATE (int)$0 AS MovieID, ToDate((chararray)$3, 'yyyy-MM-dd') AS ReviewDate;

-- Sort by 'MovieID' and then 'ReviewDate' in ascending order
sorted_movie_records = ORDER movie_records BY MovieID, ReviewDate ASC;

-- Grouping by 'MovieID'
grouped_movie_records = GROUP sorted_movie_records BY MovieID;

-- Obtain first review date for each movie
movie_launch_record = FOREACH grouped_movie_records {
 opening_date_record = limit sorted_movie_records 1;
 GENERATE FLATTEN(opening_date_record);
};

-- Join column having launch date with sorted movie records to filter by date
join_results = JOIN sorted_movie_records BY MovieID, movie_launch_record BY MovieID;

-- Filter records which are only 30 days after launch
filter_results = FILTER join_results BY DaysBetween($1, $3) < 30;

-- Parse and group filtered records by 'MovieID'
opening_strength_records = FOREACH filter_results GENERATE $0 AS MovieID, $1 AS ReviewDate;
grouped_opening_strength = GROUP opening_strength_records BY MovieID;

-- Records with 'MovieID' and the movie's respective opening strength
opening_strength = FOREACH grouped_opening_strength {
  record_count = COUNT(opening_strength_records);
  GENERATE $0 AS MovieID, record_count AS OpeningStrength;
};

-- Joining with second input file to get movie name
join_movie_name = JOIN opening_strength BY MovieID, movie_title_records BY MovieID;

-- Parsing movie name and opening strength for final output
final_output = FOREACH join_movie_name GENERATE $4 AS MovieTitle, $1 AS OpeningStrength;


STORE final_output INTO '$OUTPUT';