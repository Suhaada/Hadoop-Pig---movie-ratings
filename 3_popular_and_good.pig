ratings = LOAD '/user/maria_dev/movie_dataset/u.data' AS (userID: int, movieID: int, rating: int, ratingTime: int);

metadata = LOAD '/user/maria_dev/movie_dataset/u.item' USING PigStorage('|') 
	AS (movieID: int, movieTitle: chararray, releaseDate: chararray, videoRelease: chararray, imdblink: chararray);

nameLookup = FOREACH metadata GENERATE movieID, movieTitle;

groupedRatings = Group ratings BY movieID; 

averageRatings = FOREACH groupedRatings GENERATE group AS movieID, 
	AVG(ratings.rating) AS avgRating, COUNT(ratings.rating) AS numRatings;
	
badMovies = FILTER averageRatings BY avgRating > 4.0;

namedBadMovies = JOIN badMovies BY movieID, nameLookup BY movieID;

finalResults = FOREACH namedBadMovies GENERATE nameLookup::movieTitle AS movieName,
	badMovies::avgRating AS avgRating, badMovies::numRatings AS numRatings;
	
finalResultsSorted = ORDER finalResults BY numRatings ASC;

DUMP finalResultsSorted;
