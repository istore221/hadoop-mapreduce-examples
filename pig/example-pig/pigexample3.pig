-- count movies by directors

movies = LOAD 'movies.json' 
    USING JsonLoader('movie_id:int, 
    				  title:chararray,
                      tags: {(name:chararray)},
                      director: (id:int,name:chararray,age:int)');

/*


(100,Star Wars,{(Horror),(Action)},(1,john,21))
(200,Final Destination,{(Horror),(Thriller)},(2,smith,28))
(300,Finding Nemo,{(Animation),(Thriller)},(1,john,21))

*/

directors_gt_25 = FILTER movies BY director.age > 25;

/*

(200,Final Destination,{(Horror),(Thriller)},(2,smith,28))

*/

groupdirectors = GROUP movies BY director;

/*

((1,john,21),{(100,Star Wars,{(Horror),(Action)},(1,john,21)),(300,Finding Nemo,{(Animation),(Thriller)},(1,john,21))})
((2,smith,28),{(200,Final Destination,{(Horror),(Thriller)},(2,smith,28))})

*/

countresult = FOREACH groupdirectors GENERATE group.name,COUNT(movies);

/*

(john,2)
(smith,1)

*/

DUMP countresult;