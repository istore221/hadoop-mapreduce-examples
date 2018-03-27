-- count genere occurence

movies = LOAD 'movies.json' 
    USING JsonLoader('movie_id:int, 
    				  title:chararray,
                      tags: {(name:chararray)}');

 /*
 
 (100,Star Wars,{(Horror),(Action)})
(200,Final Destination,{(Horror),(Thriller)})
(300,Finding Nemo,{(Animation),(Thriller)})

 */                     

flatten1 = FOREACH movies GENERATE movie_id,title,FLATTEN(tags) as tag;

/*

(100,Star Wars,Horror)
(100,Star Wars,Action)
(200,Final Destination,Horror)
(200,Final Destination,Thriller)
(300,Finding Nemo,Animation)
(300,Finding Nemo,Thriller)

*/

groupbygenere = GROUP flatten1 BY tag; 


/*

(Action,{(100,Star Wars,Action)})
(Horror,{(100,Star Wars,Horror),(200,Final Destination,Horror)})
(Thriller,{(200,Final Destination,Thriller),(300,Finding Nemo,Thriller)})
(Animation,{(300,Finding Nemo,Animation)})

*/


countgenere = FOREACH groupbygenere GENERATE group,COUNT(flatten1);

/*


(Action,1)
(Horror,2)
(Thriller,2)
(Animation,1)

*/

DUMP countgenere;
                   
