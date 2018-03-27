-- count movies by genere

REGISTER my-udf-0.0.1-SNAPSHOT.jar;

movies  = LOAD 'movies.txt' using PigStorage(',') AS (id:int,title:chararray,genere:chararray,did:int,dname:chararray,age:int);

/*

(100,Star Wars,horror|action,1,john,21)
(200,Final Destination,horror|thriller|suspence,2,smith,28)
(300,Finding Nemo,horror|thriller,1,john,21)

*/

movies_transform = FOREACH movies GENERATE id,title,FLATTEN(com.udf.my_udf.MYUDF(genere)) as genere,did,dname,age;

/*

com.udf.my_udf.MYUDF(genere) will genarate (100,Star Wars, {(horror),(action)},1,john,21) for (100,Star Wars,horror|action,1,john,21)
flattern will genarate row for each touple in the bag

(100,Star Wars,horror,1,john,21)
(100,Star Wars,action,1,john,21)
(200,Final Destination,horror,2,smith,28)
(200,Final Destination,thriller,2,smith,28)
(200,Final Destination,suspence,2,smith,28)
(300,Finding Nemo,horror,1,john,21)
(300,Finding Nemo,thriller,1,john,21)

*/


groupbygenere = GROUP movies_transform BY genere;

/*

(action,{(100,Star Wars,action,1,john,21)})
(horror,{(100,Star Wars,horror,1,john,21),(200,Final Destination,horror,2,smith,28),(300,Finding Nemo,horror,1,john,21)})
(suspence,{(200,Final Destination,suspence,2,smith,28)})
(thriller,{(200,Final Destination,thriller,2,smith,28),(300,Finding Nemo,thriller,1,john,21)})


*/

countgenere = FOREACH groupbygenere GENERATE group,COUNT(movies_transform);

/*

(action,1)
(horror,3)
(suspence,1)
(thriller,2)

*/

ordercountgenere = ORDER countgenere BY $1 DESC;

/*


(horror,3)
(thriller,2)
(action,1)
(suspence,1)

*/

DUMP ordercountgenere;