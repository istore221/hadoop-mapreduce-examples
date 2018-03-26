-- http://joshualande.com/cube-rollup-pig-data-science

people  = LOAD 'people.csv' USING PigStorage(',')
   		 AS (name:chararray, country:chararray,
   		 gender:chararray, sport:chararray, height:float);

groupedbygender = GROUP people BY gender;

heights = FOREACH groupedbygender GENERATE
    group AS gender,
    COUNT(people) AS num_people,
    AVG(people.height) AS avg_height;

grouped = GROUP people BY (gender,sport);

cubed = CUBE people BY CUBE(gender,sport);

test = FOREACH cubed GENERATE group.gender;


explain groupedbygender;
