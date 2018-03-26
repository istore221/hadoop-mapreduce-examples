people  = LOAD '/Users/kalanathejitha/Desktop/people.csv' USING PigStorage(',') AS (name:chararray, country:chararray,gender:chararray, sport:chararray, height:float);

data = GROUP people BY gender;



