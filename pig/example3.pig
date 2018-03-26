customers  = LOAD 'customers.txt' USING PigStorage(',')
   		 AS (id:int, name:chararray,
   		 age:int, address:chararray, salary:float);


orders = LOAD 'orders.txt' USING PigStorage(',')
 	 AS (oid:int, date:chararray, customer_id:int, amount:int);


paracustomers  = LOAD '$input' USING PigStorage(',')
                 AS (id:int, name:chararray,
                 age:int, address:chararray, salary:float);

groupbycustomer = GROUP orders BY customer_id;


count = foreach groupbycustomer generate group AS customerid ,COUNT(orders);

joined = JOIN count BY customerid, customers BY id;

joined2 = JOIN groupbycustomer BY group, customers BY id;

final = foreach joined2 generate name,COUNT(orders);

join3 = JOIN customers BY id , orders BY  customer_id;

join4 = GROUP customers ALL;

res1 = FOREACH join4 GENERATE $1 AS customers;

nulltest = FOREACH customers GENERATE name,NULL AS bla,salary;

filternull = FILTER nulltest BY bla IS  NULL;

dump paracustomers;
