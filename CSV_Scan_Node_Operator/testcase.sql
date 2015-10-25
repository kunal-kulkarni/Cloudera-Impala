
--  Added by Kunal Kulkarni
--  Graduate Student in Computer Science at The Ohio State University 2014
--  kulkarni.120@osu.edu
--  CSV Scan Node Operator in Cloudera Impala
-- Below are the test cases

set enable_custom_op=true;

-- 1.) Query to select all for number of rows in CSV file  > 1024. This test case should be run on a CSV file which contains number of rows > 1024. I have used the submitted  extest2.csv file

explain select * from extest;
select * from extest;

----------------------------------------------------------------------------------------------------------------

-- Rest of the text cases were run on the submitted extest1.csv file which contains escaped comma characters. However there is no constraint that they have to be run on a particular CSV file

----------------------------------------------------------------------------------------------------------------
-- 2.) The string field contains escaped comma characters. For example if the file contains 'Ahoy\, Matey!'it should be parsed and extracted as 'Ahoy, Matey!' as one cell unit and this value should show up in the query result

explain select * from extest;
select * from extest;

----------------------------------------------------------------------------------------------------------------

-- 3.) Escaped comma is present in double field. For example 123.1\,23 is extracted as 123.1,23 and passed to double. Should result in NULL value as the double value cannot have commas. Observe the first row in the query result

select * from extest where id=1;

----------------------------------------------------------------------------------------------------------------

-- 4.) Select only particular columns. Verify the correct matching values are generated in the result

explain select col_2  from extest;
select col_2  from extest;

----------------------------------------------------------------------------------------------------------------
-- 5.) Select columns in any order in select clause. Verify the correct matching values are generated in the result

select col_2, id, col_1  from extest;

----------------------------------------------------------------------------------------------------------------
-- 6.) Where clause. Select condition based on say col_1

select * from extest where col_1=true;

----------------------------------------------------------------------------------------------------------------
-- 7a.) where clause with and

select * from extest where col_1=true and id=1;

----------------------------------------------------------------------------------------------------------------
-- 8.) where clause with or

select id,col_1 from extest where col_1=true or id=3;

----------------------------------------------------------------------------------------------------------------
-- 9.) Group By

select id from extest where id > 0 group by id;

----------------------------------------------------------------------------------------------------------------
-- 10.) Order By - verify results are arranged in ascending order of id

select * from extest where id > 1 order by id;

----------------------------------------------------------------------------------------------------------------
-- 11.) Join - on itself, Verify all rows are returned. (Could not use other tables as they were returning 0 rows when enable_custom_op=true;)

select * from extest where extest.id = extest.id;

----------------------------------------------------------------------------------------------------------------
-- 12.) Join - on itself but on non-matching attributes. Verify 0 rows is returned.

select * from extest where extest.id = extest.col_2;

----------------------------------------------------------------------------------------------------------------
