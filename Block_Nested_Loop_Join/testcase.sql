
--  Added by Kunal Kulkarni
--  Graduate Student in Computer Science at The Ohio State University 2014
--  kulkarni.120@osu.edu
--  Block Nested Loop Join Operator in Cloudera Impala

-- List of Test Cases Covered:
-- 1a Join on attribute of type INT between 2 tables
-- 1b Join on attribute of type STRING between 2 tables
-- 1c Join on attribute of type FLOAT between 2 tables
-- 1d Join on attribute of type BOOLEAN between 2 tables
-- 1e Join on attribute of type DOUBLE among 3 tables
-- 2a Join among multiple (3) tables
-- 2b Join among multiple (3) tables, select only join attributes
-- 3  Join on NULL values - Expected NULL is not considered as a match for join
-- 4a Join on attributes of mis-matching types INT and STRING
-- 4b Join on attribute of mis-matching types STRING and DOUBLE
-- 5  Join on attribute where there are no matching values - Expected 0 rows returned
-- 6  Self Join - Expected ERROR Duplicate table alias
-- 7  Join with GROUP BY
-- 8  Join with ORDER BY
-- 9  When either input to Join is Empty table
-- 10 Cross Join of 2 tables

-- Create 3 new tables and populate with values so as to test custom block nested loop Join operator fully

create table test_tab_one (id int, first_name string, last_name string, price float, married boolean, gpa double);

insert into test_tab_one (id,first_name,last_name,price,married,gpa) values (11,"rahul","kumar",NULL,true,3.31), (6,"anish","patil",3.45,true,3.69), (19,"arvind","shankar",NULL,false,3.88), (NULL,"kiran","pai",NULL,true,3.81), (8,"rahul","kumar",752.53,false,3.92);

create table test_tab_two (s_id int, age int, f_name string, l_name string, s_price float, s_married boolean, s_gpa double);

insert into test_tab_two (s_id,age,f_name,l_name,s_price,s_married,s_gpa) values (6,34,"karant","shankar",42.2,true,3.92), (18,38,"kiran","kulkarni",NULL,true,3.31), (11,47,"anish","sriram",NULL,false,4.00), (NULL,53,"sachin","shankar",34.5,false,3.69), (19,NULL,"anish","shankar",34.5,false,3.69);

create table test_tab_three (t_id int, t_name string,t_gpa double);

insert into test_tab_three (t_id,t_name,t_gpa) values (11,"raghav",3.31),(19,"kiran",3.69),(NULL,"suhas",3.72);

-- Create an Empty table having id int attribute for Testcase 9
create table empty_table (id int);

-- Show all tables with all their tuples
select * from test_tab_one;
select * from test_tab_two;
select * from test_tab_three;
select * from empty_table;

-----------------------------------------------------------------------------------------------------------
-- Testcase 1a - Join on attribute of type INT between 2 tables

-- Switch to built-in operator
set enable_custom_op=false;
EXPLAIN select * from test_tab_one join test_tab_two on test_tab_one.id = test_tab_two.s_id;
select * from test_tab_one join test_tab_two on test_tab_one.id = test_tab_two.s_id;

-- Switch to custom operator
set enable_custom_op=true;
EXPLAIN select * from test_tab_one join test_tab_two on test_tab_one.id = test_tab_two.s_id;
select * from test_tab_one join test_tab_two on test_tab_one.id = test_tab_two.s_id;

-----------------------------------------------------------------------------------------------------------
-- Testcase 1b - Join on attribute of type STRING between 2 tables

-- Switch to built-in operator
set enable_custom_op=false;
EXPLAIN select * from test_tab_one join test_tab_two on test_tab_one.first_name = test_tab_two.f_name;
select * from test_tab_one join test_tab_two on test_tab_one.first_name = test_tab_two.f_name;

-- Switch to custom operator
set enable_custom_op=true;
EXPLAIN select * from test_tab_one join test_tab_two on test_tab_one.first_name = test_tab_two.f_name;
select * from test_tab_one join test_tab_two on test_tab_one.first_name = test_tab_two.f_name;

-----------------------------------------------------------------------------------------------------------
-- Testcase 1c - Join on attribute of type FLOAT between 2 tables

-- Switch to built-in operator
set enable_custom_op=false;
EXPLAIN select test_tab_one.id,test_tab_two.s_id, test_tab_one.price,test_tab_two.s_price  from test_tab_one join test_tab_two on test_tab_one.price = test_tab_two.s_price;
select test_tab_one.id,test_tab_two.s_id, test_tab_one.price,test_tab_two.s_price  from test_tab_one join test_tab_two on test_tab_one.price = test_tab_two.s_price;

-- Switch to custom operator
set enable_custom_op=true;
EXPLAIN select test_tab_one.id,test_tab_two.s_id, test_tab_one.price,test_tab_two.s_price  from test_tab_one join test_tab_two on test_tab_one.price = test_tab_two.s_price;
select test_tab_one.id,test_tab_two.s_id, test_tab_one.price,test_tab_two.s_price  from test_tab_one join test_tab_two on test_tab_one.price = test_tab_two.s_price;

-----------------------------------------------------------------------------------------------------------
-- Testcase 1d - Join on attribute of type BOOLEAN between 2 tables

-- Switch to built-in operator
set enable_custom_op=false;
EXPLAIN select test_tab_one.id,test_tab_two.s_id, test_tab_one.married,test_tab_two.s_married  from test_tab_one join test_tab_two on test_tab_one.married = test_tab_two.s_married;
select test_tab_one.id,test_tab_two.s_id, test_tab_one.married,test_tab_two.s_married  from test_tab_one join test_tab_two on test_tab_one.married = test_tab_two.s_married;

-- Switch to custom operator
set enable_custom_op=true;
EXPLAIN select test_tab_one.id,test_tab_two.s_id, test_tab_one.married,test_tab_two.s_married  from test_tab_one join test_tab_two on test_tab_one.married = test_tab_two.s_married;
select test_tab_one.id,test_tab_two.s_id, test_tab_one.married,test_tab_two.s_married  from test_tab_one join test_tab_two on test_tab_one.married = test_tab_two.s_married;

-----------------------------------------------------------------------------------------------------------
-- Testcase 1e - Join on attribute of type DOUBLE among 3 tables

-- Switch to built-in operator
set enable_custom_op=false;
EXPLAIN select test_tab_one.id,test_tab_two.s_id, test_tab_three.t_id, test_tab_one.gpa,test_tab_two.s_gpa, test_tab_three.t_gpa  from test_tab_one join test_tab_two on test_tab_one.gpa = test_tab_two.s_gpa join test_tab_three on test_tab_two.s_gpa = test_tab_three.t_gpa;
select test_tab_one.id,test_tab_two.s_id, test_tab_three.t_id, test_tab_one.gpa,test_tab_two.s_gpa, test_tab_three.t_gpa  from test_tab_one join test_tab_two on test_tab_one.gpa = test_tab_two.s_gpa join test_tab_three on test_tab_two.s_gpa = test_tab_three.t_gpa;

-- Switch to custom operator
set enable_custom_op=true;
EXPLAIN select test_tab_one.id,test_tab_two.s_id, test_tab_three.t_id, test_tab_one.gpa,test_tab_two.s_gpa, test_tab_three.t_gpa  from test_tab_one join test_tab_two on test_tab_one.gpa = test_tab_two.s_gpa join test_tab_three on test_tab_two.s_gpa = test_tab_three.t_gpa;
select test_tab_one.id,test_tab_two.s_id, test_tab_three.t_id, test_tab_one.gpa,test_tab_two.s_gpa, test_tab_three.t_gpa  from test_tab_one join test_tab_two on test_tab_one.gpa = test_tab_two.s_gpa join test_tab_three on test_tab_two.s_gpa = test_tab_three.t_gpa;

-----------------------------------------------------------------------------------------------------------
-- Testcase 2a - Join among multiple (3) tables

-- Switch to built-in operator
set enable_custom_op=false;
EXPLAIN select * from test_tab_one join test_tab_two on test_tab_one.id = test_tab_two.s_id join test_tab_three on test_tab_two.s_id = test_tab_three.t_id;
select * from test_tab_one join test_tab_two on test_tab_one.id = test_tab_two.s_id join test_tab_three on test_tab_two.s_id = test_tab_three.t_id;

-- Switch to custom operator
set enable_custom_op=true;
EXPLAIN select * from test_tab_one join test_tab_two on test_tab_one.id = test_tab_two.s_id join test_tab_three on test_tab_two.s_id = test_tab_three.t_id;
select * from test_tab_one join test_tab_two on test_tab_one.id = test_tab_two.s_id join test_tab_three on test_tab_two.s_id = test_tab_three.t_id;

-----------------------------------------------------------------------------------------------------------
-- Testcase 2b - Join among multiple (3) tables, select only join attributes

-- Switch to built-in operator
set enable_custom_op=false;
EXPLAIN select test_tab_one.id,test_tab_two.s_id, test_tab_three.t_id  from test_tab_one join test_tab_two on test_tab_one.id = test_tab_two.s_id join test_tab_three on test_tab_two.s_id = test_tab_three.t_id;
select test_tab_one.id,test_tab_two.s_id, test_tab_three.t_id  from test_tab_one join test_tab_two on test_tab_one.id = test_tab_two.s_id join test_tab_three on test_tab_two.s_id = test_tab_three.t_id;

-- Switch to custom operator
set enable_custom_op=true;
EXPLAIN select test_tab_one.id,test_tab_two.s_id, test_tab_three.t_id  from test_tab_one join test_tab_two on test_tab_one.id = test_tab_two.s_id join test_tab_three on test_tab_two.s_id = test_tab_three.t_id;
select test_tab_one.id,test_tab_two.s_id, test_tab_three.t_id  from test_tab_one join test_tab_two on test_tab_one.id = test_tab_two.s_id join test_tab_three on test_tab_two.s_id = test_tab_three.t_id;

-----------------------------------------------------------------------------------------------------------
-- Testcase 3 - Join on NULL values - Expected NULL is not considered as a match for join

-- Switch to built-in operator
set enable_custom_op=false;
EXPLAIN select * from test_tab_one join test_tab_two on test_tab_one.id = test_tab_two.s_id;
select * from test_tab_one join test_tab_two on test_tab_one.id = test_tab_two.s_id;

-- Switch to custom operator
set enable_custom_op=true;
EXPLAIN select * from test_tab_one join test_tab_two on test_tab_one.id = test_tab_two.s_id;
select * from test_tab_one join test_tab_two on test_tab_one.id = test_tab_two.s_id;

-----------------------------------------------------------------------------------------------------------
-- Testcase 4a - Join on attributes of mis-matching types INT and STRING

-- Switch to built-in operator
set enable_custom_op=false;
EXPLAIN select * from test_tab_one join test_tab_two on test_tab_one.id = test_tab_two.f_name;
select * from test_tab_one join test_tab_two on test_tab_one.id = test_tab_two.f_name;

-- Switch to custom operator
set enable_custom_op=true;
EXPLAIN select * from test_tab_one join test_tab_two on test_tab_one.id = test_tab_two.f_name;
select * from test_tab_one join test_tab_two on test_tab_one.id = test_tab_two.f_name;

-----------------------------------------------------------------------------------------------------------
-- Testcase 4b - Join on attribute of mis-matching types STRING and DOUBLE

-- Switch to built-in operator
set enable_custom_op=false;
EXPLAIN select * from test_tab_one join test_tab_two on test_tab_one.first_name = test_tab_two.s_gpa;
select * from test_tab_one join test_tab_two on test_tab_one.first_name = test_tab_two.s_gpa;

-- Switch to custom operator
set enable_custom_op=true;
EXPLAIN select * from test_tab_one join test_tab_two on test_tab_one.first_name = test_tab_two.s_gpa;
select * from test_tab_one join test_tab_two on test_tab_one.first_name = test_tab_two.s_gpa;

-----------------------------------------------------------------------------------------------------------
-- Testcase 5 - Join on attribute where there are no matching values - Expected 0 rows returned

-- Switch to built-in operator
set enable_custom_op=false;
EXPLAIN select * from test_tab_one join test_tab_two on test_tab_one.id = test_tab_two.age;
select * from test_tab_one join test_tab_two on test_tab_one.id = test_tab_two.age;

-- Switch to custom operator
set enable_custom_op=true;
EXPLAIN select * from test_tab_one join test_tab_two on test_tab_one.id = test_tab_two.age;
select * from test_tab_one join test_tab_two on test_tab_one.id = test_tab_two.age;

-----------------------------------------------------------------------------------------------------------
-- Testcase 6 - Self Join - Expected ERROR Duplicate table alias

-- Switch to built-in operator
set enable_custom_op=false;
EXPLAIN select * from test_tab_one join test_tab_one on test_tab_one.id = test_tab_one.id;
select * from test_tab_one join test_tab_one on test_tab_one.id = test_tab_one.id;

-- Switch to custom operator
set enable_custom_op=true;
EXPLAIN select * from test_tab_one join test_tab_one on test_tab_one.id = test_tab_one.id;
select * from test_tab_one join test_tab_one on test_tab_one.id = test_tab_one.id;

-----------------------------------------------------------------------------------------------------------
-- Testcase 7 - Join with GROUP BY

-- Switch to built-in operator
set enable_custom_op=false;
EXPLAIN select test_tab_one.id,test_tab_two.s_id from test_tab_one join test_tab_two on test_tab_one.id = test_tab_two.s_id GROUP BY test_tab_one.id, test_tab_two.s_id;
select test_tab_one.id,test_tab_two.s_id from test_tab_one join test_tab_two on test_tab_one.id = test_tab_two.s_id GROUP BY test_tab_one.id, test_tab_two.s_id;

-- Switch to custom operator
set enable_custom_op=true;
EXPLAIN select test_tab_one.id,test_tab_two.s_id from test_tab_one join test_tab_two on test_tab_one.id = test_tab_two.s_id GROUP BY test_tab_one.id, test_tab_two.s_id;
select test_tab_one.id,test_tab_two.s_id from test_tab_one join test_tab_two on test_tab_one.id = test_tab_two.s_id GROUP BY test_tab_one.id, test_tab_two.s_id;

-----------------------------------------------------------------------------------------------------------
-- Testcase 8 - Join with GROUP BY

-- Switch to built-in operator
set enable_custom_op=false;
EXPLAIN select test_tab_one.id,test_tab_two.s_id, test_tab_one.price,test_tab_two.s_price  from test_tab_one join test_tab_two on test_tab_one.id = test_tab_two.s_id ORDER BY test_tab_one.id, test_tab_two.s_id;
select test_tab_one.id,test_tab_two.s_id, test_tab_one.price,test_tab_two.s_price  from test_tab_one join test_tab_two on test_tab_one.id = test_tab_two.s_id ORDER BY test_tab_one.id, test_tab_two.s_id;

-- Switch to custom operator
set enable_custom_op=true;
EXPLAIN select test_tab_one.id,test_tab_two.s_id, test_tab_one.price,test_tab_two.s_price  from test_tab_one join test_tab_two on test_tab_one.id = test_tab_two.s_id ORDER BY test_tab_one.id, test_tab_two.s_id;
select test_tab_one.id,test_tab_two.s_id, test_tab_one.price,test_tab_two.s_price  from test_tab_one join test_tab_two on test_tab_one.id = test_tab_two.s_id ORDER BY test_tab_one.id, test_tab_two.s_id;

-----------------------------------------------------------------------------------------------------------
-- Testcase 9 - When either input table to Join is an Empty table

-- Switch to built-in operator
set enable_custom_op=false;
EXPLAIN select * from test_tab_one join empty_table on test_tab_one.id = empty_table.id;
select * from test_tab_one join empty_table on test_tab_one.id = empty_table.id;

-- Switch to custom operator
set enable_custom_op=true;
EXPLAIN select * from test_tab_one join empty_table on test_tab_one.id = empty_table.id;
select * from test_tab_one join empty_table on test_tab_one.id = empty_table.id;
-----------------------------------------------------------------------------------------------------------
-- Testcase 10 - Cross Join between 2 tables

-- Switch to built-in operator
set enable_custom_op=false;
EXPLAIN select test_tab_one.id,test_tab_two.s_id, test_tab_one.married,test_tab_two.s_married  from test_tab_one cross join test_tab_two;
select test_tab_one.id,test_tab_two.s_id, test_tab_one.married,test_tab_two.s_married  from test_tab_one cross join test_tab_two;

-- Switch to custom operator
set enable_custom_op=true;
EXPLAIN select test_tab_one.id,test_tab_two.s_id, test_tab_one.married,test_tab_two.s_married  from test_tab_one cross join test_tab_two;
select test_tab_one.id,test_tab_two.s_id, test_tab_one.married,test_tab_two.s_married  from test_tab_one cross join test_tab_two;

-----------------------------------------------------------------------------------------------------------
