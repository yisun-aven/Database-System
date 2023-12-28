# Database System Design Project
# Overview
This is a database system that supports both relational and NoSQL dataset. 
It avoids memory issue when dealing with large dataset by using different algorithms.
It also supports various functions and queries.

# Support Models
- Relational Database:
    - Self-Created tables
    - CSV files
- NoSQL Database:
    - json files

# Data Storage Design
- rootDirectory -> MyDB
    - Database -> Store Actual Database & Imported Dataset
        - Database directory contains subdirectories dbs (db1, db2, db3...)
        - User can specifies existing database or create a new one
        - db
            - Each db has a file storing MetaData of the db
            - MetaData contains all the tables names of the db
            - Each db has a tables directory
                - Contains all the tables directories
                - tabel1, table2, table3....
                    - Inside each table are chuncked files number 1 to n 
                    - If joining/sorting, we will have additional joinning/sorting folders 
                        - Joined/Sorted files are chuncked as well 
    - External -> Store All the Downloaded External CSV/json files 

# Files & Structure
- db_manager.py
    - This file handles the loading and creation of database in the beginning
    - If the database already exists, it will load the database
    - If the database does not exists, it will ask the user if they want to create this new database
- file_handler.py
    - This file handles imported external dataset by loading & saving in chunks to prevent memory issue
- mydb.py
    - This file handles all the basic functions of the system
- cli.py
    - This file handles all the input command and split them into tokens
    - Tokens will be interpret and functions form mydb and other file will be called to handle the command accordingly
- helper.py
    - This file handles has functions that matches pattern and evaluate conditions
- convert.py
    - This file only gets called on NoSQL database when we have to convert .json format file into csv
    - It flatterns the nested structure of the json file
- external_sort.py
    - This file handels the sorting of the tables
    - Uses external sort
- join.py
    - This file handels the joining of different tables
    - Uses sort-merge join
- aggregation.py
    - This file handels the aggregation functions
- main.py
    - This file is where we execute the entire system 

# System Setup:
- Install tabulate
```bash
pip3 install tabulate
```
- Install ijson
```bash
pip3 install ijson
```
- Execute main.py
```bash
python3 main.py
```
- root storage directory myDB will be automatically created under the current working directory

# Project Qeuries Sample Command
```bash
Specify database when prompted
- any name (db1, db2, db3...)
When ask (yes/no)
- yes
```

Step1: Simple queries on self created dataset
```bash
Create person:

- create table person (name string, age int, primary key name)
- show tables (there should be a person table) 
- show table person


Create student:
- create table student (name string, sid int, primary key name)
- show tables (there should be a person, and a student table) 
- show table student

Insert person:
- insert table person {name:"kevin", age:20}
- insert table person {name:"Aven", age:23}
- insert table person {name:"Mike", age:28}
- insert table person {name:"Phoebee", age:22}
- show table person (there should be four person in the tables)
- insert table person {name:"Phoebee", age:20}
(Should show duplicate key constraint)

Insert student:
- insert table student {name:"kevin", sid:100}
- insert table student {name:"Aven", sid:300}
- insert table student {name:"Mike", sid:500}
- insert table student {name:"Ian", sid:400}
- insert table student {name:"Phoebee", sid:200}
- show table student  (there should be four students in the tables)
- insert table student {name:"Aven", sid:300}
(Should show duplicate key constraint)

Find (Projection): 
- FIND everything ON person
- FIND name sid ON student

IF (Filtering):
- FIND everything ON person IF age > 21
- FIND everything ON student IF name = Mike

JOIN: 
- FIND everything ON person JOIN student BY name EQUAL name
- FIND everything ON person JOIN student BY name EQUAL name in descending ORDER OF age

ORDER BY:
- FIND everything ON person JOIN student BY name EQUAL name in ascending ORDER OF age
- FIND everything ON person in ascending ORDER OF age
- FIND everything ON student in ascending ORDER OF sid
- FIND everything ON student in descending ORDER OF sid

Update:
- update ON person SET age = 20 IF age > 20
- show table person
- update ON person SET name = Nathan IF name = Mike
- show table person

Delete:
- delete ON student IF name = Mike
- show table student

Drop:
- drop table person
- drop table student
- show tables
```
Step2: Imported Dataset
```bash
Load:
- load shop1 'External/customer_shopping_data.csv'
- load shop2 'External/customer_shopping_data2.csv'
- show tables

Show:
- show table shop1
- show table shop2

Insert: 
- insert table shop1 {invoice_no: "I000000", customer_id: "C000000", gender:"Male", category:"Unknown", quantity:2, payment_method:"Cash", invoice_date:"Today"}
- insert table shop2 {customer_id: "C000000", age: 16, price: 4000, shopping_mall:"USC"}
- FIND everything ON shop1 IF invoice_date = Today
- FIND everything ON shop2 IF age < 19

Find (Projection) and Filtering:
- FIND everything ON shop1 IF gender = Male
- FIND everything ON shop2 IF shopping_mall = Kanyon AND price > 1000
- FIND customer_id price shopping_mall ON shop2 IF shopping_mall = Kanyon AND price > 1000
- FIND customer_id price shopping_mall ON shop2 IF shopping_mall = Kanyon OR price > 1000

JOIN:
- FIND everything ON shop1 JOIN shop2 BY customer_id EQUAL customer_id
- FIND customer_id gender age ON shop2 JOIN shop1 BY customer_id EQUAL customer_id in ascending ORDER OF age
- FIND everything ON shop1 IF gender = Female AND payment_method = Cash JOIN shop2 BY customer_id EQUAL customer_id
- FIND everything ON shop1 IF age > 60 AND gender = Male JOIN shop2 BY customer_id EQUAL customer_id

ORDER BY:
- FIND everything ON shop1 in descending ORDER OF quantity
- FIND everything ON shop2 in ascending ORDER OF age
- FIND everything ON shop1 IF gender = Female AND payment_method = Cash JOIN shop2 BY customer_id EQUAL customer_id in ascending ORDER OF quantity
- FIND everything ON shop2 IF age > 50 AND payment_method = Cash JOIN shop1 BY customer_id EQUAL customer_id in ascending ORDER OF age

Aggregation and Group By:
- FIND COUNT OF category ON shop1 IF gender = Male
- FIND COUNT OF category ON shop1 IF payment_method = Cash
- FIND price SUM OF shopping_mall ON shop2 IF age > 60 in ascending ORDER
- FIND COUNT OF category ON shop1 IF price > 2000 JOIN shop2 BY customer_id EQUAL customer_id

Update:
- update ON shop1 SET gender = M IF gender = Male
- show table shop1

Delete:
- delete ON shop1 IF gender = M

Drop:
- drop table shop1
- drop table shop2

Exit:
- exit()
```


NoSQL Database demo:
open the NoSQL Database

Step1: schedules dataset
```bash
Load:
- load schedules 'External/schedules.json'
- view tables

View:
- view table schedules

Projection (SHOW):
- SHOW everything IN schedules 
- SHOW station_name train_name train_number IN schedules

FILTERING:
- SHOW everything IN schedules IF station_name = Mhow

Update:
- update ON schedules SET arrival = tomorrow IF arrival = None
- SHOW everything IN schedules

Delete:
- SHOW everything IN schedules IF departure = None
- delete ON schedules IF departure = None
- SHOW everything IN schedules IF departure = None

Drop:
- drop table schedules
```
Step2: country, city, countrylanguage datasets:
```bash
Load: 
- load city 'External/city.json'
- load country 'External/countryf.json'
- load countrylanguage 'External/countrylanguage.json'

View:
- view tables
- view table city
- view table country
- view table countrylanguage

Projection (SHOW):
- SHOW everything IN city 
- SHOW Population Name LifeExpectancy Continent IN country

ORDER:
- SHOW everything IN country in descending ORDER OF LifeExpectancy
- SHOW everything IN country in ascending ORDER OF Population

JOIN:
- SHOW everything IN country COMBINE city BY Code EQUAL CountryCode
- SHOW everything IN country IF LifeExpectancy > 70 COMBINE city BY Code EQUAL CountryCode
- SHOW everything IN country COMBINE city BY Code EQUAL CountryCode in descending ORDER OF LifeExpectancy

Aggregation and GroupBy:
- SHOW COUNT OF Continent IN country
- SHOW GNP SUM OF Continent IN country 

Exit:
- exit()
```
