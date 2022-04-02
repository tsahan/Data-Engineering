# Project: Data Modeling with Postgres

## Summary and Purpose:

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis.

## How to run the Python scripts:

1. Run `create_tables.py` at least once to create the sparkifydb database, which these other files connect to.
2. Run `python etl.py` to see the results of the ETL process.
3. Run `test.ipynb` to confirm the creation of the tables with the correct columns. Make sure to click "Restart kernel" to close the connection to the database after running this notebook.


## Files:

1. `test.ipynb` displays the first few rows of each table to let you check your database.
2. `create_tables.py` drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.
3. `etl.ipynb` reads and processes a single file from `song_data` and `log_data` and loads the data into your tables. This notebook contains detailed instructions on the ETL process for each of the tables.
4. `etl.py` reads and processes files from `song_data` and `log_data` and loads them into your tables. You can fill this out based on your work in the ETL notebook.
5. `sql_queries.py` contains all your sql queries, and is imported into the last three files above.
6. `README.md` provides discussion on your project.

## Database schema design and ETL pipeline:

A star schema database design is preferred for analyzing data more efficiently.
Star schema simplifies queries and minimizes number of joins for fast analytical performance.

![Schema](/images/schema_design.png)
