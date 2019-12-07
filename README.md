# Construction of the **Song Play Database**

This repository sets for the codes that setup the Song Play Database. It is an analytical database that includes multiple tables such as users, songs, artists, time, and song plays. It allows us to query into these fact and dimensional tables and derive unique business insights for this online song playing platform. These analytical tables are inserted from JSON files stored in the s3 storage. Therefore, staging tables need to be created first.

As for the codes,

* *sql_queries.py* contains the SQL queries that generate the tables, including both the staging tables and the analytical tables, in the redshift database. For the staging tables, it uses the COPY command to directly copy the JSON files from s3. For the analytical tables, it uses the INSERT command to insert the data from the staging tables.
* *create_tables.py* connects to the AWS redshift cluster and creates the tables accordingly using the queries in the preceding file.
* *etl.py* connects to the AWS redshift cluster, copies the data from s3 into the staging tables, and inserts the data into the analytical tables from these staging tables.

## Getting Started

The starting point is the queries. I simply write down the queries to create the tables and to insert the data into these tables. I also try to reuse some of the codes from the previous project because of their overlaps. For external libraries, we particularly rely on the [psycopg2](https://pypi.org/project/psycopg2/) package to connect to the AWS redshift database we created. You can install this package by running

```cmd
pip install psycopg2
```

in your terminal.

## Table Design

The most important perspectives for table design with regard to redshift analytical tables are *sort key*, *distribution key*, and *distribution style* (for more information, please refer to the [doc](https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html)).

For the sort key, I simply define all the primary keys of the tables as sort keys. For the distribution style, I distribute the dimension tables to all the nodes, for the reason that the fact table, songplays, may need to draw data from each of these dimensional tables. As for the fact table, I assign the 'auto' method, figuring that this table may start as small and quickly grow bigger as the business attracts more customers, whereas the dimension tables may grow in a much slower pace. However, we may want to change their distribution methods in a later stage when these tables get too big.

## Populating Tables

Rather than using the INSERT command to insert the values row by row, I combine INSERT INTO with SELECT to insert the values from the staging tables in blocks and thus accelerate the process.

For the **users** table, I need to select the user id with the latest timestamp to make sure that the *level* column is up to date.

## Some Other Improvements

I simply add try-exception pairs to the functions so that the codes are more traceable and robust. I also add docstrings to explain the functions.

## Usage

We can use some popular dashboard tools to create dashboards out of this database, for example, [plotly](https://plot.ly), [Apache Superset](https://superset.incubator.apache.org), or [streamlit](https://streamlit.io).