## Requirements
1. Python 3.10.4 or above
2. Access to a Postgres database

## Postgres Server Setup
Use the .env file at the root of the project for Postgres credentials

### Required Environment Variables
- DEMETER\_PG\_HOST
- DEMETER\_PG\_DATABASE
- DEMETER\_PG\_OPTIONS
- DEMETER\_PG\_USER

### Optional Environment Variables
- DEMETER\_PG\_PORT
- DEMETER\_PG\_PASSWORD


### Method 1: Use an existing database
1) Locate a Postgres instance

A suggested approach is to use the existing analytics database using an SSH tunnel.
Note: You will need to have SSH credentials with the AWS Analytics Bastion

Example tunnel command
```
ssh -o ServerAliveInterval=36000 -vvv -L 127.0.0.1:5433:demeter-database.cbqzrf0bsec9.us-east-1.rds.amazonaws.com:5432 my-analytics-username-goes-here@bastion-lt-lb-369902c3f6e57f00.elb.us-east-1.amazonaws.com
```

2) Configure your '.env' file to point at the Postgres server.
A template .env file is in this repository root at '.env.template'

3) Test your connection to the database:
Example
```
psql --host localhost --port 5433 --user postgres postgres
```
You will have to ask somebody for the password


### Method 2: Create your own database locally
1) Download Postgres
https://www.postgresql.org/download/


2) Initialize a Postgres database cluster on disk with 'initdb'

Example:
```
initdb -D /usr/local/pgsql/data
```


3) Start the server process

Example:
```
postgres -D /usr/local/pgsql/data
```

4) Connect to you database with a command like this:
psql --host localhost --user my\_username -f schema.sql postgres


## Notes
This repository supports a root level '.env' file
[python-dotenv](https://github.com/theskumar/python-dotenv)



## Temporary Documentation
https://sentera.atlassian.net/wiki/spaces/GML/pages/3107520532/Demeter+Data+Types

There will soon be auto-generated Sphinx documents


## Scripts

1) Example
   A barebones example of using Demeter data classes and functions

\> python3 -m scripts.sample

```
Root group id: 75910
Argentina group id: 75911
Field Geom id: 105669
Field id: 250158
Irrigation type id: 90
Gallons type id: 118
Observation geom id:  105669
Local value id: 228730
```

2) Generate Graphviz chart of Postgres database
Make sure that you have dev-dependencies installed via Poetry.
```pg_dump --schema-only --schema test_mlops --host localhost --dbname postgres | python3 -m scripts.to_graphviz | dot -Tpng > schema.png```


## Troubleshooting
Installing GeoPandas for M1
https://stackoverflow.com/questions/71137617/error-installing-geopandas-in-python-on-mac-m1


## TODO

Guide for setting up user account postgres using 'postgres' account and 'read\_and\_write' role and \password <user>
  grant read\_and\_write to <user>;

