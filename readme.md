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

#### 1. Configure your `.env` file
Your `.env` file holds the credentials necessary to connect to the desired Postgres server. It should never be committed to Github (i.e., should be part of the `.gitignore`). See [`.env.template`](https://github.com/SenteraLLC/demeter/blob/main/.env.template) for an example, and ask @Joel-Larson or @tnigon if you have questions about credentials.

#### 2. Connect to an SSH Tunnel
**IMPORTANT**: Your account on the bastion machine exists only to hold the public portion of your cryptographic key(s). See [Connecting to a Database (safely)](https://sentera.atlassian.net/wiki/spaces/GML/pages/3173416965/Connecting+to+a+Database+safely#The-General-Problem) for more information.

``` bash
ssh -o ServerAliveInterval=36000 -vvv -L 127.0.0.1:<DEMETER_PG_PORT>:<DATABASE_NAME>:<SSH_PORT><AWS_ANALYTICS_BASTION_USERNAME>@<SSH_HOST>
```

For example:

``` bash
ssh -o ServerAliveInterval=36000 -vvv -L 127.0.0.1:5433:demeter-database.cbqzrf0bsec9.us-east-1.rds.amazonaws.com:5432 myname@bastion-lt-lb-369902c3f6e57f00.elb.us-east-1.amazonaws.com
```

#### 3. Test your database connection
``` bash
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


## Troubleshooting
Installing GeoPandas for M1
https://stackoverflow.com/questions/71137617/error-installing-geopandas-in-python-on-mac-m1


## TODO

Guide for setting up user account postgres using 'postgres' account and 'read\_and\_write' role and password <user>
  grant read\_and\_write to <user>;

