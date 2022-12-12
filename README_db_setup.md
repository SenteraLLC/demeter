# Demeter Database Setup
A guide for configuring your project to connect to a `demeter` database schema.

## Before you begin

**Add an `.env` file**

The `.env` file holds the credentials necessary to connect to the desired Postgres server (see [python-dotenv](https://github.com/theskumar/python-dotenv) for more information). It should never be committed to Github (i.e., should be part of the `.gitignore`). See [`.env.template`](https://github.com/SenteraLLC/demeter/blob/main/.env.template) for an example, and ask @tnigon or @marissakivi if you have questions about credentials.

**.env.template**
```bash
DEMETER_PG_HOST=localhost
DEMETER_PG_DATABASE=postgres
DEMETER_PG_OPTIONS=-c search_path=test_demeter,public
DEMETER_PG_USER=postgres
DEMETER_PG_PASSWORD=abc123
DEMETER_PG_PORT=5433
```

## Method 1: Connect to an existing database (via an SSH tunnel)

To proceed with this method, you must obtain SSH access from Elliot or Sam ([see py-analytics-db repo](https://github.com/SenteraLLC/py-analytics-db#credentials)).

**IMPORTANT**: Your account on the bastion machine exists only to hold the public portion of your cryptographic key(s). See [Connecting to a Database (safely)](https://sentera.atlassian.net/wiki/spaces/GML/pages/3173416965/Connecting+to+a+Database+safely#The-General-Problem) for more information.

### Step 1: Connect to the SSH Tunnel
``` bash
ssh -o ServerAliveInterval=36000 -vvv -L 127.0.0.1:<DEMETER_PG_PORT>:<DATABASE_NAME>:<SSH_PORT><AWS_ANALYTICS_BASTION_USERNAME>@<SSH_HOST>
```

**Example**
``` bash
ssh -o ServerAliveInterval=36000 -vvv -L 127.0.0.1:5433:demeter-database.cbqzrf0bsec9.us-east-1.rds.amazonaws.com:5432 myname@bastion-lt-lb-369902c3f6e57f00.elb.us-east-1.amazonaws.com
```

### Step 2: Test your database connection
``` bash
psql --host localhost --port 5433 --user postgres postgres
```

## Method 2: Create your own database locally (Mac)
Note: This method should work for Mac (did not work with WSL2).

### Step 1: [Download](https://www.postgresql.org/download/)/Install PostgreSQL
```bash
sudo apt update
sudo apt install postgresql-14
```

### Step 2: Initial setup

Be sure the database is running:
```bash
sudo service postrgresql start
```

Add the `postgres` user
```bash
sudo -u postgres createuser --interactive
```

### Step 3: Initialize a PostgreSQL cluster
Create a `/usr/local/pgsql` directory and assign ownership to the `postgres` user:
```bash
sudo mkdir /usr/local/pgsql
sudo chown postgres /usr/local/pgsql
```

Log in with your `postgres` user and `initdb`:
``` bash
sudo -i -u postgres
initdb -D /usr/local/pgsql/data
```

### Step 4: Start the server process
``` bash
postgres -D /usr/local/pgsql/data
```

### Step 5: Initialize the database schema
``` bash
psql --host localhost --user postgres -f schema.sql postgres
```

### Step 6: Connect to the database
You can do this in one of the following ways:
1. PGAdmin (see ["Step 5" of README_wsl.md](https://github.com/SenteraLLC/demeter/blob/main/README_wsl.md#step-5-connect-to-wsls-postgres))
2. Python/Jupyter notebook (via `demeter`):


```python
from demeter.db import getConnection
c = getConnection()
```

Be sure to you've added a `.env` file with appropriate credentials ([see .env.template example above](https://github.com/SenteraLLC/demeter/blob/main/README_db_setup.md#before-you-begin)).