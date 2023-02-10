# Demeter Database Setup
A guide for configuring your project to connect to a `demeter` database.

There are two main ways in which our team interacts with `demeter`. 
(1) We read and write data to the `demeter` database instances on AWS: `demeter-dev` and `demeter-prod`. 
(2) We spin up local `demeter-dev` database instances on our local machine for local development, fulfillment, and testing.

Let's walk through how to connect to accomplish both of these tasks.

## Before you begin

**Add an `.env` file**

The `.env` file holds the credentials necessary to connect to the desired Postgres server (see [python-dotenv](https://github.com/theskumar/python-dotenv) for more information). It should never be committed to Github (i.e., should be part of the `.gitignore`). See [`.env.template`](https://github.com/SenteraLLC/demeter/blob/main/.env.template) for an example, and ask @tnigon or @marissakivi if you have questions about credentials or set-up.

**.env.template**
```bash
DEMETER_TEST = "{'host':'localhost', 'port':'5433', 'username':'demeter_user', 'password':'abc123', 'database':'demeter-dev', 'schema_name':'test_demeter'}"
```

## Method 1: Connect to the AWS RDS `demeter_database` via an SSH tunnel.

To proceed with this method, you must obtain SSH access to the bastion server from Elliot or Sam ([see py-analytics-db repo](https://github.com/SenteraLLC/py-analytics-db#credentials)).
They will provide you with your bastion username. You will not be able to SSH into the bastion server but only tunnel through it. 

### Step 1: Connect to database through an SSH Tunnel
See [Connecting to a Database (safely)](https://sentera.atlassian.net/wiki/spaces/GML/pages/3173416965/Connecting+to+a+Database+safely#The-General-Problem) for more information.
``` bash
ssh -o ServerAliveInterval=36000 -i <FILE LOCATION OF IDENTITY FILE> -NL 127.0.0.1:<DEMETER_PG_PORT>:<DATABASE_LOCATION>:<SSH_PORT> <AWS_ANALYTICS_BASTION_USERNAME>@<SSH_HOST> -v
```

Note: The `-i` flag is optional.

**Example**
``` bash
ssh -o ServerAliveInterval=36000 -i "identity_key.pem" -NL 127.0.0.1:5433:demeter-database.cbqzrf0bsec9.us-east-1.rds.amazonaws.com:5432 my_bastion_user@bastion-lt-lb-369902c3f6e57f00.elb.us-east-1.amazonaws.com -v
```

### Step 2: Test your database connection (optional)
In a separate terminal window (running on your local machine), run the following line. It should prompt you for the `demeter_user` password, which you can get from Tyler or Marissa.
``` bash
psql --host localhost --port 5433 --user demeter_user demeter-dev
```

## Method 2: Create your own database locally (Mac)
Note: This method should work for Mac (did not work with WSL2).


### Step 1: Set up database (only need to do this once)

#### [Download](https://www.postgresql.org/download/)/Install PostgreSQL
```bash
sudo apt update
sudo apt install postgresql-14
```

#### Initial setup of server
Be sure the database service is running:
```bash
sudo service postrgresql start
```

Add the `postgres` user
```bash
sudo -u postgres createuser --interactive
```

#### Initialize a PostgreSQL cluster/server
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
You will be prompted for your system password. Use `exit` to return to normal user.

### Step 2: Initialize the database (only need to do when you want a fresh schema)

#### Start the server process
``` bash
sudo -i -u postgres
postgres -D /usr/local/pgsql/data
```

#### Initialize the database schema with a given name (see `initialize.demeter.py` for more information)
Open a different terminal window (the server needs to be running)
``` bash
python3 -m scripts.initialize_demeter --schema_name='demeter' --database_host='LOCAL'
```

### Step 3: Connect to the database

#### Start the server (needs to be running in the background)
``` bash
sudo -i -u postgres
postgres -D /usr/local/pgsql/data
```

#### Create connection
You can do this in one of the following ways:
1. PGAdmin (see ["Step 5" of README_wsl.md](https://github.com/SenteraLLC/demeter/blob/main/README_wsl.md#step-5-connect-to-wsls-postgres))
2. Python/Jupyter notebook (via `demeter`):


```python
from demeter.db import getConnection
conn = getConnection(env_name="DEMETER-DEV_LOCAL")
```

Be sure you've added a `.env` file with appropriate credentials ([see .env.template example above](https://github.com/SenteraLLC/demeter/blob/main/README_db_setup.md#before-you-begin)).
