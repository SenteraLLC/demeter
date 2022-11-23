# demeter

The database schema for agronomic modeling and supporting data science activities.

## Setup and Installation (for development)
1) [Set up SSH](https://github.com/SenteraLLC/install-instructions/blob/master/ssh_setup.md)
2) Install [pyenv](https://github.com/SenteraLLC/install-instructions/blob/master/pyenv.md) and [poetry](https://python-poetry.org/docs/#installation).
3) Install package
``` bash
git clone git@github.com:SenteraLLC/demeter.git
cd demeter
pyenv install $(cat .python-version)
poetry config virtualenvs.in-project true
poetry env use $(cat .python-version)
poetry install
```
4) Set up `pre-commit` to ensure all commits to adhere to **black** and **PEP8** style conventions.
``` bash
poetry run pre-commit install
```

## Setup and Installation (used as a library)
If using `demeter` as a dependency in your script, simply add it to the `pyproject.toml` in your project repo. Be sure to uuse the `ssh:` prefix so Travis has access to the repo for the library build process.

<h5 a><strong><code>pyproject.toml</code></strong></h5>

``` toml
[tool.poetry.dependencies]
demeter = { git = "ssh://git@github.com/SenteraLLC/demeter.git", branch = "main"}
```

Install `demeter` and all its dependencies via `poetry install`.

``` console
poetry install
```

## Requirements
- Python `3.10.4`+
- Access to a Postgres database (with connection credentials)
- A local installation of PostgresQL
- OSX Users may need to manually install the 'gdal' system requirement (e.g. brew install gdal)

## Configure `.env` file
Your `.env` file holds the credentials necessary to connect to the desired Postgres server (see [python-dotenv](https://github.com/theskumar/python-dotenv) for more information). It should never be committed to Github (i.e., should be part of the `.gitignore`). See [`.env.template`](https://github.com/SenteraLLC/demeter/blob/main/.env.template) for an example, and ask @Joel-Larson or @tnigon if you have questions about credentials.

**Environment Variables**
- `DEMETER_PG_HOST`
- `DEMETER_PG_DATABASE`
- `DEMETER_PG_OPTIONS`
- `DEMETER_PG_USER`
- `DEMETER_PG_PORT` (optional)
- `DEMETER_PG_PASSWORD` (optional)


## Postgres Server Setup
If you haven't done so already, add an `.env` file at the project root with connection credentials.

### Method 1: Use an existing database

#### Step 1: Connect to an SSH Tunnel
**IMPORTANT**: Your account on the bastion machine exists only to hold the public portion of your cryptographic key(s). See [Connecting to a Database (safely)](https://sentera.atlassian.net/wiki/spaces/GML/pages/3173416965/Connecting+to+a+Database+safely#The-General-Problem) for more information.

``` bash
ssh -o ServerAliveInterval=36000 -vvv -L 127.0.0.1:<DEMETER_PG_PORT>:<DATABASE_NAME>:<SSH_PORT><AWS_ANALYTICS_BASTION_USERNAME>@<SSH_HOST>
```

**For example**:

``` bash
ssh -o ServerAliveInterval=36000 -vvv -L 127.0.0.1:5433:demeter-database.cbqzrf0bsec9.us-east-1.rds.amazonaws.com:5432 myname@bastion-lt-lb-369902c3f6e57f00.elb.us-east-1.amazonaws.com
```

#### Step 2: Test your database connection
``` bash
psql --host localhost --port 5433 --user postgres postgres
```

### Method 2: Create your own database locally
#### Step 1: [Download Postgres](https://www.postgresql.org/download/)
#### Step 2: Initialize a Postgres database cluster on disk with `initdb`
``` bash
initdb -D /usr/local/pgsql/data
```

#### Step 3: Start the server process
``` bash
postgres -D /usr/local/pgsql/data
```

#### Step 4: Connect to your new database
``` bash
psql --host localhost --user my_username -f schema.sql postgres
```

## Demeter Background References
- [Demeter Data Types](https://sentera.atlassian.net/wiki/spaces/GML/pages/3172270107/Demeter+Data+Types+use+this)

## Example Usage

### Using Demeter data classes and functions

``` bash
python3 -m scripts.sample
```

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

## Troubleshooting References
- [Installing GeoPandas for M1](https://stackoverflow.com/questions/71137617/error-installing-geopandas-in-python-on-mac-m1)

## TODO
- Guide for setting up user account postgres using `postgres` account and `read_and_write` role and password `<user>`
- Grant `read_and_write` to `<user>`
