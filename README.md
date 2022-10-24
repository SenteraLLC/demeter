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

## Usage Examples
Note: You should rename the directory of the git repo from "mlops-db" to "mlops_db" because python doesn't allow hyphens in package names. All of the scripts and tools are run as packages from outside the directory containing the repo.

### Example 1 - Initialize the schema

The base schema is required to do anything with Demeter. To initiliaze it yourself, start a postgres instance, then run the following:

``` bash
psql --host localhost -f schema.sql postgres
```

### Example 2 - Make HTTP Types

``` bash
python3 -m mlops_db.scripts.make_http
```

### Example 3 - Load MLOps data
Loads some geoml data for predicting potato N from a set of sample data. You may need to contact Joel for this data.

``` bash
python3 -m mlops_db.load_mlops --host localhost --data_path ~/projects/geoml_db/new_db/sample_data --field_id_path /tmp/field_stuff.json
```
Note: The 'field_id_path' argument points to a generated file, it store some intermediate data that is used between sample data loads.

### Example 4 - Insert Keys
Inserts a couple of keys into the database which point at data loaded in Example #3 'Load MLOps Data'

### Example 5 - Server
A simple HTTP Server for testing the http types created in the #1 'Make HTTP Script'

### Example 6 - An Example MLOps Function
Currently, it only loads some data from the MLOps Database and hits the test server for some sample HTTP results

``` bash
python3 -m mlops_db.scripts.example
```

## Other installation tips
Installing GeoPandas for M1
https://stackoverflow.com/questions/71137617/error-installing-geopandas-in-python-on-mac-m1

## Process flow

1) User locates or creates a key set in the MLOps Database

  Key sets are composed of two parts:
    Geospatial Keys
      Reference geometries (and possibly fields)
    Temporal Keys
      Start and End dates


2) User targets an MLOps function and supplies their Key Set ids
     E.G. MyFunction 1.X
          Arguments: Geospatial Keys {1, 3, 6}
                     Temporal Keys {4}

   The @Function decorator captures the arguments and stores them internally.
   The @Function decorator also uses environment variables to establish connections with:
     The MLOPs Database
     S3
   The @Function decorator passes any arguments unrelated to the keys as function arguments

3) The function executes
   Any datasource functions internally uses the keys, so they are transparent to the user

4)  The function returns an artifact (most likely a geopandas dataframe) to be stored in S3

5) The @Function decorator records the S3 output and the keys used to generate it
