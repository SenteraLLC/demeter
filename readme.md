# Demeter API

The `demeter_api` is the library that the modeler uses to interact with the Demeter DB. The Demeter DB uses a robust typing system via `demeter_api` that enables the data scientist working on a project to define and add any new Type of data to the Demeter DB.

The Demeter DB is the Postgres + PostGIS database responsible for tracking and accessing all agronomic data for ag-modeling. Demeter DB has a particular focus on ensuring all data is spatially and temporally aware. Likewise, spatial and temporal queries are the basis for constructing all training and prediction feature matrices.

See [Demeter Definitions](https://sentera.atlassian.net/wiki/spaces/AI/pages/3020652567/Demeter+Definitions) for background on the terms we use in the Demeter/ML Ops project.


To initialize the schema:
1) Start a postgres instance
2) Run 'psql --host localhost -f schema.sql postgres'


Installing GeoPandas for M1
https://stackoverflow.com/questions/71137617/error-installing-geopandas-in-python-on-mac-m1

== Scripts ==

Note: You should rename the directory of the git repo from "mlops-db" to "mlops_db" because python doesn't allow hyphens in package names. All of the scripts and tools are run as packages from outside the directory containing the repo.

1) Make HTTP
  Makes some http types in the database for testing
  Example: `python3 -m scripts.make_http`

2) Load MLOps data
  Loads some geoml data for predicting potato N from a set of sample data
  You may need to contact Joel for this data.
  Example:
  python3 -m mlops_db.load_mlops --host localhost --data_path ~/projects/geoml_db/new_db/sample_data --field_id_path /tmp/field_stuff.json
  Note: The 'field_id_path' argument points to a generated file, it store some intermediate data that is used between sample data loads.

3) Insert Keys
  Inserts a couple of keys into the database which point at data loaded in step #2 'Load MLOps Data'

4) Server
   A simple HTTP Server for testing the http types created in the #1 'Make HTTP Script'

5) Example
   A simple example of an mlops function.
   Currently, it only loads some data from the MLOps Database and hits the test server for some sample HTTP results
   Example: python3 -m mlops_db.scripts.example






Process flow:

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







