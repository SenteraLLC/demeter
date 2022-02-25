To initialize the schema:
1) Start a postgres instance
2) Run 'psql --host localhost -f schema.sql postgres'


Installing GeoPandas for M1
https://stackoverflow.com/questions/71137617/error-installing-geopandas-in-python-on-mac-m1



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







