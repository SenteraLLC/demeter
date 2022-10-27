## Requirements

1. Python 3.10.4 or above
2. A Postgres database running the Demeter schema:
https://github.com/SenteraLLC/demeter


### Required Environment Variables
- DEMETER\_PG\_HOST
- DEMETER\_PG\_DATABASE
- DEMETER\_PG\_OPTIONS
- DEMETER\_PG\_USER

### Optional Environment Variables
- DEMETER\_PG\_PORT
- DEMETER\_PG\_PASSWORD

## Notes
This repository supports a root level '.env' file
[python-dotenv](https://github.com/theskumar/python-dotenv)


## Postgres Server
1) Start or locate a postgres instance where you have permissions to create tables
2) Run 'psql --host localhost --user my\_usernmae -f schema.sql postgres' (substituting any custom arguments)


## Temporary Documentation
https://sentera.atlassian.net/wiki/spaces/GML/pages/edit-v2/3107520532

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


## Notes
Installing GeoPandas for M1

https://stackoverflow.com/questions/71137617/error-installing-geopandas-in-python-on-mac-m1


## TODO
Guide for setting up SSH tunnel

Guide for setting up user account postgres using 'postgres' account and 'read\_and\_write' role and \password <user>
  grant read\_and\_write to <user>;

