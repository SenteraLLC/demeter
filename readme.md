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

