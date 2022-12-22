# PostGIS FAQs, Tips, and Tricks

## How/where do I install Postgresql on my local machine?
- [Download PostgreSQL](https://www.postgresql.org/download/) for your OS
- Go through the installation wizard, but be sure NOT to set the PROJ_LIB `ENV` variable (it should instead be set to point towards the PROJ database installed via your Python environment). See PyPROJ question below for more information.
- Set your username/password, and make note of your password - you'll need these credentials everytime you want to access the database.

### Note: test installation using `psql`
- `psql` should be available to use in the command line - check by running `psql -U $username`.
- If `psql` isn't recognized, check your user/system environment variables. On Windows, there should be a PATH variable pointing to the PostgreSQL `\bin` directory (e.g., `C:\Program Files\PostgreSQL\14\bin`).

## What version of Postgresql should I install?
- I (Tyler) haven't ever had any compatibility issues with versions (not yet anyways), so I've always tried to keep Postgres updated with the latest version. As of now (Oct 2021), I'm using PostgreSQL 14.
- Using the [Stack Builder app](https://www.enterprisedb.com/docs/supported-open-source/postgresql/installer/03_using_stackbuilder/) (included with the Postgresql installation), you can install different local database versions if you wish.

## How do I get the PostGIS capabilities? Do they work "out of the box"?
- No, they do not work "out of the box". You have to deliberately install the "PostGIS Spatial Extension Bundle for PostgreSQL".
- You may have the option to install during the initial installation (I can't remember), but if not or if you forget, the [Stack Builder app](https://www.enterprisedb.com/docs/supported-open-source/postgresql/installer/03_using_stackbuilder/) is the solution!
- It should be self explanatory, but in Stack Builder, check the PostGIS option under the *Spatial Extensions* Category, then go through with the installation.
- Remember, do NOT set the PROJ_LIB `ENV` variable to point towards that installed via PostGIS.

### Note
Only certian PostGIS versions are compatible with the various PostgreSQL versions. With PostgreSQL 14, it is PostGIS 3.1 (I have v3.1.4 installed).

## Okay, so now PostGIS is ready to roll?
- Not quite - the `postgis` extension has to be added to every database that we want to use PostGIS with.
- GUI method: Open [pgAdmin](https://www.pgadmin.org/) (included with the Postgresql installation), expand the database tree until *Extensions* is visible, right click and Create -> Extension... Select both `postgis` and `postgis_raster` and click Save.
- SQL: `CREATE EXTENSION postgis;` `CREATE EXTENSION postgis_raster;`

## How do I spawn up and hydrate a version of the Insight Sensing/ML Ops database (locally)?
- In pgAdmin, right click "Databases" -> Create -> Database. Name it to whatever you want (e.g., ml_ops).
- Remember to install `postgis` and `postgis_raster` extensions for this new database (see question above).
- Download [insight_dev-dev_client-v14-2021-09-02.sql](https://drive.google.com/open?id=1-hJpvDOBEncsmssSeKN8rQVjVTy2lKA2&authuser=tyler.nigon%40sentera.com&usp=drive_fs) (database dump) from Google Drive.
- `cd $download_dir`
- `psql -U $username -d $database --set ON_ERROR_STOP=on -f insight_dev-dev_client-v14-2021-09-02.sql` - this will load the "dev_client" schema and the corresponding data from .sql file.
- If you prefer, the schema can be renamed (e.g., pgAdmin -> Databases -> Schemas -> Properties).

### Note: `pg_dump` cannot be used to restore a dump from a plain SQL file
- `psql` must be used to restore the contents into the new database when `pg_dump format=plain` was used.

## How do I do a data dump from my local database?
- `pg_dump -U $username -p 5432 -d $database -n $schema --file=$filename.sql --format=plain`
- The `-n` tag can be dropped to dump all schema within the database
- This plain .sql dump file must be restored using psql (`pg_restore` will not work with `--format=plain`)

## PROJ/PyProj Conflict

### Problem: When I install PostGIS for the first time, there are conflicting PROJ databases (between `db`/PyPROJ and PostGIS).

### How to avoid the issue
During installation of PostGIS, do not set the PROJ_LIB ENV variable via the PostGIS installation wizard.

### Details
When I installed PostgreSQL/PostGIS on my machine for the first time, the PROJ_LIB environment variable was set to point towards that of the PostGIS installation (I believe it asked me if this is what I wanted, and I INCORRECTLY said yes) - in my case it was located at: C:\Program Files\PostgreSQL\14\share\contrib\postgis-3.1\proj. In fact, the PROJ_LIB env var should point towards that being installed by `db` (`pyproj` is installed with the `geopandas` dependency).

The problem with this is that it conflicts with the PROJ database of the `pyproj` Python package. When `pyproj` is imported, it accesses the PROJ database from PostGIS, which is likely different from the PROJ database expected by `pyproj` (i.e., it should be the `pyproj` installed via `geopandas`. See [this help issue](https://pyproj4.github.io/pyproj/stable/gotchas.html#internal-proj-error-sqlite-error-on-select) from the `pyproj` docs for a better explanation of this.

### Solution
The issue was solved for me be simply removing the PROJ_LIB environment variable from the User Variables and System Variables of my system (Windows). In the future, do not set the PROJ_LIB ENV variable during PostGIS installation.
