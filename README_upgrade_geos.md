# Upgrade GEOS

You will need to upgrade GEOS because some PostGIS functions (e.g., [ST_ReducePrecision](https://postgis.net/docs/ST_ReducePrecision.html)) requires GEOS v3.9+.

Note: Please follow [this helpful cybertec guide](https://www.cybertec-postgresql.com/en/postgis-upgrade-geos-with-ubuntu-in-3-steps/) during the upgrade. The comments and code below are simply documentation to show what versions I'm using.

## Step 1 ([of the cybertec guide](https://www.cybertec-postgresql.com/en/postgis-upgrade-geos-with-ubuntu-in-3-steps/))

Use this query to check your version of GEOS (before the upgrade, I have a flavor of version `3.8.0` installed), and make note of your version of PostGIS:
```sql
postgres=# select postgis_full_version();
            postgis_full_version

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 POSTGIS="3.3.2 4975da8" [EXTENSION] PGSQL="140" GEOS="3.8.0-CAPI-1.13.1 " PROJ="6.3.1" GDAL="GDAL 3.0.4, released 2020/01/28" LIBXML="2.9.10" LIBJSON="0.13.1" LIBPROTOBUF="1.3.3" WAGYU="0.5.0 (Internal)" RASTER
(1 row)
```

## Step 2 ([of the cybertec guide](https://www.cybertec-postgresql.com/en/postgis-upgrade-geos-with-ubuntu-in-3-steps/))

### GEOS
I downloaded GEOS version `3.11.1` using this build process:

```bash
wget https://download.osgeo.org/geos/geos-3.11.1.tar.bz2
tar xvfj geos-3.11.1.tar.bz2 && cd geos-3.11.1
mkdir build && cd build

# Set up the build
cmake \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX=/usr/local \
  ..
make
ctest
sudo make install
```

### PostGIS
I downloaded PostGIS version `3.3.2` using this configuration process:

```bash
wget https://download.osgeo.org/postgis/source/postgis-3.3.2.tar.gz

tar xvzf postgis-3.3.2.tar.gz && cd postgis-3.3.2
./configure
```

## Step 3 ([of the cybertec guide](https://www.cybertec-postgresql.com/en/postgis-upgrade-geos-with-ubuntu-in-3-steps/))

This is the summary of the build from step 2 of the cybertec guide:

```bash
 -------------- Dependencies --------------
  GEOS config:          /usr/local/bin/geos-config
  GEOS version:         3.11.1
  GDAL config:          /usr/bin/gdal-config
  GDAL version:         3.0.4
  PostgreSQL config:    /usr/bin/pg_config
  PostgreSQL version:   PostgreSQL 14.6 (Ubuntu 14.6-1.pgdg20.04+1)
  PROJ4 version:        63
  Libxml2 config:       /usr/bin/xml2-config
  Libxml2 version:      2.9.10
  JSON-C support:       yes
  protobuf support:     yes
  protobuf-c version:   1003003
  PCRE support:         Version 1
  Perl:                 /usr/bin/perl

 --------------- Extensions ---------------
  PostGIS Raster:                     enabled
  PostGIS Topology:                   enabled
  SFCGAL support:                     disabled
  Address Standardizer support:       enabled
  ```

And this is what was installed:

```sql
postgres=# select postgis_full_version();
            postgis_full_version

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 POSTGIS="3.3.2 4975da8" [EXTENSION] PGSQL="140" GEOS="3.11.1-CAPI-1.17.1" PROJ="6.3.1" GDAL="GDAL 3.0.4, released 2020/01/28" LIBXML="2.9.10" LIBJSON="0.13.1" LIBPROTOBUF="1.3.3" WAGYU="0.5.0 (Internal)" RASTER
(1 row)
```
