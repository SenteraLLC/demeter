# demeter

The database schema and API that supports agronomic modeling and data science activities.

> See [README_db_setup](https://github.com/SenteraLLC/demeter/blob/main/README_db_setup.md) and [README_wsl](https://github.com/SenteraLLC/demeter/blob/main/README_wsl.md) for further information about how to set up PostgreSQL and PostGIS (a prerequisite to use `demeter`).

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

### Release/Tags
- A GitHub release is created on every push to the main branch using the `create_github_release.yml` Github Action Workflow
- Releases can be created manually through the GitHub Actions UI as well.
- The name of the Release/Tag will match the value of the version field specified in `pyproject.toml`
- Release Notes will be generated automatically and linked to the Release/Tag

## Setup and Installation (used as a library)
If using `demeter` as a dependency in your script, simply add it to the `pyproject.toml` in your project repo. Be sure to uuse the `ssh:` prefix so Travis has access to the repo for the library build process.

<h5 a><strong><code>pyproject.toml</code></strong></h5>

``` toml
[tool.poetry.dependencies]
demeter = { git = "ssh://git@github.com/SenteraLLC/demeter.git", branch = "main"}
```

Install `demeter` and all its dependencies via `poetry install`.

``` bash
poetry install
```

## Requirements
- Python `3.10.4`+
- Access to a Postgres database (with connection credentials)
- A local installation of PostgreSQL (i.e., `psql`)
- OSX Users may need to manually install the 'gdal' system requirement (e.g. brew install gdal)

## Demeter Data Types
See these Confluence pages for some background on data types and tables that the `demeter` database uses:
- [Demeter Data Types](https://sentera.atlassian.net/wiki/spaces/GML/pages/3172270107/Demeter+Data+Types)
- [Demeter Schema (for v1.2.0)](https://sentera.atlassian.net/wiki/spaces/GML/pages/3198156837/Proposed+Demeter+Schema+v1.2.0+ABI)

## Tests

Before running tests, you must initialize the schema

``` bash
psql --host localhost --user postgres -f schema.sql postgres
```

**scripts/sample.py**
```bash
$ poetry run python scripts/sample.py

Root group id: 75910
Argentina group id: 75911
Field Geom id: 105669
Field id: 250158
Irrigation type id: 90
Gallons type id: 118
Observation geom id:  105669
Local value id: 228730
```

## Troubleshooting
### Installing `geopandas` on Mac
[See this SO thread](https://stackoverflow.com/questions/71137617/error-installing-geopandas-in-python-on-mac-m1)

### Installing PostgreSQL on WSL2
[See README_wsl](https://github.com/SenteraLLC/demeter/blob/main/README_wsl.md)

## Diagram Generation
Graphviz is required for automatic diagram generation. Ensure `graphviz` is installed (note that it's present in the `dev-dependencies`).

A `schema-test_demeter.png` can be automatically generated with the following:

```bash
pg_dump --schema-only --schema test_demeter -h localhost -U postgres -d postgres | poetry run python -m scripts.to_graphviz | dot -Tpng > schema-test_demeter.png
```

## TODO
- Guide for setting up user account postgres using `postgres` account and `read_and_write` role and password `<user>`
- Grant `read_and_write` to `<user>`

#### Example Schema
![Example Schema](./schema-test_demeter.png)
