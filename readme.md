## Requirements
1. Python 3.10.4 or above
2. Access to a Postgres database
3. The Postgres client installed locally


== Scripts ==

1) Generate Graphviz chart of Postgres database
Make sure that you have dev-dependencies installed via Poetry.
```pg_dump --schema-only --schema test_mlops --host localhost --dbname postgres | python3 -m scripts.to_graphviz | dot -Tpng > schema.png```


