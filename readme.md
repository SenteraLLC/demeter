## Requirements
1. Python 3.10.4 or above
2. Access to a Postgres database
3. The Postgres client installed locally


== Scripts ==

1) Generate Graphviz chart of Postgres database
Make sure that you have dev-dependencies installed via Poetry.
```pg_dump --schema-only --schema test_mlops --host localhost --dbname postgres | python3 -m scripts.to_graphviz | dot -Tpng > schema.png```
Note: You may need to provide additional credentials or configuration parameters to the 'pg_dump' command. For example, you may need to specify a different 'user' or 'dbname'. You may also be prompted for a password. Entering your password should not affect execution of the piped commands.
