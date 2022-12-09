# PostgreSQL Installation

## Connect to WSL's PostgreSQL via both WSL and Windows

### Step 1: Install PostgreSQL 14 (in WSL)

In WSL terminal:

```bash
sudo apt-get update
sudo apt-get -y install postgresql-14
sudo service postgresql status
sudo service postgresql start
```

### Step 2: Edit Postgres Config Files (in WSL)
Each of these changes ensures that the Postgres installation on WSL2 is accessibel to Windows.

**postgresql.conf**

```bash
sudo nano /etc/postgresql/14/main/postgresql.conf
```

Confirm that the following line is uncommented:

`postgresql.conf`
```bash
listen_addresses = '*'
```

**pg_hba.conf**

```bash
sudo nano /etc/postgresql/14/main/pg_hba.conf
```

Add the following to the bottom of the `pg_hba.conf` file ([as described here](https://www.cybertec-postgresql.com/en/postgresql-on-wsl2-for-windows-install-and-setup/)).

`pg_hba.conf`
```bash
host    all             all              0.0.0.0/0                       scram-sha-256
host    all             all              ::/0                            scram-sha-256
```

**Restart Postgres** for changes to take effect.
```bash
sudo service postgresql restart
```

### Step 3: Set `postgres` user's password (in WSL)

Login to `psql` with superuser access:
```bash
sudo -u postgres psql
```

Change password:
```bash
postgres=# ALTER USER postgres PASSWORD 'your_password';
ALTER ROLE
```

If successful, Postgres will output a confirmation of ALTER ROLE as seen above.

Exit the `psql` client using the `\q` command:

```bash
postgres=# \q
```

### Step 4: Get WSL hostname/IP address (in either WSL or Windows)

From WSL:
```bash
$ hostname -I
172.27.90.212
```

From Windows Powershell:
```powershell
PS C:\Users\Tyler> wsl -- hostname -I
172.27.90.212
```

### Step 5: Connect to WSL's Postgres

When prompted, enter the password you just created for the `postgres` user in Step 3.

From WSL:
```bash
psql -h 172.27.90.212 -U postgres -d postgres
```

From Windows Powershell:
```powershell
psql -h 172.27.90.212 -U postgres -d postgres
```

Connect via Windows PGAdmin:
- Servers -> Register -> Server
- In the General tab, enter a `Name` for your connection (I used `wsl`).
- In the Connection tab, use the following credentials
  - Host name/address: `<hostname/IP>` (`172.27.90.212` for this example)
  - Port: `5432`
  - Username: `postgres`
  - Password: `<your_password>`
- Save and use!
