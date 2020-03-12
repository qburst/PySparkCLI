# DB Integration project

This example is to showcase the NoSQL and SQL database integration. Please use the following details to setup corresponsing database. In the project we will adding data to a database name mydb, so after installing the database please create the corresponding db


# Commands to install MongoDB

#### Step 1: Import the public key used by the package management system.
```bash.
wget -qO - https://www.mongodb.org/static/pgp/server-4.2.asc | sudo apt-key add -
```

#### Step 2: Create a list file for MongoDB
```bash
echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu bionic/mongodb-org/4.2 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-4.2.list
```
#### Step 3: Reload local package database
```bash
sudo apt-get update
```
#### Step 4: Install the MongoDB packages
```bash
sudo apt-get install -y mongodb-org=4.2.3 mongodb-org-server=4.2.3 mongodb-org-shell=4.2.3 mongodb-org-mongos=4.2.3 mongodb-org-tools=4.2.3
```
#### Step 5: Start MongoDB
```bash
sudo systemctl start mongod
```
#### Step 6: Open Mongo Shell
```bash
mongo
```
#### Step 7: Install mongoengine using pip
```bash
pip install mongoengine
```
### Configuring SQLAlchemy for connecting Postges with PySpark
#### Step 1: Install SQLAlchemy and psycopg2 using pip
```bash
pip install SQLAlchemy
pip install psycopg2
```
#### Step 2: Configure variable 'engine' as shown below in sql_db.py
```python
engine = create_engine('postgresql://Username:Password@dbHost/dbName')
```
### Reference:
https://docs.mongodb.com/manual/tutorial/install-mongodb-on-ubuntu/
