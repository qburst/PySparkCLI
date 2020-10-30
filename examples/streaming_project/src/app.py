import time
import sys
from json import loads
from os import path
from pyspark.storagelevel import StorageLevel
from mongoengine import connect, Document, StringField

from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# The base class which our objects will be defined on.
Base = declarative_base()

class StreamData(Base):
    __tablename__ = 'StreamData'

    # Every SQLAlchemy table should have a primary key named 'id'
    id = Column(Integer, primary_key=True)

    user = Column(String)
    location = Column(String)

    # Lets us print out object conveniently.
    def __repr__(self):
        return "<Data(user='%s', location='%s')>" % (
            self.name, self.location)


def getSession():
    engine = create_engine('postgresql://postgres:Welcome1@localhost/mydb')

    # Create all tables by issuing CREATE TABLE commands to the DB.
    Base.metadata.create_all(engine)

    # Creates a new session to the database by using the engine we described.
    Session = sessionmaker(bind=engine)
    session = Session()
    return session



def saveToDB(data):
    data = loads(data)
    user = data.get('user', {}).get('name', '--NA--')
    location = data.get('user', {}).get('location', '--NA--')

    etl = StreamData(user=user, location=location)
    session.add(etl)
    session.commit()
    return True

if __name__ == "__main__":
    sys.path.append(path.join(path.dirname(__file__), '..'))
    from configs import spark_config

    ssc = spark_config.ssc
    lines = ssc.socketTextStream(spark_config.IP, spark_config.Port)

    # When your DStream in Spark receives data, it creates an RDD every batch interval.
    # We use coalesce(1) to be sure that the final filtered RDD has only one partition,
    # so that we have only one resulting part-00000 file in the directory.
    # The method saveAsTextFiles() should really be re-named saveInDirectory(),
    # because that is the name of the directory in which the final part-00000 file is saved.
    # We use time.time() to make sure there is always a newly created directory, otherwise
    # it will throw an Exception.

    # lines.persist(StorageLevel.MEMORY_AND_DISK)
    #
    # data = lines.map(lambda x: loads(x)).map(lambda result: {"user": result.get('user', {}).get('name', '--NA--'), "location": result.get('user', {}).get('location', '--NA--'), "text": result.get("text", "--NA--")})
    #
    # data.saveAsTextFiles("./tweets/%f" % time.time())
    # data.pprint()
    # data = lines.map(lambda x: loads(x)).map(lambda result: saveToDB(result))
    session = getSession()
    lines.foreachRDD(lambda rdd: rdd.filter(saveToDB).coalesce(1).saveAsTextFile("./tweets/%f" % time.time()))
    session.close()
    # You must start the Spark StreamingContext, and await process termination…
    ssc.start()
    ssc.awaitTermination()