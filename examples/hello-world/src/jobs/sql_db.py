from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# The base class which our objects will be defined on.
Base = declarative_base()


class EtlData(Base):
    __tablename__ = 'EtlData'

    # Every SQLAlchemy table should have a primary key named 'id'
    id = Column(Integer, primary_key=True)

    name = Column(String)
    steps_to_desk = Column(Integer)

    # Lets us print out object conveniently.
    def __repr__(self):
        return "<EtlData(name='%s', steps_to_desk='%s')>" % (
            self.name, str(self.steps_to_desk))


def getSession():
    engine = create_engine('postgresql://postgres:Welcome1@localhost/mydb')

    # Create all tables by issuing CREATE TABLE commands to the DB.
    Base.metadata.create_all(engine)

    # Creates a new session to the database by using the engine we described.
    Session = sessionmaker(bind=engine)
    session = Session()
    return session

