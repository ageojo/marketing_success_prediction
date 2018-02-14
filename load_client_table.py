import os, sys
import pandas as pd
import psycopg2 as pg
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import (Column, ForeignKey, Integer, String,
                        Float, Boolean)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.schema import MetaData
from sqlalchemy import Table

from settings import db_uri

def main(infile, db_uri):
    Base = declarative_base()

    class Client(Base):
        __tablename__ = 'client'
        id = Column(Integer, primary_key=True)
        age = Column(Float)
        job = Column(String(250))
        marital = Column(String(250))
        education = Column(String(250))
        default = Column(String)
        housing = Column(String)
        loan = Column(String)
        subscribed = Column(Boolean)

        def __repr__(self):
            return f'id={self.id}, age={self.age}"\
            job={self.job}, marital={self.marital}\
            education=(self.education),\
            default={self.default},\
            housing={self.housing}, loan={self.loan}'

    # create connection to database
    engine = create_engine(db_uri, echo=True)
    print("dropping table")
    Client.__table__.drop(engine)
    print("table dropped")

    # Use Declarative to create tables; this is when engine is first used!
    Base.metadata.create_all(engine)

    # Read in data to a pandas dataframe and create a dict
    client_df = pd.read_csv(infile)
    client_df = client_df.head()
    client_df_dict = client_df.to_dict(orient='records')

    # create connection to database
    conn = engine.connect()

    print("Connected. Starting session!")

    # open the session
    Session = sessionmaker(bind=engine)
    session = Session()

    print("Creating Metadata and Table objects")
    metadata = MetaData(bind=engine, reflect=True)  

    tablename = 'client'
    table = Table(tablename, metadata, autoload=True)

    print("starting to populate table")
    conn.execute(table.insert(), client_df_dict)
    print("done inserting records!")

    # commit the changes
    session.commit()

    # close session
    session.close()


if __name__ == "__main__":
    infile = "bank_client_data.csv"
    db_uri = db_uri
    sys.exit(main(infile, db_uri))