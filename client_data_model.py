import psycopg2 as pg
from sqlalchemy import create_engine
from sqlalchemy import (Column, ForeignKey, Integer, String, Float, Boolean)
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()


class Client(Base):
    __tablename__ = 'client'
    id = Column(Integer, primary_key=True)å
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


# Create all Table objects bound to Declarative Base
# engine = create_engine('postgresql://ubuntu:metis@54.203.158.82:5432', echo=True)
# Base.metadata.create_all(engine)


# Drop Client table
# Client.__table__.drop(engine)

# ?? create Client table
# Client.create(engine)