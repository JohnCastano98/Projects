import base64
import os

from sqlalchemy import MetaData
from sqlalchemy.engine import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session

DIALECT = 'oracle'
SQL_DRIVER = 'cx_oracle'
USERNAME = base64.b64decode(os.getenv('DB_USERNAME').encode()).decode()
PASSWORD = base64.b64decode(os.getenv('DB_PASSWORD').encode()).decode()
HOST = os.getenv('DB_HOST')
PORT = int(os.getenv('DB_PORT'))
SERVICE = os.getenv('DB_SERVICE')
ENGINE_PATH_WIN_AUTH = f"{DIALECT}+{SQL_DRIVER}://{USERNAME}:{PASSWORD}@{HOST}:{str(PORT)}/?service_name={SERVICE}"

engine = create_engine(ENGINE_PATH_WIN_AUTH)
session = Session(engine)
Base = declarative_base()
meta = MetaData()
