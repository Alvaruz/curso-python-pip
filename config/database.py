import os
from sqlalchemy import create_engine
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from dotenv import load_dotenv

asuncion = os.environ.get('asuncion')
user = os.environ.get('user')
passw = os.environ.get('passw')
host = os.environ.get('host')
port = os.environ.get('port')
db = os.environ.get('db')

# Fastapi Code
sqlite_file_name = "../database.sqlite"
base_dir = os.path.dirname(os.path.realpath(__file__))

db_connection_url = f"postgresql://{user}:{passw}@{host}:{port}/{db}"

sql_engine  = create_engine(db_connection_url, echo=True)

Session = sessionmaker(bind=sql_engine)

Base = declarative_base()

if __name__ == '__main__':
    print(asuncion)

