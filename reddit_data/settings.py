import os
from os import getenv
from dotenv import load_dotenv

BASEDIR = os.path.abspath(os.path.dirname(__file__))
load_dotenv(os.path.join(BASEDIR, '.env'),override=True)

LOGGING_LEVEL = getenv('LOGGING_LEVEL','ERROR').upper()
MY_CLIENT_ID = getenv('MY_CLIENT_ID',None)
MY_USER_AGENT=getenv('MY_USER_AGENT',None)
MY_REDDIT_USERNAME= getenv('MY_REDDIT_USERNAME',None)
MY_REDDIT_PASSWORD= getenv('MY_REDDIT_PASSWORD',None)
MY_CLIENT_SECRET = getenv('MY_CLIENT_SECRET',None)

DB_SETTINGS = {
    'mongo_uri': getenv('mongo_uri','mongodb://localhost:27017'),
    'mongo_db': getenv('mongo_db','feeds')
}