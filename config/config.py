# config/config.py

import os
from dotenv import load_dotenv

# Charger le fichier .env
load_dotenv()

class S3Config:
    BUCKET = os.getenv("S3_BUCKET")
    REGION = os.getenv("AWS_REGION")
    ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
    SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

class DBConfig:
    HOST = os.getenv("DB_HOST")
    PORT = os.getenv("DB_PORT")
    NAME = os.getenv("DB_NAME")
    USER = os.getenv("DB_USER")
    PASSWORD = os.getenv("DB_PASSWORD")

class AppConfig:
    ENV = os.getenv("ENV", "dev")

class OpenFoodFactsConfig:
    USER_AGENT = os.getenv("OPENFOODFACTS_USER_AGENT")