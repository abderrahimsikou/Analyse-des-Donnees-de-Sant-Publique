from sqlalchemy import create_engine, Table, MetaData, Column, String, Float, Select, func, cast, Numeric
from dotenv import load_dotenv
import urllib.parse
import os
import pandas as pd

# Read Dataset
data = pd.read_csv('Cleaned_data.csv')

# Connect with Database
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

password_encoded = urllib.parse.quote_plus(DB_PASSWORD)

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{password_encoded}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)

# Create Table
table_data = MetaData()

health_data_cleaned = Table(
    'health_data_cleaned', table_data,
    Column('smoking', String),
    Column('gender', String),
    Column('age', Float),
    Column('education', String),
    Column('weight', Float),
    Column('height', Float),
    Column('bmi', Float)
)

try:
    table_data.create_all(engine)
except Exception as error:
    print(error)

# Insert Data Into Table
with engine.begin() as conn:
    conn.execute(health_data_cleaned.insert(), data.to_dict(orient='records'))

# repartition <gender>
repartition_gender = Select(
    health_data_cleaned.c.gender,
    func.count().label('count')
).group_by(health_data_cleaned.c.gender)

with engine.begin() as con:
    result = con.execute(repartition_gender).fetchall()
    print(result)

# repartition <smoking> 
repartition_smoke = Select(
    health_data_cleaned.c.smoking,
    func.count().label('count')
).group_by(health_data_cleaned.c.smoking)

with engine.begin() as con:
    result = con.execute(repartition_smoke).fetchall()
    print(result)

# Repartition <bmi>
repartition_bmi = Select(
    health_data_cleaned.c.gender,
    func.round(cast(func.avg(health_data_cleaned.c.bmi), Numeric), 2).label('avg_bmi')
).group_by(health_data_cleaned.c.gender)

with engine.begin() as con:
    result = con.execute(repartition_bmi).fetchall()
    print(result)

# repartition <education> 
repartition_education = Select(
    health_data_cleaned.c.education,
    func.count().label('count')
).group_by(health_data_cleaned.c.education)

with engine.begin() as con:
    result = con.execute(repartition_education).fetchall()
    print(result)

# moyene <age> <smoking>
moyen_age_smoke = Select(
    health_data_cleaned.c.smoking,
    func.round(cast(func.avg(health_data_cleaned.c.age), Numeric), 1).label('avg_age')
).group_by(health_data_cleaned.c.smoking)

with engine.begin() as con:
    result = con.execute(moyen_age_smoke).fetchall()
    print(result)
