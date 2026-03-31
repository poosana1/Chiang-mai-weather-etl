import requests
import psycopg2
from dotenv import load_dotenv
import os

# Create Database Connection
load_dotenv()

conn = psycopg2.connect(
    host="localhost",
    port=os.getenv("POSTGRES_PORT"),
    database=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD")
)

# Extract
response = requests.get("https://api.open-meteo.com/v1/forecast?latitude=18.7883&longitude=98.9853&hourly=temperature_2m")
data = response.json()
hourly_time = data["hourly"]["time"]
hourly_temperature = data["hourly"]["temperature_2m"]

# Transform
listData = []
for time, temperature in zip(hourly_time, hourly_temperature):
    listData.append({
        "time": time,
        "temperature": temperature
    })

print(listData)


# Create Table
cursor = conn.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS weather_data (
    id SERIAL PRIMARY KEY,
    time TIMESTAMP UNIQUE,
    temperature FLOAT
)
""")
conn.commit()

# Load
for item in listData:
    cursor.execute("""
    INSERT INTO weather_data (time, temperature) VALUES (%s, %s)
    ON CONFLICT (time) DO NOTHING
    """, (item["time"], item["temperature"]))

conn.commit()

cursor.close()
conn.close()