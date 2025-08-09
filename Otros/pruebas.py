import pandas as pd
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS

bucket = "Alumnos"
org = "TFG"
token = "TmYTccKyAKj-meNu-JpQH7-iACOI_5mgWCeasgeyjHsLxFG7azZNJvwARDpyJ8ZNSKD9rqAAxnqBRZXN3Mjl5w=="
url = "http://localhost:8086"

client = influxdb_client.InfluxDBClient(url=url, token=token, org=org) # Cliente de InfluxDB
writer = client.write_api(write_options=SYNCHRONOUS)  # API de escritura
reader = client.query_api()  # API de lectura
