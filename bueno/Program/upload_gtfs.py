import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
import os
import csv

def config_influx():
    bucket = "Alumnos"
    org = "TFG"
    token = "TmYTccKyAKj-meNu-JpQH7-iACOI_5mgWCeasgeyjHsLxFG7azZNJvwARDpyJ8ZNSKD9rqAAxnqBRZXN3Mjl5w=="
    url = "http://localhost:8086"

    client = influxdb_client.InfluxDBClient(url=url, token=token, org=org) # Cliente de InfluxDB
    writer = client.write_api(write_options=SYNCHRONOUS)  # API de escritura
    reader = client.query_api()  # API de lectura

    return writer, reader, org, bucket

def read_file():
    with open("stops.txt", "r") as file:
        file_reader = csv.DictReader(file)
        stops = [row for row in file_reader]
    return stops

#def read_influx():

#def semiverseno():

#def calc_tiempo():

#def write_influx():



# Main #

config_influx()
read_file()

    # Espera a recibir las nuevas coordenadas + velocidad
    # Usando coordenadas, calcula con el semiverseno la parada más cercana -> Lee stops.txt
    # En base a esa velocidad, calcula el tiempo que le queda para llegar a la parada más cercana

    # Sube el tiempo a influx
