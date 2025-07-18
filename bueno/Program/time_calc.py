import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
from haversine import haversine
import csv
import time

def config_influx():
    bucket = "Alumnos"
    org = "TFG"
    token = "TmYTccKyAKj-meNu-JpQH7-iACOI_5mgWCeasgeyjHsLxFG7azZNJvwARDpyJ8ZNSKD9rqAAxnqBRZXN3Mjl5w=="
    url = "http://localhost:8086"

    client = influxdb_client.InfluxDBClient(url=url, token=token, org=org) # Cliente de InfluxDB
    writer = client.write_api()  # API de escritura
    reader = client.query_api()  # API de lectura

    return writer, reader, org, bucket

def read_file():
    with open("bueno/Program/paradas.csv", "r", newline='', encoding="utf-8") as file:
        file_reader = csv.reader(file)
        header = next(file_reader)
        stops = []
        for row in file_reader:
            stops.append(dict(zip(header, row)))
        return stops

def read_influx(reader_api, org):
    query = 'from(bucket: "Alumnos")\
  |> range(start: -5m)\
  |> filter(fn: (r) => r["_measurement"] == "mqtt_consumer")\
  |> filter(fn: (r) => r["_field"] == "uplink_message_decoded_payload_decoded_latitud" or r["_field"] == "uplink_message_decoded_payload_decoded_longitud" or r["_field"] == "uplink_message_decoded_payload_decoded_v")'

    result = reader_api.query(org=org, query=query)
    return result

def semiverseno(lat1, lon1, lat2, lon2):
    return haversine((lat1, lon1), (lat2, lon2), unit='m')

def parada_mas_cercana(lat_bus, lon_bus, stops):
    # Encuentra la parada más cercana a las coordenadas dadas
    min_distancia = float('inf')
    parada_cercana = None

    for row in stops:
        lat_parada = float(row['stop_lat'])
        lon_parada = float(row['stop_lon'])
        distancia = semiverseno(lat_bus, lon_bus, lat_parada, lon_parada)

        if distancia < min_distancia:
            min_distancia = distancia
            parada_cercana = row
    print(f"Parada más cercana: {parada_cercana['stop_name']} a {min_distancia:.2f} metros")
    return parada_cercana, min_distancia

def calc_tiempo(distancia, velocidad):
    # Calcula el tiempo en segundos que tarda en recorrer una distancia a una velocidad dada
    if velocidad <= 0:
        return float('inf')  # Evita división por cero
    return distancia / velocidad

def write_influx(writer_api, bucket, org, tiempo_restante):
    point = influxdb_client.Point("mqtt_consumer").tag("device", "bus").field("tiempo_restante", tiempo_restante)
    writer_api.write(bucket=bucket, org=org, record=point)


# Main #

# Configuración inicial
[writer, reader, org, bucket] = config_influx()
stops = read_file()
last_data = None  # Variable para almacenar los últimos datos leídos

while True:
    data_influx = read_influx(reader, org)
    if data_influx == last_data:
        print("No hay nuevos datos en Influx. Esperando...")
        time.sleep(1*60)
        continue

    lat_bus = data_influx[0].records[0].get_value()
    lon_bus = data_influx[1].records[0].get_value()
    v_bus = data_influx[2].records[0].get_value()

    [_, dist_parada] = parada_mas_cercana(lat_bus, lon_bus, stops)

    tiempo_restante = calc_tiempo(dist_parada, v_bus)
    print(f"Tiempo restante para llegar a la parada más cercana: {tiempo_restante:.2f} segundos")

    write_influx(writer, bucket, org, tiempo_restante)
    time.sleep(1)
    print("Datos escritos en Influx")

    last_data = data_influx

    time.sleep(5*60)