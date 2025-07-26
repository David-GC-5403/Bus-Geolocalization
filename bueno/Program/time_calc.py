import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
from haversine import haversine
import csv
from datetime import datetime, timedelta, timezone
import time

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
    with open("bueno/Program/paradas.csv", "r", newline='', encoding="utf-8") as file:
        file_reader = csv.reader(file)
        header = next(file_reader)
        stops = []
        for row in file_reader:
            stops.append(dict(zip(header, row)))

    with open("bueno/Program/stop_times.csv", "r", newline='', encoding="utf-8") as file:
        file_reader = csv.reader(file)
        header_times = next(file_reader)
        stop_times = []
        for row in file_reader:
            stops.append(dict(zip(header_times, row)))

    return stops, stop_times

def read_influx(reader_api, org):
    query = 'from(bucket: "Alumnos")\
  |> range(start: -15m)\
  |> filter(fn: (r) => r["_measurement"] == "mqtt_consumer")\
  |> filter(fn: (r) => r["_field"] == "uplink_message_decoded_payload_decoded_latitud" or r["_field"] == "uplink_message_decoded_payload_decoded_longitud" or r["_field"] == "uplink_message_decoded_payload_decoded_v")\
  |> last()'

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
            parada_cercana = row["stop_name"]

    print(f"Parada más cercana: {parada_cercana} a {min_distancia:.2f} metros")
    return parada_cercana, min_distancia

def calc_tiempo(distancia, velocidad):
    # Calcula el tiempo en segundos que tarda en recorrer una distancia a una velocidad dada
    if velocidad <= 0:
        return float('inf')  # Evita división por cero
    return distancia / velocidad

def write_influx(writer_api, bucket, org, tiempo_restante):
    point = influxdb_client.Point("mqtt_consumer").tag("device", "bus").field("tiempo_restante", tiempo_restante)
    writer_api.write(bucket=bucket, org=org, record=point)

def read_sequence(stop_times, primera_parada, segunda_parada, sequence):
    for row in stop_times:
        if row["stop_id"] == primera_parada: # Busca la id de la parada más cercana
            if row+1["stop_id"] == segunda_parada: 
                # Si la siguiente parada en la lista es la real, hemos encontrado la ruta
                valor_secuencia_inicial = row["stop_sequence"] # Guarda la secuencia de la primera parada (la segunda que haya pillado)
                trip_id = row["trip_id"]

    # Encontrado el trip_id, ahora buscamos la secuencia de paradas
    for row in stop_times:
        if row["trip_id"] == trip_id:
            sequence.append(row["stop_id"])   

    return sequence



# Main #

# Configuración inicial
while True:
    try:
        [writer, reader, org, bucket] = config_influx()
        [stops, stop_times] = read_file()
        timestamp = None
        allow_read_sequence = False
        sequence = []

        break

    except Exception as e:
        print(f"Error al configurar InfluxDB o leer el archivo: {e}")
        time.sleep(1)
        continue

# Loop principal
while True:
    data_influx = read_influx(reader, org)

    # Comprueba si hay datos nuevos viendo si ha cambiado el timestamp
    try:
        if data_influx[0].records[0].get_time() == timestamp or data_influx[0].records[0].get_value() is None:
            print("No hay nuevos datos en Influx. Esperando...")
            time.sleep(30) # Pausa para dejar que lleguen los datos
            continue
    except IndexError:
        print("No se han encontrado datos en Influx. Esperando...")
        time.sleep(30)
        continue

    # Si hay datos nuevos, procesa la información
    try:
        timestamp = data_influx[0].records[0].get_time() # Hora de llegada de los datos
        lat_bus = data_influx[0].records[0].get_value()
        lon_bus = data_influx[1].records[0].get_value()
        v_bus = data_influx[2].records[0].get_value()

        [parada_cercana, dist_parada, _] = parada_mas_cercana(lat_bus, lon_bus, stops)

        if allow_read_sequence is False:
            if parada_cercana != parada_cercana_old: # Si la parada más cercana ha cambiado, permite la lectura de la secuencia
                allow_read_sequence = True
                sequence.append(parada_cercana_old) 

        parada_cercana_old = parada_cercana


        tiempo_restante = calc_tiempo(dist_parada, v_bus)
        print(f"Tiempo restante para llegar a la parada más cercana: {tiempo_restante:.2f} segundos")

        write_influx(writer, bucket, org, 10)
        print("Datos escritos en Influx")

        last_lat = lat_bus
        last_lon = lon_bus

        cambio_brusco = haversine((lat_bus, lon_bus), (last_lat, last_lon), unit='m') > 50  # Cambia brusco si se mueve más de 50 metros
        if cambio_brusco:
            proxima_medida = timestamp + timedelta(minutes=2) # Espera al siguiente mensaje, a los 2 minutos, si hay cambio brusco
        else:
            proxima_medida = timestamp + timedelta(minutes=5) # Espera al siguiente mensaje, a los 5 minutos, si no hay cambio brusco

        while datetime.now(timezone.utc) < proxima_medida: # Comprueba la hora actual. Si es menor que la hora de la proxima medida, espera
            segundos_espera = (proxima_medida - datetime.now(timezone.utc)).total_seconds()
            print(f"Hora actual: {datetime.now(timezone.utc).strftime('%H:%M:%S')}")
            print(f"Esperando hasta: {proxima_medida.strftime('%H:%M:%S')} ({int(segundos_espera)} segundos)")

            time.sleep(segundos_espera)

    except Exception as e: 
        print(f"Error al procesar los datos: {e}")
        #time.sleep(60)
        continue

