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
    # Lee los archivos y carga los datos en variables
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
            stop_times.append(dict(zip(header_times, row)))

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


def parada_mas_cercana_libre(lat_bus, lon_bus, stops, last_distance):
    # Encuentra la parada más cercana a las coordenadas dadas
    print("Calculando distancia libre:")

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
    last_distance = min_distancia

    return parada_cercana, min_distancia, last_distance


def parada_mas_cercana_seq(lat_bus, lon_bus, sequence, coords_secuencia,
                           valor_secuencia_inicial, last_distance, indice_parada_actual):
    # Calcula la distancia según la secuencia de la ruta
    print("Calculando distancia secuencial:")

    global reboot # Variabke para reiniciar en caso de que se llegue al final de la ruta

    distancia_secuencial = float("inf")
    valor_secuencia_inicial_int = int(valor_secuencia_inicial) # Convierte el valor de secuencia a entero
    
    if indice_parada_actual is None:
        indice_parada_actual = valor_secuencia_inicial_int 

    if indice_parada_actual < len(sequence): # Evita desbordar   

        lat_parada = coords_secuencia[indice_parada_actual*2]
        lon_parada = coords_secuencia[indice_parada_actual*2 + 1]
        distancia_secuencial = semiverseno(lat_bus, lon_bus, lat_parada, lon_parada)

        if last_distance < distancia_secuencial: 
        # Si la distancia minima actual es mayor que la anterior, ha pasado la parada y se esta alejando de ella
            indice_parada_actual+= 1 # Avanza a la siguiente parada

            # Vuelve a calcular la distancia, esta vez la correcta
            if indice_parada_actual < len(sequence):
                # Actualiza la nueva distancia mínima
                lat_parada = coords_secuencia[indice_parada_actual * 2]
                lon_parada = coords_secuencia[indice_parada_actual * 2 + 1]
                distancia_secuencial = semiverseno(lat_bus, lon_bus, lat_parada, lon_parada)
            else:
                print(f"Ha pasado la ultima parada.")

                lat_parada = coords_secuencia[(len(sequence)-1) * 2]
                lon_parada = coords_secuencia[(len(sequence)-1) * 2 + 1]
                distancia_secuencial = semiverseno(lat_bus, lon_bus, lat_parada, lon_parada)

                reboot = True # Va a llegar a la ultima parada, renicia

        last_distance = distancia_secuencial

    return distancia_secuencial, last_distance, indice_parada_actual


def calc_tiempo(distancia, velocidad):
    # Calcula el tiempo en segundos que tarda en recorrer una distancia a una velocidad dada
    if velocidad <= 0:
        return float('inf')  # Evita división por cero
    return distancia / velocidad


def write_influx(writer_api, bucket, org, tiempo_restante):
    point = influxdb_client.Point("mqtt_consumer").tag("device", "bus").field("tiempo_restante", tiempo_restante)
    writer_api.write(bucket=bucket, org=org, record=point)


def read_sequence(stop_times, primera_parada, segunda_parada, sequence):
    coords_secuencia = []
    sequence = []
    for i in range(len(stop_times)): # Recorre las paradas
        if stop_times[i]["stop_id"] == primera_parada: # Busca la id de la parada más cercana
            if stop_times[i+1]["stop_id"] == segunda_parada: 
                # Si la siguiente parada en la lista es la real, hemos encontrado la ruta
                valor_secuencia_inicial = stop_times[i]["stop_sequence"] # Guarda la secuencia de la primera parada (la segunda que haya pillado)
                trip_id = stop_times[i]["trip_id"]
                break # Ya tenemos el trip_id, no hace falta seguir

    # Encontrado el trip_id, ahora buscamos la secuencia de paradas
    for row in stop_times:
        if row["trip_id"] == trip_id:
            sequence.append(row["stop_id"]) # En teoría las paradas ya vienen ordenadas por secuencia
            # Guarda también las coordenadas de cada parada en la secuencia
            coords_secuencia.append(float(row["stop_lat"]))
            coords_secuencia.append(float(row["stop_lon"]))
    
    return sequence, valor_secuencia_inicial, coords_secuencia

def tiempo_espera(proxima_medida):
    if datetime.now(timezone.utc) < proxima_medida: # Comprueba la hora actual. Si es menor que la hora de la proxima medida, espera
            segundos_espera = (proxima_medida - datetime.now(timezone.utc)).total_seconds()
            print(f"Hora actual: {datetime.now(timezone.utc).strftime('%H:%M:%S')}")
            print(f"Esperando hasta: {proxima_medida.strftime('%H:%M:%S')} ({int(segundos_espera)} segundos)")

            time.sleep(segundos_espera)

def reiniciar_variables():
    # Reincia las variables para prepararlas para la siguiente ruta
    allow_read_sequence = False
    indice_parada_actual = None
    sequence = []
    coords_secuencia = []
    parada_cercana_old = None
    inicio_secuencia = None
    last_distance = float('inf')

    return allow_read_sequence, indice_parada_actual, sequence, coords_secuencia, parada_cercana_old, inicio_secuencia, last_distance

# Main #

# Configuración inicial

while True:
    try:
        [writer, reader, org, bucket] = config_influx()
        [stops, stop_times] = read_file()
        timestamp = None
        allow_read_sequence = False 
        parada_cercana_old = None
        inicio_secuencia = None
        last_distance = float('inf')
        indice_parada_actual = None
        last_lat, last_lon = 0, 0
        reboot = False

        break

    except Exception as e:
        print(f"Error al configurar InfluxDB o leer el archivo: {e}")
        time.sleep(1)
        continue

# Loop principal
while True:
    # Obtención de datos de InfluxDB
    data_influx = read_influx(reader, org)
    # Comprueba si hay datos nuevos viendo si ha cambiado el timestamp
    try:
        if data_influx[0].records[0].get_time() == timestamp or data_influx[0].records[0].get_value() is None:
            print("No hay nuevos datos en Influx. Esperando...")
            time.sleep(1) # Pausa para dejar que lleguen los datos
            continue
    except IndexError:
        print("No se han encontrado datos en Influx. Esperando...")
        time.sleep(30)
        continue

    # Avanza si hay datos nuevos:
    timestamp = data_influx[0].records[0].get_time() # Hora de llegada de los datos
    lat_bus = data_influx[0].records[0].get_value()
    lon_bus = data_influx[1].records[0].get_value()
    v_bus = data_influx[2].records[0].get_value()



    # Bloque de calculo de distancia a todas las paradas
    if allow_read_sequence is False: 
        try:
            [parada_cercana, dist_parada, last_distance] = parada_mas_cercana_libre(lat_bus, lon_bus, stops, 
                                                                                    last_distance)

            if parada_cercana != parada_cercana_old and parada_cercana_old is not None: # Si ha llegado a la segunda parada, lee la secuencia
                allow_read_sequence = True
                [sequence, inicio_secuencia, coords_secuencia] = read_sequence(stop_times, parada_cercana_old, 
                                                                               parada_cercana)

            parada_cercana_old = parada_cercana # Guarda la parada más cercana para la siguiente iteración            
            

            proxima_medida = timestamp + timedelta(minutes=3)
            tiempo_espera(proxima_medida)


        except Exception as e: 
            print(f"Error al calcular la distancia: {e}")
            #time.sleep(60)
            continue
    


    # Bloque de cálculo de distancia según la secuencia
    else:
        [distancia_secuencial, last_distance, indice_parada_actual] = parada_mas_cercana_seq(lat_bus, lon_bus, 
                                                                               sequence, coords_secuencia,
                                                                               inicio_secuencia, 
                                                                               last_distance,
                                                                               indice_parada_actual)
        
        tiempo_restante = calc_tiempo(distancia_secuencial, v_bus)
        print(f"Tiempo restante para llegar a la parada más cercana: {tiempo_restante:.2f} segundos")

        try:
            write_influx(writer, bucket, org, 10)
            print("Datos escritos en Influx")
        except Exception as e:
            print(f"Error al escribir en InfluxDB: {e}")

        cambio_brusco = semiverseno(lat_bus, lon_bus, last_lat, last_lon) > 50  # Cambia brusco si se mueve más de 50 metros
        if cambio_brusco:
            proxima_medida = timestamp + timedelta(minutes=1) # Espera al siguiente mensaje, al minuto, si hay cambio brusco
        else:
            proxima_medida = timestamp + timedelta(minutes=3) # Espera al siguiente mensaje, a los 3 minutos, si no hay cambio brusco

        last_lat = lat_bus
        last_lon = lon_bus


    if reboot:  # Si se ha alcanzado el final del trayecto, reinicia las variables
        print("Reiniciando variables...")

        [allow_read_sequence, indice_parada_actual, 
        sequence, coords_secuencia, parada_cercana_old, inicio_secuencia
        , last_distance] = reiniciar_variables()

        reboot = False

    tiempo_espera(proxima_medida)
