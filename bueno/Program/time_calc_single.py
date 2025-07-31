import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
from haversine import haversine
import csv
from datetime import datetime, timedelta, timezone
import time

# Vamos a hacer la ruta primero de Algeciras a los barrios
id_ruta_ida = "5_395_90" # Trip id de la primera ruta (siempre es la misma)
id_ruta_vuelta = "5_410_90" # Trip id de la ruta de vuelta

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

def next_stop(lat_bus, lon_bus, stops_ida, stops_vuelta, coord_ida, 
              coord_vuelta, ida, last_distance, index_ida, index_vuelta):
    
    # Calcula la distancia a la siguiente parada según la secuencia
    print("Calculando distancia a la siguiente parada:")
    if ida == True:
        if index_ida <= len(stops_ida):
            lat_parada = coord_ida[index_ida*2]
            lon_parada = coord_ida[index_ida*2 + 1]
            distancia = semiverseno(lat_bus, lon_bus, lat_parada, lon_parada)u
            
            # Comprueba si ha pasado la parada
            if parada_ya_pasada(last_distance, distancia):
                index_ida += 1
                
                # Calcula la nueva distancia, hacia la siguiente parada
                if index_ida <= len(stops_ida):
                    lat_parada = coord_ida[index_ida*2]
                    lon_parada = coord_ida[index_ida*2 + 1]
                    distancia = semiverseno(lat_bus, lon_bus, lat_parada, lon_parada)

                elif index_ida > len(stops_ida): # Ida completa, toca volver
                    ida = False
                    index_ida = 1

    else:
        if index_vuelta <= len(stops_vuelta):
            lat_parada = coord_vuelta[index_vuelta*2]
            lon_parada = coord_vuelta[index_vuelta*2 + 1]
            distancia = semiverseno(lat_bus, lon_bus, lat_parada, lon_parada)

            if parada_ya_pasada(last_distance, distancia) and index_vuelta < len(stops_vuelta):
                index_vuelta += 1

                lat_parada = coord_vuelta[index_vuelta*2]
                lon_parada = coord_vuelta[index_vuelta*2 + 1]
                distancia = semiverseno(lat_bus, lon_bus, lat_parada, lon_parada)

            elif index_vuelta > len(stops_vuelta): # Vuelta completa, toca ir
                ida = True
                index_vuelta = 1

    
    last_distance = distancia

    return distancia, last_distance, index_ida, index_vuelta, ida
 

def parada_ya_pasada(last_distance, now_distance):
    # Comprueba si ha pasado la ultima parada
    if last_distance < now_distance:
        return True
    else:
        return False


def calc_tiempo(distancia, velocidad):
    # Calcula el tiempo en segundos que tarda en recorrer una distancia a una velocidad dada
    if velocidad <= 0:
        return float('inf')  # Evita división por cero
    return distancia / velocidad


def write_influx(writer_api, bucket, org, tiempo_restante):
    point = influxdb_client.Point("mqtt_consumer").tag("device", "bus").field("tiempo_restante", tiempo_restante)
    # Habria que guardar la info en un tag con la id del dispositivo que envio las coordenadas
    writer_api.write(bucket=bucket, org=org, record=point)


def lee_secuencia(ida, vuelta, seq_ida, seq_vuelta):
    # Stop_id de las rutas

    # Coordenadas de las rutas
    coords_ida = []
    coords_vuelta = []

    for i in range(len(stop_times)): # Recorre las paradas
        if stop_times[i]["trip_id"] == ida: # Busca la info de la ida
            seq_ida.append(stop_times[i]["stop_id"]) # Guarda la secuencia de paradas
            coords_ida.append(float(stop_times[i]["stop_lat"]))
            coords_ida.append(float(stop_times[i]["stop_lon"]))

        elif stop_times[i]["trip_id"] == vuelta: # Busca la info de la vuelta
            seq_vuelta.append(stop_times[i]["stop_id"]) # Guarda la secuencia de paradas
            coords_vuelta.append(float(stop_times[i]["stop_lat"]))
            coords_vuelta.append(float(stop_times[i]["stop_lon"]))

    return seq_ida, coords_ida, seq_vuelta, coords_vuelta


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
        reboot = False

        index_ida, index_vuelta = 1, 1 # Empieza en la segunda parada, ya que la primera es redundante
                                       # (La primera de la ida es la ultima de la vuelta, y viceversa)
        seq_ida, seq_vuelta = None, None 
        ida = True

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
    if seq_ida or seq_vuelta is None: # Si aun no se han establecido las rutas, las crea
        try:
            [seq_ida, coords_ida, seq_vuelta, coords_vuelta] = lee_secuencia(id_ruta_ida, id_ruta_vuelta, seq_ida, seq_vuelta)            

        except Exception as e: 
            print(f"Error al leer la secuencia: {e}")
            #time.sleep(60)
            continue
    
    else:
        # Calcula la distancia a la siguiente parada
        [distancia, last_distance, index_ida, index_vuelta, modo_ida] = next_stop(lat_bus, lon_bus, seq_ida, seq_vuelta, coords_ida,
                       coords_vuelta, modo_ida, last_distance, index_ida, index_vuelta)

        tiempo_restante = calc_tiempo(distancia, v_bus)
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

    if reboot:  # Si se ha alcanzado el final del trayecto, reinicia las variables
        print("Reiniciando variables...")

        [allow_read_sequence, indice_parada_actual, 
        sequence, coords_secuencia, parada_cercana_old, inicio_secuencia
        , last_distance] = reiniciar_variables()

        reboot = False

    tiempo_espera(proxima_medida)
