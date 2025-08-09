import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
from haversine import haversine
from datetime import datetime, timedelta, timezone
import time
import pandas as pd

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
    # Lee el archivo desde el github y lo carga con pandas

    gtfs_data = pd.read_csv("https://raw.githubusercontent.com/David-GC-5403/Bus-Geolocalization/refs/heads/Pruebas/bueno/Program/stops_info.csv")

    return gtfs_data

def read_influx(client):
    # Query para obtener los datos de influx
    query = '''
        from(bucket: "Alumnos")
        |> range(start: -15m)
        |> filter(fn: (r) => r["_measurement"] == "mqtt_consumer")
        |> filter(fn: (r) => 
            r["_field"] == "uplink_message_decoded_payload_decoded_latitud" or
            r["_field"] == "uplink_message_decoded_payload_decoded_longitud" or
            r["_field"] == "uplink_message_decoded_payload_decoded_v")
        |> last()
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        |> rename(columns: {
            uplink_message_decoded_payload_decoded_latitud: "latitud",
            uplink_message_decoded_payload_decoded_longitud: "longitud",
            uplink_message_decoded_payload_decoded_v: "velocidad"
        })        })

        |> keep(columns: ["_time","latitud","longitud","velocidad"])
    '''

    result = client.query.api().query_data_frame() # Datos en dataframe

    return result

def semiverseno(lat1, lon1, lat2, lon2):
    # Calcula el semiverseno con las coordenadas dadas en metros
    return haversine((lat1, lon1), (lat2, lon2), unit='m')


def next_stop(lat_bus, lon_bus, stops_ida, stops_vuelta, coord_ida, 
              coord_vuelta, ida, last_distance, index_ida, index_vuelta):
    # Calcula la distancia a la siguiente parada según la secuencia
    fin_calculo = False

    print("Calculando distancia a la siguiente parada:")
    while not fin_calculo: # Nos aseguramos que en el cambio de direccion se calcule la distancia a la nueva parada
        fin_calculo = True

        if ida == True:
            if index_ida < len(stops_ida):
                [lat_parada, lon_parada] = coord_ida[index_ida]
                distancia = semiverseno(lat_bus, lon_bus, lat_parada, lon_parada)
                
                # Comprueba si ha pasado la parada
                if parada_ya_pasada(last_distance, distancia):
                    index_ida += 1
                    
                    # Calcula la nueva distancia, hacia la siguiente parada
                    if index_ida < len(stops_ida):
                        [lat_parada, lon_parada] = coord_ida[index_ida]
                        distancia = semiverseno(lat_bus, lon_bus, lat_parada, lon_parada)

                    elif index_ida >= len(stops_ida): # Ida completa, toca volver
                        ida = False
                        index_ida = 1
                        fin_calculo = False
            

        else:
            if index_vuelta < len(stops_vuelta):
                [lat_parada, lon_parada] = coord_vuelta[index_vuelta]
                distancia = semiverseno(lat_bus, lon_bus, lat_parada, lon_parada)

                if parada_ya_pasada(last_distance, distancia) and index_vuelta < len(stops_vuelta):
                    index_vuelta += 1

                    if index_ida < len(stops_ida):
                        [lat_parada, lon_parada] = coord_vuelta[index_vuelta]
                        distancia = semiverseno(lat_bus, lon_bus, lat_parada, lon_parada)

                    elif index_vuelta >= len(stops_vuelta): # Retorno completo, toca ir
                        ida = True
                        index_vuelta = 1
                        fin_calculo = False

        
    last_distance = distancia

    return distancia, last_distance, index_ida, index_vuelta, ida
 

def parada_ya_pasada(last_distance, now_distance):
    # Comprueba si ha pasado la parada anterior
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
    # Escribe el tiempo en influx

    point = influxdb_client.Point("mqtt_consumer").tag("device", "bus").field("tiempo_restante", tiempo_restante)
    # Habria que guardar la info en un tag con la id del dispositivo que envio las coordenadas
    writer_api.write(bucket=bucket, org=org, record=point)


def lee_secuencia(df_gtfs, trip_id_ida, trip_id_vuelta):
    # Obtiene la secuencia de las rutas
    
    coords_ida = []
    coords_vuelta = []

    data_ida = df_gtfs[df_gtfs["trip_id"] == trip_id_ida] # Info de la ida
    data_vuelta = df_gtfs[df_gtfs["trip_id"] == trip_id_vuelta] # Info de la vuelta

    seq_ida = data_ida["stop_id"].tolist() # Secuencia de paradas de la ida
    seq_vuelta = data_vuelta["stop_id"].tolist() # Secuencia de paradas de la vuelta

    # Guarda las coordenadas de las paradas en tuplas
    coords_ida = list(zip(data_ida["stop_lat"], data_ida["stop_lon"]))
    coords_vuelta = list(zip(data_vuelta["stop_lat"], data_vuelta["stop_lon"]))

    return seq_ida, coords_ida, seq_vuelta, coords_vuelta


def tiempo_espera(proxima_medida):
    # Establece el tiempo de espera para el siguiente mensaje
    if datetime.now(timezone.utc) < proxima_medida: # Comprueba la hora actual. Si es menor que la hora de la proxima medida, espera
            segundos_espera = (proxima_medida - datetime.now(timezone.utc)).total_seconds()
            print(f"Hora actual: {datetime.now(timezone.utc).strftime('%H:%M:%S')}")
            print(f"Esperando hasta: {proxima_medida.strftime('%H:%M:%S')} ({int(segundos_espera)} segundos)")

            time.sleep(segundos_espera)


# Main #

# Configuración inicial


[writer, reader, org, bucket] = config_influx()
gtfs_data = read_file()
timestamp = None
last_distance = float('inf')
last_lat, last_lon = 0, 0

index_ida, index_vuelta = 1, 1 # Empieza en la segunda parada, ya que la primera es redundante
                                # (La primera de la ida es la ultima de la vuelta, y viceversa)
seq_ida, seq_vuelta = None, None 
ida = True


# Loop principal
while True:
    # Obtención de datos de InfluxDB
    data_influx = read_influx(reader, org)
    # Comprueba si hay datos nuevos viendo si ha cambiado el timestamp
    try:
        if data_influx[0].records[0].get_time() == timestamp or data_influx[0].records[0].get_value() is None:
            print("No hay nuevos datos en Influx. Esperando...")
            time.sleep(1)
            continue
    except IndexError:
        print("No se han encontrado datos en Influx. Esperando...")
        time.sleep(30)
        continue

    # Avanza si hay datos nuevos:
    timestamp = data_influx[0].records[0].get_time() # Hora de llegada de los datos
    lat_bus = data_influx[0].records[0].get_value() # Latitud
    lon_bus = data_influx[1].records[0].get_value() # Longitud
    v_bus = data_influx[2].records[0].get_value() # Velocidad



    # Lectura de secuencia
    if seq_ida is None or seq_vuelta is None: # Si aun no se han establecido las rutas, las crea
        try:
            [seq_ida, coords_ida, seq_vuelta, coords_vuelta] = lee_secuencia(gtfs_data, id_ruta_ida, id_ruta_vuelta)            

        except Exception as e: 
            print(f"Error al leer la secuencia: {e}")
            #time.sleep(60)
            continue
    
    else:
        # Calcula la distancia a la siguiente parada
        [distancia, last_distance, index_ida, index_vuelta, modo_ida] = next_stop(lat_bus, lon_bus, seq_ida, seq_vuelta, coords_ida,
                                                                                coords_vuelta, modo_ida, last_distance, 
                                                                                index_ida, index_vuelta)

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

        # Guarda las coordenadas para el cambio brusco de la siguiente iteracion
        last_lat = lat_bus
        last_lon = lon_bus

    tiempo_espera(proxima_medida) # Espera el tiempo teorico necesario para que llegue un mensaje nuevo
