/*  SCRIPT DE GEOLOCALIZACION DE TRANSPORTE PUBLICO
Este programa utiliza el GPS del Seeeduino LoRaWAN GPS para obtener la localización en tiempo real del vehículo,
enviandola a la TTN a través del protocolo LoRAWAN.
*/

// ---------------------------------------------------CONFIG------------------------------------------------------------ //
#include <TinyGPSPlus.h>
#include <LoRaWan.h>
#include <tinyFrame.h>

// Structs y objects
struct tinyFrame frame;
TinyGPSPlus gps;

// Las coordenadas se promediarán con tantos valores como se haya establecido en "precision" 
#define precision 10

// Variables globales
float lat[precision];
float lng[precision];
float lat_old = 0, lng_old = 0;
float distancia = 0;

int i = 0;
int ultimo_envio = 0;
int counter = 0;

bool cambio_brusco = false;

//char buffer[256];


// Credenciales para la TTN
char AppKey[] = "4FD4027BF712477561B43A6ABA38B129";
char devEUI[] = "70B3D57ED006BB05";
char AppEUI[] = "0000000000000000";


// ---------------------------------------------------SCRIPT------------------------------------------------------------ //

void setup()
{
  // Comienza la comunicación serie con el ordenador para debug y con el GPS (Serial2)
  SerialUSB.begin(115200);
  Serial2.begin(9600);

  // Inicia el módulo LoRa
  setup_lora();
  while(!lora.setOTAAJoin(JOIN));

  //Serial.println("JOINED");

  frame.printDecoder = false; // Decoder de tinyframe. Útil para decodificar el payload en la TTN
}

void loop()
{
  while (Serial2.available() > 0){
    if (gps.encode(Serial2.read())){
      
      if (gps.location.isValid()){ // Si la señal de gps es valida, captura las medidas
        lat[i] = gps.location.lat();  // latitud
        lng[i] = gps.location.lng();  // longitud
        //displayInfo();
        i++;
      } else {
        //Serial.println("Señal de gps no válida");
        delay(2000);
      }

      if (i >= precision){  // Cuando los vectores de tamaño "precision" se llenan:
        // Inicia valores medios:
        float lat_m = 0;
        float lng_m = 0;

        // Promedia las medidas
        lat_m = average(lat, precision);
        lng_m = average(lng, precision);

        // Descomentar para ver los valores medios en pantalla
        /*
        Serial.println("Latitude:");
        Serial.println(lat_m, 6);
        Serial.println("Longitude:");
        Serial.println(lng_m, 6);
        */

        // Envia datos a la TTN
        //memset(buffer, 0, 256);
        envio_lora(lat_m, lng_m);

        // Calcula la distancia con la posicion anterior
        distancia = dist_semiverseno(lat_old, lat_m, lng_old, lng_m);
        //Serial.println(distancia);
        
        // Guarda los valores actuales
        lat_old = lat_m;
        lng_old = lng_m;

        // Selecciona si ha habido un cambio de posicion brusco
        if (distancia >= 50){
          cambio_brusco = true;
        } else {
          cambio_brusco = false;
        }

        // Reinicia los array
        memset(lat, 0, sizeof(lat));
        memset(lng, 0, sizeof(lng));
        i = 0;

        // Tiempo de espera si el mensaje se envio exitosamente
        if (cambio_brusco == false){
          while((millis() - ultimo_envio) < 1000*60*5){} // 5 minutos si no hubo cambio brusco
        } else {
          while((millis() - ultimo_envio) < 1000*60*2){} // 2 minutos si hubo cambio brusco
        }
      }
    }
  }
}

/* ---------------Functiones-----------------*/ 

float average(float data[], int size){
/* Funcion simple que hace la media
  data[] -> Vector con los datos a promediar
  size -> Tamaño del vector data[]*/
  float sum = 0;
  for (int i = 0; i<size; i++){
    sum += data[i];
  }
  return sum/size;
}

void displayInfo()
// Función para mostrar la información en pantalla para hacer debugging
{
  Serial.print(F("Location: ")); 
  if (gps.location.isValid())
  {
    Serial.print(gps.location.lat(), 6);
    Serial.print(F(","));
    Serial.print(gps.location.lng(), 6);
  }
  else
  {
    Serial.print(F("INVALID"));
  }

  Serial.print(F("  Date/Time: "));
  if (gps.date.isValid())
  {
    Serial.print(gps.date.month());
    Serial.print(F("/"));
    Serial.print(gps.date.day());
    Serial.print(F("/"));
    Serial.print(gps.date.year());
  }
  else
  {
    Serial.print(F("INVALID"));
  }

  Serial.print(F(" "));
  if (gps.time.isValid())
  {
    if (gps.time.hour() < 10) Serial.print(F("0"));
    Serial.print(gps.time.hour());
    Serial.print(F(":"));
    if (gps.time.minute() < 10) Serial.print(F("0"));
    Serial.print(gps.time.minute());
    Serial.print(F(":"));
    if (gps.time.second() < 10) Serial.print(F("0"));
    Serial.print(gps.time.second());
    Serial.print(F("."));
    if (gps.time.centisecond() < 10) Serial.print(F("0"));
    Serial.print(gps.time.centisecond());
  }
  else
  {
    Serial.print(F("INVALID"));
  }

  Serial.println();
}

void setup_lora (){
// LoRa module
  lora.init();

  lora.setKey(NULL, NULL, AppKey);
  lora.setId(NULL, devEUI, AppEUI);

  lora.setDeciveMode(LWOTAA);
  lora.setDataRate(DR4, EU868);

  lora.setDutyCycle(true);
  lora.setJoinDutyCycle(true);

  lora.setPower(14);

}

void envio_lora(float latitude, float longitude){
  // Envia la información
  frame.clear();
  frame.append_int32_t((int32_t) (latitude*1000000.0));
  frame.append_int32_t((int32_t) (longitude*1000000.0));

  // Envía el mensaje a TTN
  /*
  Serial.println("Intentando enviar: ");
  
  if (lora.transferPacket(frame.buffer, 10)){
    SerialUSB.println("Data sent");
    ultimo_envio = millis();
  } else {
    SerialUSB.println("Error");
    error_envio = true;
  }
  */

  //Serial.println("Intentando enviar");

  for (counter = 0;counter <= 3;counter++){
    lora.transferPacket(frame.buffer, 10);
  }
  //SerialUSB.println("Data sent");
  ultimo_envio = millis();
}

float dist_semiverseno(float lat_1, float lat_2, float lng_1, float lng_2){
  // Funcion para calcular la distancia entre 2 coordenadas mediante la formula del semiverseno
  float R = 6378*1000;

  float lat_1_rad = lat_1*PI/180.0;
  float lat_2_rad = lat_2*PI/180.0;
  float lng_1_rad = lng_1*PI/180.0;
  float lng_2_rad = lng_2*PI/180.0;

  float h = (1 - cos(lat_1_rad - lat_2_rad))/2 + cos(lat_1_rad)*cos(lat_2_rad)*(1 - cos(lng_1_rad - lng_2_rad))/2;

  float d = 2 * R * asin(sqrt(h));

  return d;
}

