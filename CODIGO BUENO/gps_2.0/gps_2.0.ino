/*  PUBLIC TRANSPORT GEOLOCALIZATION SCRIPT
This program uses the Seeeduino LoRaWAN GPS to receive the actual localization of the bus, train, etc. This info goes to the TTN, where you can resend it elsewhere
*/

// ---------------------------------------------------CONFIG------------------------------------------------------------ //
#include <TinyGPSPlus.h>
#include <LoRaWan.h>
#include <tinyFrame.h>

// Structs and objects
struct tinyFrame frame;
TinyGPSPlus gps;

// The coordinates are averaged with the <precision> number of values. Example: precision = 10 means it will average 10 values
#define precision 10

// Initiate global variables
float lat[precision];
float lng[precision];

int i = 0;

char buffer[256];


// Credentials for TTN
char AppKey[] = "4FD4027BF712477561B43A6ABA38B129";
char devEUI[] = "70B3D57ED006BB05";
char AppEUI[] = "0000000000000000";


// ---------------------------------------------------SCRIPT------------------------------------------------------------ //

void setup()
{
  // Begin serial communication for debugging and GPS communication (Serial2)
  SerialUSB.begin(115200);
  Serial2.begin(9600);

  // Initiate LoRa module
  setup_lora();
  while(!lora.setOTAAJoin(JOIN));
  Serial.println("JOINED");

  frame.printDecoder = true; // Decoder helper from tinyframe. Useful to decode data in TTN later on

  delay(1000);
}

void loop()
{
  while (Serial2.available() > 0){

    if (gps.encode(Serial2.read())){

      if (gps.location.isValid()){
        
        lat[i] = gps.location.lat();  // Stores latitude
        lng[i] = gps.location.lng();  // Stores longitude
        i++;

        if (i >= precision){  // When vectors are full:
          // Initiate average variables

          float lat_m = 0;
          float lng_m = 0;

          // Average the measurements
          lat_m = average(lat, precision);
          lng_m = average(lng, precision);

          // Uncomment if you wanna see values
          //Serial.println("Latitude:");
          //Serial.println(lat_m, 6);
          //Serial.println("Longitude:");
          //Serial.println(lng_m, 6);

          // Send data to TTN
          memset(buffer, 0, 256);
          envio_lora(lat_m, lng_m);
          
          // Reset vector to restart
          for (i = precision; i>0; i--){
            memset(lat, 0, sizeof(lat));
            memset(lng, 0, sizeof(lng));
          }
          i = 0;
        }
      }
    }
  }
}

/* ---------------Functiones-----------------*/ 
float average(float data[], int size){

/* Simple function to average data from a vector

  data[] -> Vector with the info you wanna average
  size -> Size of the vector data[]

*/

  float sum = 0;
  for (int i = 0; i<size[data]; i++){
    sum += data[i];
  }
  return sum/size;
}

void displayInfo()
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
// Initiate LoRa module

    lora.init();

    lora.setKey(NULL, NULL, AppKey);
    lora.setId(NULL, devEUI, AppEUI);

    lora.setDeciveMode(LWOTAA);
    lora.setDataRate(DR0, EU868);

    lora.setDutyCycle(false);
    lora.setJoinDutyCycle(false);

    lora.setPower(14);
}

void envio_lora(float latitude, float longitude){
// Send coordinates 
  frame.clear();
  frame.append_int32_t((int32_t) (latitude*1000000.0));
  frame.append_int32_t((int32_t) (longitude*1000000.0));

  // Envía el mensaje a TTN
  if (lora.transferPacket(frame.buffer, 10)){
    SerialUSB.println("Data sent");
  } else{
    SerialUSB.println("Error");
  }
}

