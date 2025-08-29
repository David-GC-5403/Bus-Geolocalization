# tinyTTNFrame
Tiny Frame for Arduino with TTN Decoder helper

# Straight Forward Usage

```C
#define FRAME_SERIAL SerialUSB // #if you want to use SerialUSB instead of Serial object
#include <tinyFrame.h>

struct tinyFrame frame;

void setup(){

  frame.printDecoder = true; // if you want to see TTN / javascript decoder helper
}

void loop(){
  frame.clear(); // Empty the frame
  frame.append_uint8_t(135); // Append the value 135 as unsigned int of 8 bits
  frame.append_int16_t(30000); // Append the value 30000 as integer (with sign) of 16 bits  
  frame.printHex(); // Show the hex values of the current frame
  
  // frame.buffer -> Gives the current frame as buffer (uint8_t array) (compatible with LoRa RF modules)
  // frame.size -> Gives the current frame size
  
  delay(10000);
}
```

## Console output
```console
val_uint8 = input.bytes[0] << 0;
val_int16 = (input.bytes[1] << 0 | input.bytes[2] << 8) << 16 >> 16;
Frame [Hex]: 0x87 0x30 0x75
```
