#ifndef _TINYFRAME_H_
#define _TINYFRAME_H_

#include <Arduino.h>

#ifndef FRAME_SERIAL
    #define FRAME_SERIAL Serial // Can be overwrite
#endif

struct tinyFrame {
    uint8_t buffer[255];
    uint8_t idx = 0;
    bool printDecoder = false;
    bool printBufferBytes = false;

    void clear(){
        memset(buffer, 0, sizeof(buffer));
        idx = 0;
    }


    void append_uint8_t(uint8_t valor){
       if(printDecoder){
            FRAME_SERIAL.print("val_uint8 = "); TTNDecoder(1); FRAME_SERIAL.println(";");
        } 
        append(valor, 1);
    }

    void append_uint16_t(uint16_t valor){
        if(printDecoder){
            FRAME_SERIAL.print("val_uint16 = "); TTNDecoder(2); FRAME_SERIAL.println(";");
        } 
        append(valor, 2);
    }

    void append_uint32_t(uint32_t valor){
        if(printDecoder){
            FRAME_SERIAL.print("val_uint32 = ("); TTNDecoder(4); FRAME_SERIAL.println(") >>> 0;");
        } 
        append(valor, 4);
    }

    void append_int8_t(int8_t valor){
        if(printDecoder){
            FRAME_SERIAL.print("val_int8 = "); TTNDecoder(1); FRAME_SERIAL.println(" << 24 >> 24;");
        }
        append(valor, 1);
    }

    void append_int16_t(int16_t valor){
        if(printDecoder){
            FRAME_SERIAL.print("val_int16 = ("); TTNDecoder(2); FRAME_SERIAL.println(") << 16 >> 16;");
        }
        append(valor, 2);
    }

    void append_int32_t(int32_t valor){
        if(printDecoder){
           FRAME_SERIAL.print("val_int32 = "); TTNDecoder(4); FRAME_SERIAL.println(";");
        }
        append(valor, 4);
    }

    void TTNDecoder(uint8_t num){
        for (uint8_t i = 0; i<num; i++){
            if(i > 0) FRAME_SERIAL.print(" | ");
            FRAME_SERIAL.print("input.bytes["); FRAME_SERIAL.print(idx+i); 
            FRAME_SERIAL.print("] << "); FRAME_SERIAL.print((uint8_t) 8*i);
        }
    }

    void append(uint32_t valor, uint8_t numBytes) {
        for (uint8_t i = 0; i < numBytes; i++) {
            buffer[idx] = (uint32_t)(valor & 0xff);
            
            if(printBufferBytes){
                FRAME_SERIAL.print("buffer["); FRAME_SERIAL.print(idx); 
                FRAME_SERIAL.print("] = "); FRAME_SERIAL.println((uint32_t)(valor & 0xff), HEX);
            }

            idx++;
            valor >>= 8;
        }
    }

    uint8_t size(void){
        return idx;
    }

    void printHex(void){
        for(unsigned char i = 0; i < idx; i ++)
        {
            FRAME_SERIAL.print("0x"); FRAME_SERIAL.print(buffer[i], HEX); FRAME_SERIAL.print(" ");
        }
        FRAME_SERIAL.println();
    }

    void hexChar(char *buff, uint16_t size){
        if (size < idx*2){
            FRAME_SERIAL.println("no enough size to hexChar representation");
            return;
        }
        strcpy(buff, "");
        for(unsigned char i = 0; i < idx; i++)
        {
            sprintf(buff+i*2, "%02X", buffer[i]);
        }

        return;
    }
};


#endif
