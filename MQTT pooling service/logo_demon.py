import paho.mqtt.client as mqtt
import snap7
from snap7.util import *
import os
import time
import datetime
import sys

#plc = snap7.client.Client()

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    #client.subscribe("$SYS/#")
    client.subscribe("siemens/logo/#")

    send_status(client, 12)


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    try:
        #print(msg.topic+" "+str(msg.payload))
        path_list = msg.topic.split("/")

        
        if(len(path_list) < 5):
            return

        output_number = int(path_list[3])

        command = path_list[4]

        if(path_list[0] != "siemens"):
            return

        if(command != 'switch'):
            return

        print("============================================================")
        print(msg.topic+" "+str(msg.payload))
        print(path_list)

        print("output_number", str(output_number))

        for x in range(0, 3):
            try:
                if(command == 'switch'): 
                    if(str(msg.payload) == "b'on'"):
                        print("Need on output ", str(output_number))
                        on_output(output_number)
                        client.publish("siemens/logo/" + str(output_number) + "/status","on", 0, True)
                        time.sleep(0.5)
                        return
         
                    if(str(msg.payload) == "b'off'"):
                        print("Need off output ", str(output_number))
                        off_output(output_number)
                        client.publish("siemens/logo/" + str(output_number) + "/status","off", 0, True)
                        time.sleep(0.5)
                        return
        
                #if(command == 'status'):
                #    print("Status requested")
                #    send_status(client, output_number)
                #    time.sleep(0.5)
                #    return

                print("Unknown commad")  
                return
            except Exception:
                print("Exception on_message exception")
                statusplc = None
                time.sleep(0.5)
    except Exception:
                print("Exception on_message main")
                time.sleep(0.5)
    
    


byte_number = 3


def preparePlc(plc):
    print("preparePlc 1")
    #if(plc is None):
    #    print("Try create client...")
    #    plc = snap7.client.Client()

    print("preparePlc 2")
    connectionstate = plc.get_connected()
    print("connection state = " + str(connectionstate))
    if(not connectionstate ):    
        print("Try connect...")
        plc.connect('SIEMES_LOGO!_ADDRESS_OR_IP', 0, 1)
    print("preparePlc 3")    


statusplc = None

def send_status(client, outputs_count):
    print("Send status")
    global statusplc
    if( statusplc is None):
       statusplc = snap7.client.Client()
    try:
        preparePlc(statusplc)

        # Read actual output state
        print("Try read state...")
        outputs = statusplc.read_area(0x82, 0, 0, byte_number)
        #print("Try disconnect...")
        #statusplc.disconnect()
        #print("Try destroy...")
        #statusplc.destroy()

        status = "State "

        for output_number in range(0, outputs_count):
            byte_num = output_number // 8
            bit_num = output_number % 8
            #print("byte_num = ",byte_num, " bit_num = ", bit_num)

            actual_state =  get_bool(outputs, byte_num, bit_num)
        
            if(actual_state == 1):
                client.publish("siemens/logo/" + str(output_number) + "/status","on", 0, True)
                #print("statsu on")
                status = status + " 1"
            else:
                client.publish("siemens/logo/" + str(output_number) + "/status","off", 0, True)
                #print("statsu off")
                status = status + " 0"

        #print("End send status")
        timestr = datetime.datetime.now().strftime("%d.%m.%y %H:%M.%S.%f")
        print(timestr + " " + status)
    except:
        statusplc = None
        print("Exception", sys.exc_info()[0])
        time.sleep(0.5)


def on_output(output_number):
    #global statusplc
    statusplc = snap7.client.Client()
    print("Switch on ")
    byte_num = output_number // 8
    bit_num = output_number % 8

    print("byte_num = ",byte_num, " bit_num = ", bit_num)


    preparePlc(statusplc)
    # Read actual output state
    outputs = statusplc.read_area(0x82, 0, 0, byte_number)
    actual_state =  get_bool(outputs, byte_num, bit_num)
    if(actual_state == 1):
        print("already on!")
        #Send actual device state
        #plc.disconnect()
        #plc.destroy()
        return

    byte_array = statusplc.db_read(1, 800, byte_number)
    old_value = get_bool(byte_array, byte_num, bit_num)
    new_value = not old_value
    set_bool(byte_array, byte_num, bit_num, new_value)

    statusplc.db_write(1, 800, byte_array)

    statusplc.disconnect()
    statusplc.destroy()
    print("Switch on operation completed!")

def off_output(output_number):
    #global statusplc
    statusplc = snap7.client.Client()
    print("Switch off ")
    byte_num = output_number // 8
    bit_num = output_number % 8

    print("byte_num = ",byte_num, " bit_num = ", bit_num)


    preparePlc(statusplc)
    # Read actual output state
    outputs = statusplc.read_area(0x82, 0, 0, byte_number)
    actual_state =  get_bool(outputs, byte_num, bit_num)
    if(actual_state == 0):
        print("already off!")
        #Send actual device state
        #plc.disconnect()
        #plc.destroy()
        return

    byte_array = statusplc.db_read(1, 800, byte_number)
    old_value = get_bool(byte_array, byte_num, bit_num)
    new_value = not old_value
    set_bool(byte_array, byte_num, bit_num, new_value)

    statusplc.db_write(1, 800, byte_array)

    statusplc.disconnect()
    statusplc.destroy()
    print("Switch off operation completed!")

while True:
    try:
        client = mqtt.Client()
        client.on_connect = on_connect
        client.on_message = on_message

        client.connect("MQTT_SERVE_ADDRESS_OR_IP", 1883, 60)

        # Blocking call that processes network traffic, dispatches callbacks and
        # handles reconnecting.
        # Other loop*() functions are available that give a threaded interface and a
        # manual interface.
        #client.loop_forever()

        client.loop_start()
        while True:
            #client.loop()
            send_status(client, 12)
            timestr = datetime.datetime.now().strftime("%d.%m.%y %H:%M.%S.%f")
            client.publish("siemens/logo/updatetime", timestr, 0, True)
            time.sleep(1)

    except KeyboardInterrupt:
        logging.info("Stopping...")
        break
    except:
        print("Main loop exception")
        statusplc = None
        time.sleep(1)
