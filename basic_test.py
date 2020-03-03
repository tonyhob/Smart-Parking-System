import paho.mqtt.client as mqtt
import mysql.connector
import time
import datetime

#############################config

mqtt_server = "localhost"
mqtt_port = 1883
mqtt_alive = 60
sub_topic = "sps/#"
pub_agv_topic = "sps/agv/"
pub_site_topic = "sps/"

host="localhost"
user="spsadmin"
passwd="sps"
database="parksys"

#############################



def log_event(event,event_detail):
    cur = mdb.cursor()
    dt = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    cur.execute("INSERT INTO system_log (Time ,Event , Details) VALUES (%s, %s, %s)", (dt, event, event_detail))
    mdb.commit()

def on_connect(client, userdata, flags, rc):
    print("Connected mqtt with result code "+str(rc))
    client.subscribe(sub_topic)

def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))
    # topinfo = msg.topic.split("/")
    # if topinfo[2] == 'CB201':
    #     payloadinfo = msg.payload.split(";")
        # if payloadinfo[1] == '221':
            

    
dbsetup = 1
mqttsetup = 1
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
try:
    while True:
        #########################setupevent
        while dbsetup == 1:
            try: 
                mdb = mysql.connector.connect(host="localhost", user="spsadmin", passwd="sps", database="parksys")
                dbsetup = 0
                print(mdb)
            except:
                print( "Warning: No database (connection) found. Retry in one minute.")
                time.sleep(60)
                pass

        while mqttsetup == 1:
            try:
                client.connect(mqtt_server,mqtt_port, mqtt_alive)
                mqttsetup = 0
            except:
                print("Warning: No broker found. Retry in one minute.")
                log_event(401, "No broker found")
                time.sleep(60)
                pass
        
        # client.publish(pub_prefix+"/MySql/MESSAGE", "Connection with broker established.")
        log_event(110, "DB and MQTT Ready!")

        #########################

        #############mainevent
        while mqttsetup == 0:
            mqttsetup = client.loop()

        #############

except KeyboardInterrupt:
    print('end')