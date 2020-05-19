#done connection to mqtt/mydb
#done init
#send staus when recieved change of status
#done entrance


import time
import paho.mqtt.client as mqtt
import mysql.connector
import json
import logging


spacenum = 5
status = []
for i in range(spacenum):
    status.append(0)


def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("SPS/CB201/#")

def log_event(event,event_detail):
    print(event, event_detail)
    # client.publish(pub_topic = "SPS/sys", str(event) + ";" + event_detail)
    cur = mdb.cursor()
    dt = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    cur.execute("INSERT INTO system_log (Time ,Event , Details) VALUES (%s, %s, %s)", (dt, event, event_detail))
    mdb.commit()
    # logging.info(str(event)+ ", " + event_detail)
    # led_blink()

def on_message(client, userdata, msg):
    # print(msg.topic+" "+str(msg.payload))
    handler(msg.topic, str(msg.payload, encoding='utf-8'))


def handler(topic, msg):
    space = int(topic.split('/')[2])
    if space == 0:
        entDetected(int(msg))
    else:
        status[(space-1)] = int(msg)
        print('space= '+str(space) + '  '+str(status))
        avail_to_db(status)


def avail_to_db(status):
    cur = mdb.cursor()
    datetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    sql = "INSERT INTO CB201 (Time, "
    for i in range(len(status)):
        sql = sql + "s" + str(i+1) + ", "
    sql = sql.strip(', ') + ") VALUES (%s, " + \
        ("%s, ")*(len(status)-1) + "%s)"
    temp_val = [datetime]
    for i in status:
        temp_val.append(i)
    val = tuple(temp_val)
    # print(sql, val)
    cur.execute(sql, val)
    mdb.commit()

def entDetected(entstate):
    if entstate == 1:
        log_event(220, "Entrance Detected")
        # client.publish("SPS/CB201/ent", entstate)


getstatus = 0
dbsetup = 0

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect("192.168.1.3", 1883, 60)
# client.loop_forever()
print('*****')

while True:
    if dbsetup == 0:
        try:
            mdb = mysql.connector.connect(
                host='192.168.1.3', user='spsadmin', passwd='sps', database='parksys')
            print('db connected')
            dbsetup = 1
        except:
            ("Warning: No database (connection) found. Retry in half minute.")
            time.sleep(30)
            pass

    if getstatus == 0:
        client.publish('SPS/CB201/initstatus', 'init')
        getstatus = 1

    client.loop()
