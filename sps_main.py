import paho.mqtt.client as mqtt
import mysql.connector
import time
import datetime
import json
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s',
                    filename='sps_main.log',
                    filemode='w')

mqtt_server = "localhost"
mqtt_port = 1883
mqtt_alive = 60
sub_topic = "SPS/#"
# pub_agv_topic = "sps/agv/"
# pub_site_topic = "sps/"

host = "localhost"
user = "spsadmin"
passwd = "sps"
database = "parksys"

counter = 0
route = ()
implementing = 0
site201num = 5
currentPosition = 0
end = False
#############################

site201avail = []
for i in range(site201num):
    site201avail.append(0)


# def log_event(event, event_detail):
    # cur = mdb.cursor()
    # dt = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    # cur.execute("INSERT INTO system_log (Time ,Event , Details) VALUES (%s, %s, %s)",
    #             (dt, event, event_detail))
    # mdb.commit()


def on_connect(client, userdata, flags, rc):
    logging.debug("Connected mqtt with result code "+str(rc))
    client.subscribe(sub_topic)


def on_message(client, userdata, msg):
    logging.debug(msg.topic+" "+str(msg.payload, encoding='utf-8'))
    handler(msg.topic, str(msg.payload, encoding='utf-8'))
    # handler(msg.topic, str(msg.payload, encoding='utf-8'))
    # if msg.topic == 'SPS/CB201/agv/uplink':
    #     agvhandler(str(msg.payload, encoding='utf-8'))


def handler(topic, msg):
    global implementing
    if implementing == 0:
        if topic == 'SPS/CB201/000':
            if int(msg) == 1:
                # print("++++")
                routegenerator(currentPosition, targetPosition())
                agvImplement()
    if topic == 'SPS/CB201/agv/uplink':
        if int(msg) == 1:
            agvImplement()


def agvImplement():
    global counter
    global route
    global implementing
    global end
    implementing = 1
    if end == True:
        time.sleep(2)
        counter = 0
        implementing = 0
        route = ()
        end = False
    else:
        logging.debug(route[counter])
        client.publish('SPS/CB201/agv/downlink', route[counter])
        if route[counter] != '20':
            counter += 1
        else:
            end = True
    # else:
        # counter = 0
        # route = ()

    # print(implementing)


def routegenerator(current, target):
    global route
    # temproute = ()

    if current == 0:
        route = ('40', '03')
        if target == 1:
            route += ('13', '04', '14', '01', '51',
                      '24', '14', '03', '13', '20')
        elif target == 2:
            route += ('13', '04', '05', '15', '02', '52',
                      '25', '15', '04', '03', '13', '20')
        elif target == 6:
            route += ('06', '56', '23', '20')
        elif target == 7:
            route += ('13', '04', '34', '07', '57',
                      '24', '34', '03', '13', '20')
        elif target == 8:
            route += ('13', '04', '05', '35', '08', '58',
                      '25', '35', '04', '03', '13', '20')
    logging.info(route)
    return route


def targetPosition():
    mdb = mysql.connector.connect(
                    host="localhost", user="spsadmin", passwd="sps", database="parksys")
    cur = mdb.cursor()
    cur.execute('select s1,s2,s3,s4,s5 from CB201 order by ID DESC limit 1')
    temp = cur.fetchone()
    
    logging.info(temp)
    for i in range(5):
        if temp[i] == 0:
            if i == 0:
                return 1
            elif i == 1:
                return 2
            elif i == 2:
                return 6
            elif i == 3:
                return 7
            elif i == 4:
                return 8
    mdb.close()


# def handler(topic, msg):
#     if topic == 'sps/CB201':
#         payload = msg.split(';')
#         # 221 means the msg is the availabilities of the site.
#         if payload[0] == '221':
#             payloaddict = json.loads(payload[1])
#             for i in range(len(payloaddict)):
#                 site201avail[i] = payloaddict[str(1000+i)]
#             logging.info(site201avail)
#         if payload[0] == '220':  # 220 means there is a car waiting on entrance
#             agvinit(201, site201avail)
#             logging.info("Got client")


def agvinit(site, avail):
    if site == 201:
        pubtopic = 'sps/CB201'
    client.publish(pubtopic, )


# def availMonitor():
#     print(site201avail)


dbsetup = 1
mqttsetup = 1
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
try:
    while True:
        # setupevent
        # while dbsetup == 1:
        #     try:
        #         mdb = mysql.connector.connect(
        #             host="localhost", user="spsadmin", passwd="sps", database="parksys")
        #         logging.info("mdb connected")
        #         dbsetup = 0
        #     except:
        #         logging.debug(
        #             "Warning: No database (connection) found. Retry in one minute.")
        #         time.sleep(60)
        #         pass

        while mqttsetup == 1:
            try:
                client.connect(mqtt_server, mqtt_port, mqtt_alive)
                logging.info("mqtt connected")
                mqttsetup = 0
            except:
                logging.debug("Warning: No broker found. Retry in one minute.")
                # log_event(401, "No broker found")
                time.sleep(60)
                pass

        # client.publish(pub_prefix+"/MySql/MESSAGE", "Connection with broker established.")
        # log_event(110, "DB and MQTT Ready!")

        #########################

        # mainevent
        while mqttsetup == 0:
            mqttsetup = client.loop()

        #############

except KeyboardInterrupt:
    print('end')
