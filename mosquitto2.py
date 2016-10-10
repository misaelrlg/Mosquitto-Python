#!/usr/bin/env python
# October 10 2016 v2.1.1
# Modificado por Misael LG, @mrlg_13 on Twitter
#
# Python script (v2.7x) that subscribes to a MQTT broker topic and consults from a mysql database

import mysql.connector
import paho.mqtt.client as mqtt
import time

#mosquitto broker config
broker = 'localhost' #localhost
broker_port = 1883 
broker_topic = 'TOPIC_NAME' #nombre del topico
broker_user = ''
broker_pass = ''

#mysql config
config = {
	'user': 'root',
	'password': '',
	'host': 'localhost',
	'database': 'YOUR_DATA_BASE_NAME',
	'option_files': '/etc/my.cnf',
	'charset': 'utf8',
	'use_unicode': True
}

M_VERSION = "1.1"
M_INFO=0

# prepare a cursor object using cursor() method

def my_info(type,message):
	if M_INFO > type:
		print(message)	

def on_connect(mosq, obj, rc):
    print("rc: "+str(rc))

def on_message(mosq, obj, msg):
    print(msg.topic+" "+str(msg.qos)+" "+str(msg.payload))
    #change locations to the table you are using
    queryText = "SELECT * FROM YOUR_TABLE where SOMETHING"
    try:
    	cursor.execute(queryText % msg.payload)
    	for(nombre, apellido, id) in cursor:
		id=str(id)
		msg.payload=str(msg.payload)
		if (id == msg.payload):
			mqttc.publish("TOPIC_NAME", "MESSAGE\0")
    	print cursor.rowcount
	if (cursor.rowcount <= 0):
		mqttc.publish("TOPIC_NAME","MESSAGE\0")
    except ValueError:
	print "Isn´t a number"
    except mysql.connector.errors.ProgrammingError:
	print "INVALID QUERY"
def on_publish(mosq, obj, mid):
    print("mid: "+str(mid))

def on_subscribe(mosq, obj, mid, granted_qos):
    print("Subscribed: "+str(mid)+" "+str(granted_qos))

def on_log(mosq, obj, level, string):
    print(string)

# If you want to use a specific client id, use
#mqttc = mqtt.Client(broker_clientid)
# but note that the client id must be unique on the broker. Leaving the client
# id parameter empty will generate a random id for you.
mainloop=1
while mainloop==1:
	mqttc = mqtt.Client()
	mqttc.on_message = on_message
	mqttc.on_connect = on_connect
	mqttc.on_publish = on_publish
	mqttc.on_subscribe = on_subscribe
	# Uncomment to enable debug messages
	#mqttc.on_log = on_log
	dc=1
	while dc == 1:
	  try:
	    	db = mysql.connector.connect(**config)	
	    	cursor = db.cursor()
		dc = 0
	  except:
	    	my_info(0," ") 
		print("WARNING: DATABASE NOT FOUND. Retrying in 30 seconds.")
	    	time.sleep(30)
		pass
	rc=1
	while rc==1:
		try:
			mqttc.connect(broker, broker_port, 60)
			rc = 0
		except:
			my_info(0," ")
			print( "WARNING: BROKER NOT FOUND. Trying again in 30 seconds")
			time.sleep(30)
			pass	
	mqttc.subscribe(broker_topic, 0)
	while rc == 0:
		try:    		
			rc = mqttc.loop()
		except:
			rc = 1
	print("WARNING: CONNECTION ERROR. RESETTING.")
	print("rc: "+str(rc))
	if M_INFO > 0:
		mainloop = 0
# disconnect from server
print ('Disconnected, done.')
db.close()
