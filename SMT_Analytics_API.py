# -*- coding: utf-8 -*-
"""
Created on Tue Jul 26 13:04:03 2016

@author: brennanless
"""

#Determine the datetimes to pull data for
#List all sensors
#Retrive data from each sensor, post to sMAP in a loop

import requests
import xmltodict
import time
from datetime import datetime
import os
import requests
import json
import pandas as pd
from datetime import timedelta

def time_str_to_ms(time_str):
    pattern = "%Y-%m-%d %H:%M:%S"
    try:
        epoch = int(time.mktime(time.strptime(time_str, pattern)))
    except ValueError:
        print "time_str_to_ms(): Unexpected input -> %s" % time_str
        raise
    return int(epoch*1000)
    
    #smap_post() function takes ipnuts and sends data to smap database on LBNL remote server.
#smap_value is a list of lists (date in ms and value).
def smap_post(sourcename, smap_value, path, uuid, units, timeout): #prior smap_value was x, y
    smap_obj = {}
    smap_obj[path] = {}
    metadata = {}
    metadata['SourceName'] = sourcename
    metadata['Location'] = {'City': 'Fresno'}
    smap_obj[path]["Metadata"] = metadata
    smap_obj[path]["Properties"] = {"Timezone": "America/Los_Angeles",
                                    "UnitofMeasure": units,
                                    "ReadingType": "double"}
    smap_obj[path]["Readings"] = smap_value # previously:[smap_value], [x,y]
    smap_obj[path]["uuid"] = uuid
    data_json = json.dumps(smap_obj)
    http_headers = {'Content-Type': 'application/json'}
    #smap_url = "https://render04.lbl.gov/backend/add/vQRmOWwffl65TRkb4cmj3jWfDiPsglwy4Bog"
    smap_url = "https://rbs-box2.lbl.gov/backend/add/vQRmOWwffl65TRkb4cmj3jWfDiPsglwy4Bog"
    r = requests.post(smap_url, data=data_json, headers=http_headers, verify=False, timeout=timeout)
    return r.text
    
#############################################
#Fixed lists of paths, uuids, units 
#############################################    
    
    
timeout = 10    
#Need to re-order these according to the order that they come into the SMT Analytics system.    
# sensor_paths = ['/EW52_Nx_Ev_Rd-WMC', '/EW52_Nx_Pk_Rd-WMC', '/EW52_Sx_Pk_Rd-WMC', '/EW52_truss-WMC', 
# 	'/EW52_joist-WMC']
sensor_paths = ['/EW52_Nx_Ev_Rd-WMC', '/EW52_Nx_Pk_Rd-WMC', '/EW52_Sx_Pk_Rd-WMC', '/EW52_truss-WMC',
	'/EW52_joist-WMC', '/EW52_Nx_Pk_Rd_2535-WMC', '/EW52_Nx_Pk_Rd_3345-WMC', '/EW52_Nx_Pk_Rd_backup-TM', '/NS33_Ex_Rd-WMC', 
	'/NS33_Wx_Rd-WMC', '/NWg_Nx_peak_Rd-WMC', '/NS33_Nx_face_peak-WMC', '/NS33_joist-WMC', '/NS33_truss-WMC']	
# sensor_uuids = ['4da00794-581b-11e6-8fff-acbc32bae629', '53c2ee2e-581b-11e6-9741-acbc32bae629', '59fbeb35-581b-11e6-b29b-acbc32bae629', 
# 	'656d0914-581b-11e6-aaf2-acbc32bae629', '6b66e57d-581b-11e6-8879-acbc32bae629']
sensor_uuids = ['4da00794-581b-11e6-8fff-acbc32bae629', '53c2ee2e-581b-11e6-9741-acbc32bae629', '59fbeb35-581b-11e6-b29b-acbc32bae629',
	'656d0914-581b-11e6-aaf2-acbc32bae629', '6b66e57d-581b-11e6-8879-acbc32bae629', 'b160f785-208e-11e7-bae7-acbc32bae629', 
	'ba85ba5e-208e-11e7-afb2-acbc32bae629', 'c0e85ce3-208e-11e7-9551-acbc32bae629', 'fb10bf8a-6a4f-11e6-a772-acbc32bae629', 
	'05dc89cc-6a50-11e6-bc2a-acbc32bae629', '0fa4b140-6a50-11e6-bc46-acbc32bae629', '15a782f0-6a50-11e6-9b83-acbc32bae629', 
	'1ba52891-6a50-11e6-9967-acbc32bae629', '211f6f8a-6a50-11e6-8777-acbc32bae629']	
sensor_units = 'ohms' 
#smap_sourcename = 'SMT_A3_8910'  
smap_sourcename = 'Fresno_WoodMC_RawOhms' 
 
sensorIDs = range(188027, 188035) #senor IDs for SMT A3 8910, was 188032

Node8909 = range(188040, 188046) #sensor IDs for SMT A3 8909

for sens in Node8909:
	sensorIDs.append(sens) #Append 8909 IDs to 8910 IDs list.
	
for id in range(len(sensorIDs)):
	sensorIDs[id] = str(sensorIDs[id])

#############################################
#Initiate HTTP session
#############################################
s = requests.Session()


#Construct login URL and send login request
user_name = 'bdless@lbl.gov'
password = 'lbl.gov'
login_url = 'http://analytics.smtresearch.ca/api?action=login&user_username=%s&user_password=%s' %(user_name, password)

s_login = s.get(login_url) #successful
d = xmltodict.parse(s_login.text)
if d['result']['login'] != 'success':
	raise SystemExit()

#############################################
#Construct sensor data for sensor in sensorIDs
#############################################

#construct start and end times between now and four hours prior.
endDateTime = datetime.now()
startDateTime = (datetime.now()-timedelta(hours=288)) #48

startDate = startDateTime.strftime('%Y-%m-%d')
endDate = endDateTime.strftime('%Y-%m-%d')
startTime = startDateTime.strftime('%H:%M:%S.%f2')[:-5]
endTime = endDateTime.strftime('%H:%M:%S.%f2')[:-5]

data_dict = {} #Dictionary to fill with pandas Series for each sensor in loop.


ind = 0

for sensor in sensorIDs:
    #print sensor
    #sensordata_url = 'http://analytics.smtresearch.ca/api/?action=listSensorData&sensorID=%s&startDate=%s&endDate=%s' %(sensor, startDate, endDate)
    sensordata_url = 'http://analytics.smtresearch.ca/api/?action=listSensorData&sensorID=%s&startDate=%s&endDate=%s&startTime=%s&endTime=%s' %(sensor, startDate, endDate, startTime, endTime)
    try:
        s_data = s.get(sensordata_url) 
        d = xmltodict.parse(s_data.text) #Convert xml string to dictionary object.
    except:
        print 'Failed to retrieve and parse sensor data, will try again in 60 seconds.'
        continue
    datetimes = []
    data = []
    if d['result']['readings'] == None:
        continue
    n = len(d['result']['readings']['reading']) #count the number of data values
    for reading in range(n):
        datetimes.append(d['result']['readings']['reading'][reading]['timestamp'])
        data.append(int(d['result']['readings']['reading'][reading]['engUnit']))
    #print data[0:5]
    times = []
    for i in range(len(datetimes)):
        times.append(time_str_to_ms(datetimes[i]))
    smap_value = zip(times, data)
    for i in range(len(smap_value)):
        smap_value[i] = list(smap_value[i]) 
    #print smap_value[0]
    #print sensor_paths[ind], sensor_uuids[ind]
    try:
        response = smap_post(smap_sourcename, smap_value, sensor_paths[ind], sensor_uuids[ind], sensor_units, timeout)
        ind += 1
        #print ind
    except requests.exceptions.RequestException as e:	
        print e
        continue
    if not response:
        continue
    
    	
#if we want the data put into a large pandas dataframe:
#data_dict[sensor] = pd.Series(data, index=datetimes) 

#to create the large data frame the assemblage of indexed Series in data_dict    
#pdDat = pd.DataFrame(data_dict)
#data_dict[sensor] = pd.Series(data, index=datetimes)   
#data = [int(x) for x in data] 

#############################################
#Logoff the server
#############################################

logout_url = 'http://analytics.smtresearch.ca/api/?action=logout'
s.get(logout_url)



#############################################
#Construct list of nodes on jobID
#############################################

# jobID = '2770'
# listnode_url = 'http://analytics.smtresearch.ca/api/?action=listNode&jobID=%s' %jobID
# 
# s_nodes = s.get(listnode_url) #successful
# d = xmltodict.parse(s_nodes.text) #Convert xml string to dictionary object.
# #Access elements of the dict object like this:
# nodes = []
# for node in range(3):
#     nodes.append(d['result']['nodes']['node'][node]['nodeID'])


#############################################
#Construct list of sensors on all nodes
#############################################
# sensorIDs = []
# 
# for node in nodes:
#     listsensor_url = 'http://analytics.smtresearch.ca/api/?action=listSensor&nodeID=%s' %node
#     s_sensors = s.get(listsensor_url) #TooManyRedirects error    
#     d = xmltodict.parse(s_sensors.text)
#     n = len(d['result']['sensors']['sensor'])
#     for sensor in range(n):
#         sensorIDs.append(d['result']['sensors']['sensor'][sensor]['sensorID'])

