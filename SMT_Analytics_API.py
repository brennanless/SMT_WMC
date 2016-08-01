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
    
#Need to re-order these according to the order that they come into the SMT Analytics system.    
sensor_paths = ['MaR_Sx_EWms_peak_Rd-WoodMC', 'MaR_Nx_EWms_peak_Rd-WoodMC', 'MaR_Cx_truss-WoodMC', 'MaR_Cx_joist-WoodMC',
	'MiR_Ex_NSms_peak_Rd-WoodMC', 'MiR_Wx_NSms_peak_Rd-WoodMC', 'MiR_Nx_face_peak-WoodMC', 'NWg_Nx_EWms_peak_Rd-WoodMC',
	'NWg_Nx_truss-WoodMC', 'NWg_Nx_joist-WoodMC']
sensor_uuids = ['4da00794-581b-11e6-8fff-acbc32bae629', '53c2ee2e-581b-11e6-9741-acbc32bae629', '59fbeb35-581b-11e6-b29b-acbc32bae629',
	'656d0914-581b-11e6-aaf2-acbc32bae629', '6b66e57d-581b-11e6-8879-acbc32bae629', '71c34c78-581b-11e6-a46e-acbc32bae629',
	'7a1bf623-581b-11e6-b5db-acbc32bae629', '7ec67eae-581b-11e6-84d5-acbc32bae629', '855d5c8a-581b-11e6-9ad7-acbc32bae629',
	'8b8ec8ba-581b-11e6-aece-acbc32bae629']
sensor_units = ["%","%","%","%","%","%","%","%","%","%"] 
smap_sourcename = 'SMT_A3'   


#############################################
#Initiate HTTP session
#############################################
s = requests.Session()


#Construct login URL and send login request
user_name = 'bdless@lbl.gov'
password = 'lbl.gov'
login_url = 'http://analytics.smtresearch.ca/api?action=login&user_username=%s&user_password=%s' %(user_name, password)

s_login = s.get(login_url) #successful


#############################################
#Construct list of nodes on jobID
#############################################

jobID = '2770'
listnode_url = 'http://analytics.smtresearch.ca/api/?action=listNode&jobID=%s' %jobID

s_nodes = s.get(listnode_url) #successful
d = xmltodict.parse(s_nodes.text) #Convert xml string to dictionary object.
#Access elements of the dict object like this:
nodes = []
for node in range(3):
    nodes.append(d['result']['nodes']['node'][node]['nodeID'])


#############################################
#Construct list of sensors on all nodes
#############################################
sensorIDs = []

for node in nodes:
    listsensor_url = 'http://analytics.smtresearch.ca/api/?action=listSensor&nodeID=%s' %node
    s_sensors = s.get(listsensor_url) #TooManyRedirects error    
    d = xmltodict.parse(s_sensors.text)
    n = len(d['result']['sensors']['sensor'])
    for sensor in range(n):
        sensorIDs.append(d['result']['sensors']['sensor'][sensor]['sensorID'])

#############################################
#Construct sensor data for sensor in sensorIDs
#############################################

#construct start and end times between now and four hours prior.
endDateTime = datetime.now()
startDateTime = (datetime.now()-timedelta(hours=4)) 

startDate = startDateTime.strftime('%Y-%m-%d')
endDate = endDateTime.strftime('%Y-%m-%d')
startTime = startDateTime.strftime('%H:%M:%S.%f2')[:-5]
endTime = endDateTime.strftime('%H:%M:%S.%f2')[:-5]

data_dict = {} #Dictionary to fill with pandas Series for each sensor in loop.


ind = 0

for sensor in sensorIDs:
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
        data.append(d['result']['readings']['reading'][reading]['engUnit'])
    #data_dict[sensor] = pd.Series(data, index=datetimes)    
    times = []
    for i in range(len(datetimes)):
        times.append(time_str_to_ms(datetimes[i]))
    smap_value = zip(times, data)
    for i in range(len(smap_value)):
        smap_value[i] = list(smap_value[i]) 
    try:
    	response = smap_post(smap_sourcename, smap_value, sensor_paths[ind], sensor_uuids[ind], sensor_units[ind], timeout)
	except requests.exceptions.ConnectionError:	
		print 'Connection error, will try again later.'
	if not response:
		count += 1
	ind += 1 #increment counter
    	
    #if we want the data put into a large pandas dataframe:
    #data_dict[sensor] = pd.Series(data, index=datetimes) 

#to create the large data frame the assemblage of indexed Series in data_dict    
#pdDat = pd.DataFrame(data_dict)


#############################################
#Logoff the server
#############################################

logout_url = 'http://analytics.smtresearch.ca/api/?action=logout'
s.get(logout_url)

