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
    
sensor_paths = []
sensor_uuids = []
sensor_units = [] 
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

startDate = '2016-07-20'
endDate = '2016-07-21'
startTime = '00:00:00.00'
endTime = '00:00:00.00'

data_dict = {}

for sensor in sensorIDs:
    sensordata_url = 'http://analytics.smtresearch.ca/api/?action=listSensorData&sensorID=%s&startDate=%s&endDate=%s' %(sensor, startDate, endDate)
    s_data = s.get(sensordata_url) 
    d = xmltodict.parse(s_data.text) #Convert xml string to dictionary object.
    datetimes = []
    data = []
    n = len(d['result']['readings']['reading']) #count the number of data values
    for reading in range(n):
        datetimes.append(d['result']['readings']['reading'][reading]['timestamp'])
        data.append(d['result']['readings']['reading'][reading]['engUnit'])
    times = []
    for i in range(len(datetimes)):
        times.append(time_str_to_ms(datetimes[i]))
    smap_value = zip(times, data)
    for i in range(len(smap_value)):
        smap_value[i] = list(smap_value[i]) 
    response = smap_post(smap_sourcename, smap_value, sensor_paths[col], sensor_uuids[col], sensor_units, timeout)
    #if we want the data put into a large pandas dataframe:
    data_dict[sensor] = pd.Series(data, index=datetimes) 

#to create the large data frame the assemblage of indexed Series in data_dict    
pdDat = pd.DataFrame(data_dict)


#############################################
#Logoff the server
#############################################

logout_url = 'http://analytics.smtresearch.ca/api/?action=logout'
s.get(logout_url)

