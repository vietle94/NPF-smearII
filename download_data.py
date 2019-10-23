# -*- coding: utf-8 -*-
"""
Created on Sat Oct 19 21:21:37 2019

@author: VIET
"""
# This code accompanies the Project.ipynb file.
# This code will download all the data from SMEARII, export them to csv files inside newly created "data" folder

#%% Load library
import os
import requests
import concurrent.futures # For multi-threading IO and multi-processing

#%% Get data API links from SMEARII

data_links = [
        # UVA
        'https://avaa.tdata.fi/smear-services/smeardata.jsp?variables=UV_A&table=HYY_META&from=2008-01-01 00:00:00.971&to=2009-12-31 23:59:59.859&quality=CHECKED&averaging=NONE&type=NONE',
        # UVB
        'https://avaa.tdata.fi/smear-services/smeardata.jsp?variables=UV_B&table=HYY_META&from=2008-01-01 00:00:00.971&to=2009-12-31 23:59:59.859&quality=CHECKED&averaging=NONE&type=NONE',
        # Air temperature 16.8m
        'https://avaa.tdata.fi/smear-services/smeardata.jsp?variables=T168&table=HYY_META&from=2008-01-01 00:00:00.131&to=2009-12-31 23:59:59.586&quality=CHECKED&averaging=NONE&type=NONE',
        # Wind speed 16.8m
        'https://avaa.tdata.fi/smear-services/smeardata.jsp?variables=WSU168&table=HYY_META&from=2008-01-01 00:00:00.131&to=2009-12-31 23:59:59.586&quality=CHECKED&averaging=NONE&type=NONE',
        # Humidity 16.8m
        'https://avaa.tdata.fi/smear-services/smeardata.jsp?variables=RHIRGA168&table=HYY_META&from=2008-01-01 00:00:00.131&to=2009-12-31 23:59:59.586&quality=CHECKED&averaging=NONE&type=NONE',
        # O3 16.8m
        'https://avaa.tdata.fi/smear-services/smeardata.jsp?variables=O3168&table=HYY_META&from=2008-01-01 00:00:00.131&to=2009-12-31 23:59:59.586&quality=CHECKED&averaging=NONE&type=NONE',
        # NOx 16.8m
        'https://avaa.tdata.fi/smear-services/smeardata.jsp?variables=NOx168&table=HYY_META&from=2008-01-01 00:00:00.131&to=2009-12-31 23:59:59.586&quality=CHECKED&averaging=NONE&type=NONE',
        # SO2 16.8m
        'https://avaa.tdata.fi/smear-services/smeardata.jsp?variables=SO2168&table=HYY_META&from=2008-01-01 00:00:00.131&to=2009-12-31 23:59:59.586&quality=CHECKED&averaging=NONE&type=NONE',
        ]

#%% Download file & parse date time function

#Create folder to store all csv file
if not(os.path.isdir('/data')):
    os.makedirs('data')


def download_csv(url):
    with requests.get(url) as response:
        name = url.split('=')[1].split('&')[0]
        name = f'{name}.csv'
        with open(f'data/{name}', 'wb') as f:
            f.write(response.content)
            print(f'{name} was downloaded...')

#%% Multi-threading IO for downloading files
with concurrent.futures.ThreadPoolExecutor() as executor:
    executor.map(download_csv, data_links)
