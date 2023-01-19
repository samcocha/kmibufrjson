from bs4 import BeautifulSoup
import requests
from pathlib import Path
import json
import pybufrkit
from pybufrkit.decoder import Decoder
from pybufrkit.dataquery import NodePathParser, DataQuerent
from flask import jsonify
import pandas as pd
import numpy as np

timestamp = '2023-01-19T14:00:00'

class AppService:
  def create_kmi_df(self,timestamp):
    # List of 20 stations
    lst = [[6407, 'MIDDELKERKE         ', 51.2, 2.89],
            [6414, 'BEITEM              ', 50.9, 3.12],
            [6418, 'ZEEBRUGGE           ', 51.35, 3.2],
            [6434, 'MELLE               ', 50.98, 3.82],
            [6438, 'STABROEK            ', 51.32, 4.36],
            [6439, 'SINT-KATELIJNE-WAVER', 51.08, 4.52],
            [6447, 'UCCLE-UKKEL         ', 50.8, 4.36],
            [6449, 'GOSSELIES           ', 50.45, 4.44],
            [6450, 'DEURNE              ', 51.19, 4.45],
            [6451, 'ZAVENTEM/MELSBROEK  ', 50.9, 4.53],
            [6455, 'DOURBES             ', 50.1, 4.59],
            [6459, 'ERNAGE              ', 50.58, 4.69],
            [6464, 'RETIE               ', 51.22, 5.03],
            [6472, 'HUMAIN              ', 50.19, 5.26],
            [6476, 'SAINT-HUBERT        ', 50.04, 5.4],
            [6477, 'DIEPENBEEK          ', 50.92, 5.45],
            [6478, 'BIERSET             ', 50.65, 5.46],
            [6484, 'BUZENOL             ', 49.62, 5.59],
            [6490, 'SPA (AERODROME)     ', 50.48, 5.91],
            [6494, 'MONT RIGI           ', 50.51, 6.07]]
    df = pd.DataFrame(lst, columns =['stationid', 'stationname', 'lat', 'lon'])
    df = df.set_index('stationid')
    df['timestamp'] = timestamp
    df["temperature"] = np.nan
    df["dewpointtemperature"] = np.nan
    df["visibility"] = np.nan
    df["windgusts"] = np.nan
    df["windspeed"] = np.nan
    df["winddirectiondegrees"] = np.nan
    df["humidity"] = np.nan
    df["precipitation"] = np.nan
    df["rainfalllasthour"] = np.nan
    df["rainfalllast24hour"] = np.nan
    df["pressure"] = np.nan
    df["cloudcover"] = np.nan
    df["cloudbase"] = np.nan
    df["sunpower"] = np.nan

    return df

  def get_kmi_csv(self,timestamp):
    #get csv dataframe
    url=f"https://opendata.meteo.be/service/synop/wfs?request=GetFeature&service=WFS&version=1.1.0&typeName=synop:synop_data&outputFormat=csv&CQL_FILTER=(timestamp={timestamp})&sortby=timestamp"
    csv=pd.read_csv(url)
    csv = csv.set_index('code')

    df1 = self.create_kmi_df(timestamp)
    for stationid in df1.index.values:
      df1.at[stationid, "temperature"] = csv.at[stationid, 'temp']
      df1.at[stationid, "windgusts"] = csv.at[stationid, 'wind_peak_speed']
      df1.at[stationid, "windspeed"] = csv.at[stationid, 'wind_speed']
      df1.at[stationid, "winddirectiondegrees"] = int(csv.at[stationid, 'wind_direction'])
      df1.at[stationid, "humidity"] = csv.at[stationid, 'humidity_relative']
      df1.at[stationid, "precipitation"] = csv.at[stationid, 'precip_quantity']
      df1.at[stationid, "pressure"] = csv.at[stationid, 'pressure']

    return df1

  def bufr_to_dict(self,filename, timestamp):
    # Decode a BUFR file
    decoder = Decoder()
    with open(filename, 'rb') as ins:
      bufr_message = decoder.process(ins.read())

    try:
      stationid = f"6{DataQuerent(NodePathParser()).query(bufr_message, '001002').get_values(0)[0]}"
    except Exception:
      stationid = None

    try:
      stationname = f"{DataQuerent(NodePathParser()).query(bufr_message, '001015').get_values(0)[0].decode('utf-8')}"
    except Exception:
      stationname = None

    try:
      lat = round((DataQuerent(NodePathParser()).query(bufr_message, '005001').get_values(0)[0]),2)
    except Exception:
      lat = None

    try:
      lon = round((DataQuerent(NodePathParser()).query(bufr_message, '006001').get_values(0)[0]),2)
    except Exception:
      lon = None

    try:
      regio = DataQuerent(NodePathParser()).query(bufr_message, '001015').get_values(0)[0].decode('utf-8')
    except Exception:
      regio = None

    try:
      timestamp = timestamp
    except Exception:
      timestamp = None

    try:
      temperature = round((DataQuerent(NodePathParser()).query(bufr_message, '012101').get_values(0)[0] - 273.15),1)
    except Exception:
      temperature = None

    try:
      dewpointtemperature = round((DataQuerent(NodePathParser()).query(bufr_message, '012103').get_values(0)[0] - 273.15),1)
    except Exception:
      dewpointtemperature = None

    try:
      visibility = DataQuerent(NodePathParser()).query(bufr_message, '020001').get_values(0)[0]
    except Exception:
      visibility = None

    try:
      windgusts = DataQuerent(NodePathParser()).query(bufr_message, '011041').get_values(0)[0][1][0]
    except Exception:
      windgusts = None

    try:
      windspeed = DataQuerent(NodePathParser()).query(bufr_message, '011002').get_values(0)[0]
    except Exception:
      windspeed = None

    try:
      humidity = DataQuerent(NodePathParser()).query(bufr_message, '013003').get_values(0)[0]
    except Exception:
      humidity = None

    try:
      rainfalllasthour = DataQuerent(NodePathParser()).query(bufr_message, '013011').get_values(0)[0][0][0]
    except Exception:
      rainfalllasthour = None

    try:
      rainfalllast24hour = DataQuerent(NodePathParser()).query(bufr_message, '013023').get_values(0)[0]
    except Exception:
      rainfalllast24hour = None

    try:
      winddirectiondegrees = DataQuerent(NodePathParser()).query(bufr_message, '011001').get_values(0)[0]
    except Exception:
      winddirectiondegrees = None

    try:
      pressure = DataQuerent(NodePathParser()).query(bufr_message, '010051').get_values(0)[0] / 100
    except Exception:
      pressure = None

    try:
      cloudcover = DataQuerent(NodePathParser()).query(bufr_message, '020010').get_values(0)[0]
    except Exception:
      cloudcover = None

    try:
      cloudbase = DataQuerent(NodePathParser()).query(bufr_message, '020013').get_values(0)[0]
    except Exception:
      cloudbase = None

    try:
      sunpower = int((DataQuerent(NodePathParser()).query(bufr_message, '014030').get_values(0)[0][0][0]/3600))
    except Exception:
      sunpower = None

    dictionary = {
        "stationname":stationname,
        "lat":lat,
        "lon":lon,
        "timestamp":timestamp,
        "temperature":temperature,
        "dewpointtemperature":dewpointtemperature,
        "visibility":visibility,
        "windgusts":windgusts,
        "windspeed":windspeed,
        "winddirectiondegrees":winddirectiondegrees,
        "humidity":humidity,
        "precipitation":rainfalllasthour,
        "rainfalllasthour":rainfalllasthour,
        "rainfalllast24hour":rainfalllast24hour,
        "pressure":pressure,
        "cloudcover":cloudcover,
        "cloudbase":cloudbase,
        "sunpower":sunpower
    }

    return dictionary

  def get_kmi_bufr(self,timestamp):
    #get bufr filelist
    year = timestamp[0:4]
    month = timestamp[5:7]
    day = timestamp[8:10]
    hour = timestamp[11:13]
    url = f'https://opendata.meteo.be/ftp/observations/synop/{year}/{month}/{day}/{hour}/'
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')
    filelist = [url + node.get('href') for node in soup.find_all('a') if node.get('href').endswith('bufr')]
    print(f"{len(filelist)} weatherstations found")

    df2 = self.create_kmi_df(timestamp)
    for file_url in filelist:
      filename = file_url[-26:]
      stationid = int(file_url[-20:-16])
      local_path = Path(filename)
      dataset_file_response = requests.get(file_url)
      local_path.write_bytes(dataset_file_response.content)
      print(f"{filename} downloaded")

      dictionary = self.bufr_to_dict(filename, timestamp)
      df2.at[stationid] = pd.Series(dictionary)

    return df2

  def run(self,timestamp):
    df_csv = self.get_kmi_csv(timestamp)
    df_bufr  = self.get_kmi_bufr(timestamp)
    df_merged = df_csv.combine_first(df_bufr)
    df_merged = df_merged.reset_index()
    df = df_merged.replace({np.nan: None})

    return df.to_dict('index')

  def cleanup_local_files(self):
    for filename in Path(".").glob("*.bufr"):
      filename.unlink()

if __name__ =='__main__':
  appService = AppService()
  timestamp = '2023-01-19T07:00:00'
  df = appService.run(timestamp)