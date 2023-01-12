from bs4 import BeautifulSoup
import requests
from pathlib import Path
import json
import pybufrkit
from pybufrkit.decoder import Decoder
from pybufrkit.dataquery import NodePathParser, DataQuerent

class AppService:
  def run(self,timestamp):
    year = timestamp[0:4]
    month = timestamp[5:7]
    day = timestamp[8:10]
    hour = timestamp[11:13]
    url = f'https://opendata.meteo.be/ftp/observations/synop/{year}/{month}/{day}/{hour}/'
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')
    filelist = [url + node.get('href') for node in soup.find_all('a') if node.get('href').endswith('bufr')]
    print(f"{len(filelist)} weatherstations found")

    dlist = []
    for file_url in filelist:
      filename = file_url[-26:]
      local_path = Path(filename)

      # Get file
      dataset_file_response = requests.get(file_url)

      # Write dataset file to disk
      local_path.write_bytes(dataset_file_response.content)

      print(f"{filename} downloaded")

      dictionary = self.bufr_to_dict(filename, timestamp)

      dlist.append(dictionary)

    # Serializing json
    d = {"weatherstations":dlist}

    return d

  def bufr_to_dict(self, filename, timestamp):
    # Decode a BUFR file
    decoder = Decoder()
    with open(filename, 'rb') as ins:
      bufr_message = decoder.process(ins.read())

    try:
      stationid = f"6{DataQuerent(NodePathParser()).query(bufr_message, '001002').get_values(0)[0]}"
    except Exception:
      stationid = None

    try:
      stationname = f"Meetstation {DataQuerent(NodePathParser()).query(bufr_message, '001015').get_values(0)[0].decode('utf-8')}"
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
        "stationid":stationid,
        "stationname":stationname,
        "lat":lat,
        "lon":lon,
        "regio":regio,
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

  def cleanup_local_files(self):
    for filename in Path(".").glob("*.bufr"):
       filename.unlink()

from flask import Flask, jsonify

app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False
appService = AppService()

@app.route('/')
def home():
    return "Please specify /api/json/<timestamp> (ex. /api/json/2023-01-12T07:00:00)"

@app.route("/api/json/<timestamp>", methods=["GET"])
def get_kmi_json(timestamp):
    appService.cleanup_local_files()
    return jsonify(appService.run(timestamp))