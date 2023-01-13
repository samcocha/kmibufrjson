from flask import Flask, jsonify
from app_service import AppService

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