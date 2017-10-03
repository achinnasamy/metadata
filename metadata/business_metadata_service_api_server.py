from flask import Flask
app = Flask(__name__)

@app.route("/")
def rootBusinessDatat():
    return "Business Metadata-Ingestion Home Page"


@app.route("/update")
def updateBusinessData():
    return "Data-Ingestion update"


@app.route("/receive")
def receiveBusinessData():
    return "Data-Ingestion Started"


