from flask import Flask, request
import xml.etree.ElementTree as ET
app = Flask(__name__)

@app.route("/")
def rootBusinessDatat():
    return "Business Metadata-Ingestion Home Page"


@app.route("/update", methods=['GET', 'POST'])
def updateBusinessData():

    xml = request.data

    file = open("../xml/metrolinux_metadata_static.xml", "r")
    file_contents = file.read()

    new_xml_file_contents = file_contents % xml

    file.close()

    file = open("/home/dharshekthvel/NEW.xml", "w")
    file.write(new_xml_file_contents)
    file.close()

    return "0"


@app.route("/receive")
def receiveBusinessData():
    return "Data-Ingestion Started"



class RestXMLParser:

    def insertBusinessTagToXML(self, element_to_be_inserted):

        tree = ET.parse("../xml/metrolinux_metadata_static.xml")

        root = tree.getroot()

        data = root.findall('data')
        data.insert(0, element_to_be_inserted)

        new_tree = ET.ElementTree(root)
        new_tree.write("/home/dharshekthvel/ac/a.xml")


if __name__ == '__main__':
    app.run(host= '0.0.0.0',debug=True)

