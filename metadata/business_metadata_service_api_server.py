from flask import Flask, request
import xml.etree.ElementTree as ET

from metadata.metadata_util import write_file_to_hdfs

app = Flask(__name__)

@app.route("/")
def rootBusinessDatat():
    return "Business Metadata-Ingestion Home Page"


@app.route("/update", methods=['GET', 'POST'])
def updateBusinessData():

    LOCAL_XML_FILE_PATH = "/tmp/new.xml"
    HDFS_XML_PATH = "/"

    xml = request.data

    file = open("../xml/metrolinux_metadata_static.xml", "r")
    file_contents = file.read()

    new_xml_file_contents = file_contents % xml

    file.close()

    # TODO: Need to delete the file after writing
    file = open(LOCAL_XML_FILE_PATH, "w")
    file.write(new_xml_file_contents)
    file.close()

    write_file_to_hdfs(LOCAL_XML_FILE_PATH, HDFS_XML_PATH)

    return "0"


@app.route("/receive")
def receiveBusinessData():
    return "Data-Ingestion Started"



# class RestXMLParser:
#
#     def insertBusinessTagToXML(self, element_to_be_inserted):
#
#         tree = ET.parse("../xml/metrolinx_metadata_static.xml")
#
#         root = tree.getroot()
#
#         data = root.findall('data')
#         data.insert(0, element_to_be_inserted)
#
#         new_tree = ET.ElementTree(root)
#         new_tree.write("/home/user/ac/a.xml")


if __name__ == '__main__':
    app.run(host= '0.0.0.0',debug=True)

