
from xml.dom.minidom import parse
import xml.dom.minidom
import xml.etree.ElementTree as ET

#from metadata.metadata_enum import OPTYPE
from metadata.metadata_util import MetadataValue, execute_hdfs


class MetadataXMLParser:


    def parseXML(self):

        metadatavalue = MetadataValue()

        # DOMTree = xml.dom.minidom.parse("xml/metrolinux_metadatav2.xml")
        # collection = DOMTree.documentElement
        #
        # if collection.hasAttribute("signal"):
        #    print "Root element : %s" % collection.getAttribute("signal")
        #
        #    if (collection.getAttribute("signal") == 'metrolinx'):
        #         metadata_entity = collection.getElementsByTagName("data")
        #
        #         print metadata_entity.item(0).text

                    # if  me.hasAttribute("entityname"):
                    #
                    #    type = me.getElementsByTagName('op_type')[0]
                    #    metadatavalue.op_type = type.childNodes[0].data
                    #
                    #    type = me.getElementsByTagName('op_name')[0]
                    #    metadatavalue.op_name = type.childNodes[0].data
                    #
                    #    type = me.getElementsByTagName('target_path')[0]
                    #    metadatavalue.target_path = type.childNodes[0].data


        file_content_hdfs = execute_hdfs("metrolinx_metadata.xml")





        #from time import sleep
        #sleep(15)


        file = open("/tmp/testfile.xml", "w")
        file.write(file_content_hdfs)
        file.close()

        #tree = ET.parse('xml/metrolinux_metadatav2.xml')
        tree = ET.parse('/tmp/testfile.xml')
        #tree = ET.fromstring(file_content_hdfs)

        root = tree.getroot()


        for data in root.findall('data'):
            for mt in data.findall('metadata_type'):
                    #print mt.attrib
                for operations in mt.findall('operations'):
                    for optype in operations.findall('op_type'):

                        if (optype.get('name') == "transformation"):
                            for att in optype.findall('attribute'):

                                for each_element in list(att):

                                    if (each_element.tag == "op_name"):
                                        metadatavalue.op_name = each_element.text

                                    elif (each_element.tag == "job_type"):
                                        metadatavalue.job_type = each_element.text

                                    elif (each_element.tag == "source_type"):
                                        metadatavalue.source_type = each_element.text

                                    elif (each_element.tag == "source_schema_name"):
                                        metadatavalue.source_schema_name = each_element.text

                                    elif (each_element.tag == "source_location"):
                                        metadatavalue.source_location = each_element.text

                                    elif (each_element.tag == "source_entity_name"):
                                        metadatavalue.source_entity_name = each_element.text

                                    elif (each_element.tag == "source_path"):
                                        metadatavalue.source_path = each_element.text

                                    elif (each_element.tag == "target_type"):
                                        metadatavalue.target_type = each_element.text

                                    elif (each_element.tag == "target_schema_name"):
                                        metadatavalue.target_schema_name = each_element.text

                                    elif (each_element.tag == "target_entity_name"):
                                        metadatavalue.target_entity_name = each_element.text

                                    elif (each_element.tag == "target_system"):
                                        metadatavalue.target_system = each_element.text

                                    elif (each_element.tag == "target_path"):
                                        metadatavalue.target_path = each_element.text


        return metadatavalue


class XMLValidator:

    def validateXMLData(self, metadata_value):


         # if (metadata_value.op_type == OPTYPE.INGESTION.get_value()):
         #    print "ha ha"
        # else:
        #     print "sorry"

        return

