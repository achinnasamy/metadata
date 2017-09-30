
from xml.dom.minidom import parse
import xml.dom.minidom
import xml.etree.ElementTree as ET

#from metadata.metadata_enum import OPTYPE
from metadata.metadata_util import MetadataValue, execute_hdfs


class MetadataXMLParser:


    def parseXML(self):

        metadatavalue = MetadataValue()

        tree = ET.parse('/tmp/metadata.xml')

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

                        if (optype.get('name') == "ingestion"):
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

                        if (optype.get('name') == "curation"):
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

                        if (optype.get('name') == "consumption"):
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

        return True

