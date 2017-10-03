
from xml.dom.minidom import parse
import xml.dom.minidom
import xml.etree.ElementTree as ET

#from metadata.metadata_enum import OPTYPE
from metadata.metadata_util import MetadataValue, execute_hdfs, TEMP_XML_FILE_LOCATION


class MetadataXMLParser:


    def parseXML(self):


        optype_map = {}

        tree = ET.parse(TEMP_XML_FILE_LOCATION)

        root = tree.getroot()


        for data in root.findall('data'):
            for mt in data.findall('metadata_type'):
                    #print mt.attrib
                for operations in mt.findall('operations'):

                    for optype in operations.findall('op_type'):

                        if (optype.get('name') == "transformation"):
                                metadatavalue = MetadataValue()
                                for job in optype:
                                    self.populate_metadata_value(metadatavalue, job)
                                optype_map['transformation'] = metadatavalue

                        if (optype.get('name') == "ingestion"):
                                metadatavalue = MetadataValue()
                                for job in optype:
                                    self.populate_metadata_value(metadatavalue, job)
                                optype_map['ingestion'] = metadatavalue

                        if (optype.get('name') == "curation"):
                                metadatavalue = MetadataValue()
                                for job in optype:
                                    self.populate_metadata_value(metadatavalue, job)
                                optype_map['curation'] = metadatavalue

                        if (optype.get('name') == "consumption"):
                                metadatavalue = MetadataValue()
                                for job in optype:
                                    self.populate_metadata_value(metadatavalue, job)
                                optype_map['consumption'] = metadatavalue


        return optype_map



    # Populate the metadatavalue DTO with the tags from xml
    def populate_metadata_value(self, metadatavalue, job):
        metadatavalue.op_name = job.get("op_name")
        metadatavalue.job_type = job.get("job_type")
        metadatavalue.source_type = job.get("source_type")
        metadatavalue.source_schema_name = job.get("source_schema_name")
        metadatavalue.source_system = job.get("source_system")
        metadatavalue.source_entity_name = job.get("source_entity_name")
        metadatavalue.source_path = job.get("source_path")
        metadatavalue.target_type = job.get("target_type")
        metadatavalue.target_schema_name = job.get("target_schema_name")
        metadatavalue.target_entity_name = job.get("target_entity_name")
        metadatavalue.target_system = job.get("target_system")
        metadatavalue.target_path = job.get("target_path")
        return


    #
    # Fetch the opname for the corresponding optypes
    # TODO : Fetch the opname from XML
    #
    def fetch_opname_from_xml(self, list_of_op_types):

        optype_opname = {}

        tree = ET.parse(TEMP_XML_FILE_LOCATION)

        root = tree.getroot()

        for data in root.findall('data'):
            for mt in data.findall('metadata_type'):
                for operations in mt.findall('operations'):

                    for optype in operations.findall('op_type'):

                        if (optype.get('name') == "transformation"):
                            optype_opname['transformation']

        return


class XMLValidator:

    # TODO : Validate the xml data at a later point
    def validateXMLData(self, optype_metadata_map):


        return True

