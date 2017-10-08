
from xml.dom.minidom import parse
import xml.dom.minidom
import xml.etree.ElementTree as ET

#from metadata.metadata_enum import OPTYPE
from metadata.metadata_util import MetadataValue, execute_hdfs, TEMP_XML_FILE_LOCATION, get_current_time


class MetadataXMLParser:


    def parseBusinessXMLData(self):

        business_metadata_list = []

        tree = ET.parse(TEMP_XML_FILE_LOCATION)

        root = tree.getroot()


        for data in root.findall('data'):
            for mt in data.findall('metadata_type'):

                # Parsing of Business metadata type
                if (mt.get('name') == "business"):
                    for entity in mt.findall('entity'):

                        for field in entity.findall('field'):
                            metadatavalue = MetadataValue()
                            metadatavalue.business_unit = entity.get('business_unit')
                            metadatavalue.entity_name = entity.get('name')
                            metadatavalue.entity_business_definition = entity.get('entity_business_definition')
                            metadatavalue.field_name = field.get('name')
                            metadatavalue.field_type = field.get('field_type')
                            metadatavalue.field_business_definition = field.get('field_business_definition')
                            metadatavalue.time_stamp = get_current_time()

                            business_metadata_list.append(metadatavalue)

        return business_metadata_list


    def parseTechnicalXMLData(self):

        technical_metadata_list = []

        tree = ET.parse(TEMP_XML_FILE_LOCATION)

        root = tree.getroot()


        for data in root.findall('data'):
            for mt in data.findall('metadata_type'):

                # Parsing of Business metadata type
                if (mt.get('name') == "technical"):
                    for entity in mt.findall('entity'):

                        for field in entity.findall('field'):
                            metadatavalue = MetadataValue()

                            metadatavalue.entity_name = entity.get('name')
                            metadatavalue.entity_comment = entity.get('entity_comment')
                            metadatavalue.field_name = field.get('name')
                            metadatavalue.field_type = field.get('field_type')
                            metadatavalue.field_length = field.get('field_length')
                            metadatavalue.field_comment = field.get('field_comment')
                            metadatavalue.field_precision = field.get('field_precision')
                            metadatavalue.field_format = field.get('field_format')
                            metadatavalue.time_stamp = get_current_time()

                            technical_metadata_list.append(metadatavalue)

        return technical_metadata_list


    def parseXML(self):


        optype_map = {}

        tree = ET.parse(TEMP_XML_FILE_LOCATION)

        root = tree.getroot()


        for data in root.findall('data'):
            for mt in data.findall('metadata_type'):

                # Parsing of Technical metadata type
                if (mt.get('name') == "technical"):
                    for entity in mt.findall('entity'):

                        technical_data_map = {}
                        technical_data_map["table_name"] = entity.get('name')
                        for column in entity.findall('column'):
                            technical_data_map[column.get("name")] = entity.get('data_type')



                # Parsing of Operation metadata type
                for operations in mt.findall('operations'):

                    for optype in operations.findall('op_type'):

                        if (optype.get('name') == "integration"):
                                metadatavalue = MetadataValue()
                                metadatavalue.op_type = "integration"
                                for job in optype:
                                    self.populate_metadata_value(metadatavalue, job)
                                optype_map['integration'] = metadatavalue

                        if (optype.get('name') == "ingestion"):
                                metadatavalue = MetadataValue()
                                metadatavalue.op_type = "ingestion"
                                for job in optype:
                                    self.populate_metadata_value(metadatavalue, job)
                                optype_map['ingestion'] = metadatavalue

                        if (optype.get('name') == "curation"):
                                metadatavalue = MetadataValue()
                                metadatavalue.op_type = "curation"
                                for job in optype:
                                    self.populate_metadata_value(metadatavalue, job)
                                optype_map['curation'] = metadatavalue

                        if (optype.get('name') == "consumption"):
                                metadatavalue = MetadataValue()
                                metadatavalue.op_type = "consumption"
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

