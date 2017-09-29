import subprocess

from metadata.metadata_hive import MetadataHiveIngestor
from metadata.metadata_util import get_pid, get_current_time, MetadataJobDetailComputingManager, MetadataCleanerService
from metadata.metadata_validator import MetaDataValidator, MetaDataInputArgumentValidator

import sys,getopt

from metadata.metadata_xml_parser import MetadataXMLParser, XMLValidator


class MetaDataReckoner:


    def runMetaDataReckoner(self):

        meta_data_input_argument_validator = MetaDataInputArgumentValidator()

        if meta_data_input_argument_validator.validateCommandLineArguments() == True:
            print "True True True"

        meta_data_validator = MetaDataValidator()
        meta_data_validator.validateInput()
        return



def start_main(argv):

    metadataReckoner = MetaDataReckoner()
    metadataReckoner.runMetaDataReckoner()


    # Parse the XML and fetch all the data
    metadataParser = MetadataXMLParser()
    metadatavalue = metadataParser.parseXML()


    # Validate the contents of XML
    xml_validator = XMLValidator()
    xml_validator.validateXMLData(metadatavalue)




    # Should the validation return true, then ingest in Hive
    hiveIngestor = MetadataHiveIngestor()
    #hiveIngestor.ingestMetadata(metadatavalue)


    cleanup_service = MetadataCleanerService()
    cleanup_service.cleanFiles()

    print "Metadata Reckoner Done."

