import subprocess

from metadata.metadata_hive import MetadataHiveIngestor
from metadata.metadata_util import get_pid, get_current_time, MetadataJobDetailComputingManager, MetadataCleanerService
from metadata.metadata_validator import MetaDataValidator

import sys,getopt

from metadata.metadata_xml_parser import MetadataXMLParser, XMLValidator


class MetaDataReckoner:

    def runMetaDataReckoner(self):

        meta_data_validator = MetaDataValidator()
        return meta_data_validator.validateInput()



def start_main(argv):

    metadataReckoner = MetaDataReckoner()

    # If the metadata validation passes, then proceed further
    if metadataReckoner.runMetaDataReckoner():

        # Parse the XML and fetch all the data
        metadataParser = MetadataXMLParser()
        metadatavalue = metadataParser.parseXML()


        # Validate the contents of XML
        xml_validator = XMLValidator()
        did_xml_validation_succeed = xml_validator.validateXMLData(metadatavalue)



        if did_xml_validation_succeed:

            # Should the validation return true, then ingest in Hive
            hiveIngestor = MetadataHiveIngestor()
            #hiveIngestor.ingestMetadata(metadatavalue)

            cleanup_service = MetadataCleanerService()
            cleanup_service.cleanFiles()

            print "Metadata Reckoner Done."

        else:
            print "\n\n\n So Sorry !!!! XML Validation validation failed."

    else:
        print "\n Something miserable happened !!!! Metadata validation failed."



