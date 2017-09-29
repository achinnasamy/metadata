from metadata.metadata_hive import MetadataHiveIngestor
from metadata.metadata_util import MetadataCleanerService, execute_hdfs
from metadata.metadata_validator import MetaDataValidator

from metadata.metadata_xml_parser import MetadataXMLParser, XMLValidator


class MetaDataReckoner:


    def fetchMetaDataFromHDFS(self):
        file_content_hdfs = execute_hdfs("metrolinx.xml")

        file = open("/tmp/metadata.xml", "w")
        file.write(file_content_hdfs)
        file.close()

        return

    # XSD Validation check for XML
    def runMetadataXSDValidation(self):
        meta_data_validator = MetaDataValidator()
        return meta_data_validator.validateXMLUsingXSD("xml/metrolinx.xsd", "xml/metrolinx.xml")


    def runMetaDataReckoner(self):

        meta_data_validator = MetaDataValidator()
        return meta_data_validator.validateInput()



def start_main(argv):

    # Retrieve the XML file from HDFS
    metadataReckoner = MetaDataReckoner()
    metadataReckoner.fetchMetaDataFromHDFS()

    # If the metadata XSD validation passes, then proceed further
    if metadataReckoner.runMetadataXSDValidation():

        # Parse the XML and fetch all the data
        metadataParser = MetadataXMLParser()
        metadatavalue = metadataParser.parseXML()


        # Validate the contents of XML
        xml_validator = XMLValidator()
        did_xml_validation_succeed = xml_validator.validateXMLData(metadatavalue)


        # If the XML validation is success, then go ahead.
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
        print "\n Something miserable happened !!!! Metadata XSD validation failed."



