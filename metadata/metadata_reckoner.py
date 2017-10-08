from metadata.hdfs_manager import HDFSManager
from metadata.metadata_hive import MetadataHiveIngestor
from metadata.metadata_util import MetadataCleanerService, execute_hdfs
from metadata.metadata_validator import MetaDataValidator
from metadata.metadata_xml_parser import MetadataXMLParser, XMLValidator


class MetaDataReckoner:

    # XSD Validation check for XML
    # TODO: XSD validation is disabled for now. Enable it later
    def runMetadataXSDValidation(self):
        meta_data_validator = MetaDataValidator()
        return meta_data_validator.validateXMLUsingXSD("xml/metrolinx.xsd", "xml/metrolinx.xml")


    def runMetaDataReckoner(self):

        meta_data_validator = MetaDataValidator()
        return meta_data_validator.validateInput()


    def fetchXMLFromHDFS(self):

        hdfs_manager = HDFSManager()
        #hdfs_manager.fetch_the_recent_xml_file_from_hdfs()
        return hdfs_manager.fetch_all_xml_files_from_hdfs("")



def start_main(ingestion_param):

    # Retrieve the XML file from HDFS
    metadataReckoner = MetaDataReckoner()
    #metadataReckoner.fetchXMLFromHDFS()


    hdfsManager = HDFSManager()
    hdfsManager.fetchMetaDataFromHDFS()


    # If the metadata XSD validation passes, then proceed further
    # TODO: XSD validation is disabled for now. Enable it later
    if metadataReckoner.runMetadataXSDValidation():

        # Parse the XML and fetch all the data
        metadataParser = MetadataXMLParser()
        optype_metadata_map = metadataParser.parseXML()


        # Validate the contents of XML
        xml_validator = XMLValidator()
        did_xml_validation_succeed = xml_validator.validateXMLData(optype_metadata_map)


        # If the XML validation is success, then go ahead.
        if did_xml_validation_succeed:

            print "\n\n\n Metadata Reckoner Ingestion Started."

            # Should the validation return true, then ingest in Hive
            hiveIngestor = MetadataHiveIngestor()
            hiveIngestor.ingestMetadata(optype_metadata_map, ingestion_param)

            cleanup_service = MetadataCleanerService()
            cleanup_service.cleanFiles()


            print "\n\n\n Metadata Reckoner Ingestion Done."

        else:
            print "\n\n\n So Sorry !!!! XML validation failed."

    else:
        print "\n Something miserable happened !!!! Metadata XSD validation failed."



