from metadata.hdfs_manager import HDFSManager
from metadata.metadata_hive import MetadataHiveIngestor
from metadata.metadata_reckoner import MetaDataReckoner
from metadata.metadata_util import MetadataJobDetailComputingManager
from metadata.metadata_xml_parser import MetadataXMLParser


def start_main(status, ingestion_param):

        print " \n\n\n Operational Data ingestion started..."

        hdfsManager = HDFSManager()
        hdfsManager.fetchMetaDataFromHDFS()

        computing_manager = MetadataJobDetailComputingManager()
        data = computing_manager.fetchJOBDetail(status)

        ###########################################################################################

        # FOR THE OP_NAME, fetch the op_name from xml

        # Retrieve the XML file from HDFS
        metadataReckoner = MetaDataReckoner()
        metadataReckoner.fetchXMLFromHDFS()


        # Parse the XML and fetch all the data
        metadataParser = MetadataXMLParser()
        optype_metadata_map = metadataParser.parseXML()

        data.op_name = optype_metadata_map[ingestion_param[0]].op_name
        ###########################################################################################

        hiveIngestor = MetadataHiveIngestor()
        hiveIngestor.ingestOperationalData(data)


        print " \n\n\n Operational Data ingestion done."
