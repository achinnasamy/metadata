from metadata.hdfs_manager import HDFSManager
from metadata.metadata_hive import MetadataHiveIngestor
from metadata.metadata_xml_parser import MetadataXMLParser


def start_main():

    print "\n\nBusiness Metadata Ingestion started..."

    hdfsManager = HDFSManager()
    hdfsManager.fetchMetaDataFromHDFS()

    metadataParser = MetadataXMLParser()
    business_metadata_list = metadataParser.parseBusinessXMLData()

    hiveIngestor = MetadataHiveIngestor()

    hiveIngestor.ingestBusinessMetadata(business_metadata_list)
    #hiveIngestor.ingestBusinessMetadataToCSV(business_metadata_list)


    return