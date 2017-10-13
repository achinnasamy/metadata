from metadata.hdfs_manager import HDFSManager
from metadata.metadata_hive import MetadataHiveIngestor
from metadata.metadata_xml_parser import MetadataXMLParser


def start_main():

    print "\n\nTechnical Metadata Ingestion started..."

    hdfsManager = HDFSManager()
    hdfsManager.fetchMetaDataFromHDFS()

    metadataParser = MetadataXMLParser()
    technical_metadata_list = metadataParser.parseTechnicalXMLData()

    hiveIngestor = MetadataHiveIngestor()

    #hiveIngestor.ingestTechnicalMetadata(technical_metadata_list)

    hiveIngestor.ingestTechnicalMetadataToCSV(technical_metadata_list)

    return
