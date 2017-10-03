from metadata.hdfs_manager import HDFSManager
from metadata.metadata_hive import MetadataHiveIngestor
from metadata.metadata_util import MetadataJobDetailComputingManager


def start_main(status, ingestion_param):

        print " \n\n\n Operational Data ingestion started..."

        hdfsManager = HDFSManager()
        hdfsManager.fetchMetaDataFromHDFS()

        computing_manager = MetadataJobDetailComputingManager()
        data = computing_manager.fetchJOBDetail(status)


        hiveIngestor = MetadataHiveIngestor()
        hiveIngestor.ingestOperationalData(data)


        print " \n\n\n Operational Data ingestion done."
