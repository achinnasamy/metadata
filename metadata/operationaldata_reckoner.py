from metadata.metadata_hive import MetadataHiveIngestor
from metadata.metadata_util import MetadataJobDetailComputingManager


def start_main(argv):

        print " \n\n\n Operational Data ingestion started..."

        computing_manager = MetadataJobDetailComputingManager()
        data = computing_manager.fetchJOBDetail()


        hiveIngestor = MetadataHiveIngestor()
        hiveIngestor.ingestOperationalData(data)


        print " \n\n\n Operational Data ingestion done..."
