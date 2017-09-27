
from pyhive import hive
from TCLIService.ttypes import TOperationState



class MetadataHiveIngestor:


    def ingestMetadata(self, metadata):

        cursor = hive.connect('localhost').cursor()


        complete_query = "INSERT INTO TABLE dev_bd_pilot.OPERATIONAL_METADATA values(%s,%s,%s)" % (metadata.op_name, metadata.source_type, metadata.source_schema_name)

        print complete_query
        cursor.execute(complete_query, async=True)




        return True