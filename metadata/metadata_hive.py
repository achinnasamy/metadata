
# from pyhive import hive
# from TCLIService.ttypes import TOperationState

from metadata.metadata_util import execute_query


class MetadataHiveIngestor:


    def ingestMetadata(self, metadata):


        # Executing hive through pyhive
        # cursor = hive.connect('localhost').cursor()
        # complete_query = "hive -e 'CREATE TABLE ARA(i STRING)'"
        # cursor.execute(complete_query, async=True)



        complete_query = "hive -e 'INSERT INTO TABLE dev_bd_pilot.OM values(\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\")'" \
                                                                                                % (metadata.op_name,
                                                                                                   metadata.job_type,
                                                                                                   metadata.source_entity_name,
                                                                                                   metadata.source_type,
                                                                                                   metadata.source_location,
                                                                                                   metadata.source_path,
                                                                                                   metadata.source_location,
                                                                                                   metadata.source_schema_name,
                                                                                                   metadata.target_entity_name,
                                                                                                   metadata.target_type,
                                                                                                   metadata.target_path,
                                                                                                   metadata.target_path,
                                                                                                   metadata.target_schema_name,
                                                                                                   metadata.target_system)


        print complete_query

        execute_query(complete_query)


    def ingestOperationalData(self, data):
        complete_query = "hive -e 'INSERT INTO TABLE dev_bd_pilot.OPERATIONAL_DATA values(\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\")'" \
                         % (data.op_name,
                            data.process_id,
                            data.op_start_time_stamp,
                            data.op_end_time_stamp,
                            data.op_eta,
                            data.op_status,
                            data.op_owner,
                            data.record_count,
                            data.op_parent_process_name)

        print complete_query