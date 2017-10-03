
# from pyhive import hive
# from TCLIService.ttypes import TOperationState

from metadata.metadata_util import execute_query


class MetadataHiveIngestor:


    def ingestMetadata(self, optype_map, ingestion_param):

        #### Executing hive through pyhive ####
        # cursor = hive.connect('localhost').cursor()
        # complete_query = "hive -e 'CREATE TABLE ARA(i STRING)'"
        # cursor.execute(complete_query, async=True)

        for each in optype_map:

            if each in ingestion_param:
                complete_query = "hive -e 'INSERT INTO TABLE dev_bd_pilot.OPERATIONAL_METADATA values(\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\")'" \
                                                                                                    % (optype_map[each].op_name,
                                                                                                       optype_map[each].job_type,
                                                                                                       optype_map[each].source_entity_name,
                                                                                                       optype_map[each].source_type,
                                                                                                       optype_map[each].source_system,
                                                                                                       optype_map[each].source_path,
                                                                                                       optype_map[each].source_schema_name,
                                                                                                       optype_map[each].target_entity_name,
                                                                                                       optype_map[each].target_type,
                                                                                                       optype_map[each].target_path,
                                                                                                       optype_map[each].target_path,
                                                                                                       optype_map[each].target_schema_name,
                                                                                                       optype_map[each].target_system)


                print complete_query

                execute_query(complete_query)

        return


    def ingestOperationalData(self, data):
        complete_query = "hive -e 'INSERT INTO TABLE dev_bd_pilot.OPERATIONAL_DATA values(\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\")'" \
                         % (data.op_name,
                            data.process_id,
                            data.op_start_time_stamp,
                            data.op_end_time_stamp,
                            data.op_status,
                            data.op_owner,
                            data.record_count,
                            data.op_parent_process_name)

        print complete_query

        execute_query(complete_query)