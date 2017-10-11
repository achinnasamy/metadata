
# from pyhive import hive
# from TCLIService.ttypes import TOperationState

from metadata.metadata_util import execute_query, execute_query_and_fetch_output, isNotBlank


class MetadataHiveIngestor:

    #
    #
    #
    def ingestBusinessMetadata(self, business_metadata_list):

        for each in business_metadata_list:

            complete_query = "hive -e 'INSERT INTO TABLE dev_bd_pilot.bdpilot_business_metadata values(\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\")'" % (each.business_unit,
                                                                                                                                                each.entity_name,
                                                                                                                                                each.entity_business_definition,
                                                                                                                                                each.field_name,
                                                                                                                                                each.field_type,
                                                                                                                                                each.field_business_definition,
                                                                                                                                                each.date_modified)

            print complete_query

            execute_query(complete_query)


        #self.createBusinessMetadataTable(business_metadata_list)
        return


    #
    #
    #
    def createBusinessMetadataTable(self, business_metadata_list):

        table_name = business_metadata_list[0].entity_name
        field_names = ""
        for each in business_metadata_list:
            field_names += "%s %s," % (each.field_name.upper(), each.field_type.upper())

        query = "hive -e 'CREATE TABLE IF NOT EXISTS %s(%s) COMMENT 'Technical Metadata' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n' STORED AS TEXTFILE'" % (table_name, field_names[:-1])

        print query

        return


    #
    #
    #
    def ingestTechnicalMetadata(self, technical_metadata_list):


        for each in technical_metadata_list:

            complete_query = "hive -e 'INSERT INTO TABLE dev_bd_pilot.bdpilot_technical_metadata values(\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\")'" % (each.entity_name,
                                                                                                                                                                        each.entity_comment,
                                                                                                                                                                        each.field_name,
                                                                                                                                                                        each.field_type,
                                                                                                                                                                        each.field_comment,
                                                                                                                                                                        each.field_length,
                                                                                                                                                                        each.field_precision,
                                                                                                                                                                        each.field_format,
                                                                                                                                                                        each.date_modified)

            print complete_query

            execute_query(complete_query)

        self.createTechnicalMetadataTable(technical_metadata_list)
        return

        #
        #
        #
    def createTechnicalMetadataTable(self, technical_metadata_list):

            table_name = technical_metadata_list[0].entity_name
            field_names = ""
            for each in technical_metadata_list:
                field_names += "%s %s," % (each.field_name.upper(), each.field_type.upper())

            query = "hive -e 'CREATE TABLE IF NOT EXISTS %s(%s) ROW FORMAT DELIMITED FIELDS TERMINATED BY \"\\t\" LINES TERMINATED BY \"\\n\" STORED AS TEXTFILE'" % (table_name, field_names[:-1])

            print query

            return


    def ingestMetadata(self, optype_map, ingestion_param):

        #### Executing hive through pyhive ####
        # cursor = hive.connect('localhost').cursor()
        # complete_query = "hive -e 'CREATE TABLE ARA(i STRING)'"
        # cursor.execute(complete_query, async=True)

        for each in optype_map:

            if (isNotBlank(optype_map[each].op_type)) and (isNotBlank(optype_map[each].op_name)) and isNotBlank(optype_map[each].script_type) \
                    and (isNotBlank(optype_map[each].script_location)) and (isNotBlank(optype_map[each].source_entity_name)) \
                    and (isNotBlank(optype_map[each].target_entity_name)) and (isNotBlank(optype_map[each].target_type)) \
                    and (isNotBlank(optype_map[each].target_path)) and (isNotBlank(optype_map[each].target_schema_name)) \
                    and (isNotBlank(optype_map[each].target_system)) and (isNotBlank(optype_map[each].date_modified)) \
                    and (isNotBlank(optype_map[each].source_type)) and (isNotBlank(optype_map[each].source_system)) \
                    and (isNotBlank(optype_map[each].source_path)) and (isNotBlank(optype_map[each].source_schema_name)):

                if each in ingestion_param:
                    print "\n\n Injesting " + each
                    complete_query = "hive -e 'INSERT INTO TABLE dev_bd_pilot.bdpilot_operational_metadata values(\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\")'" \
                                                                                                        % (optype_map[each].op_type,
                                                                                                           optype_map[each].op_name,
                                                                                                           optype_map[each].script_type,
                                                                                                           optype_map[each].script_location,
                                                                                                           optype_map[each].source_entity_name,
                                                                                                           optype_map[each].source_type,
                                                                                                           optype_map[each].source_system,
                                                                                                           optype_map[each].source_path,
                                                                                                           optype_map[each].source_schema_name,
                                                                                                           optype_map[each].target_entity_name,
                                                                                                           optype_map[each].target_type,
                                                                                                           optype_map[each].target_path,
                                                                                                           optype_map[each].target_schema_name,
                                                                                                           optype_map[each].target_system,
                                                                                                           optype_map[each].date_modified)


                    print complete_query

                    execute_query(complete_query)

        return


    def ingestOperationalData(self, data):


        complete_query = "hive -e 'INSERT INTO TABLE dev_bd_pilot.bdpilot_operational_data values(\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\")'" \
                         % (data.op_type,
                            data.op_name,
                            data.process_id,
                            data.op_start_time_stamp,
                            data.op_end_time_stamp,
                            data.op_status,
                            data.op_owner,
                            data.op_parent_process_name,
                            data.src_entity_name,
                            data.src_record_count,
                            data.target_entity_name,
                            data.target_record_count)

        print complete_query

        execute_query(complete_query)
