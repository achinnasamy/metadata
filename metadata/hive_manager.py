from metadata.metadata_util import execute_query




    #
    # Create run scripts for Hive
    #
def runCreateQuery(self):

        create_database = 'CREATE DATABASE IF NOT EXISTS dev_bd_pilot'

        create_metadata = "CREATE TABLE IF NOT EXISTS dev_bd_pilot.OPERATIONAL_METADATA(op_name STRING," \
                          "job_type STRING,source_entity_name STRING, source_type STRING,source_location STRING," \
                                    "source_path STRING, origin_system STRING, source_schema_name STRING, target_entity_name STRING," \
                                    "target_type STRING, target_location STRING, target_path STRING, target_schema_name STRING," \
                                    "target_system STRING) COMMENT 'Metadata Details' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'" \
                                "STORED AS TEXTFILE;"


        execute_query(create_database)
        execute_query(create_database)
        return True


def start_main(argv):
    runCreateQuery()