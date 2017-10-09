def create_hive_tables():


        HIVE_SCRIPTS_DIRECTORY = "hive_scripts/"
        hive_create_table_script = []

        import os
        list_of_files = os.listdir(HIVE_SCRIPTS_DIRECTORY)

        for each_file in list_of_files:
            with open(HIVE_SCRIPTS_DIRECTORY+each_file, 'r') as content_file:
                content = content_file.read()
                hive_create_table_script.append(content)


        for each_script in hive_create_table_script:
            complete_hive_query = "hive -e '%s'" % each_script
            print complete_hive_query

            import subprocess as sp
            result = sp.Popen(complete_hive_query, shell=True, stdout=sp.PIPE, stderr=sp.PIPE).communicate()[0]

        return True


def start_main(argv):
    create_hive_tables()