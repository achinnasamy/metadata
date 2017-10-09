
import time
import datetime
import subprocess as sp
import os
#import cx_Oracle





from metadata.oracle_data_manager import OracleDataManager

TEMP_XML_FILE_LOCATION = "/home/devuser/metrolinx.xml"
TEMP_CSV_FILE_LOCATION = "/home/devuser/"
HDFS_XML_FILE_LOCATION = "/user/devuser/metrolinx1.xml"

#
#  Gets the process id of java
#
# print get_pid('java')
def get_pid(name):
    #return check_output(["pidof",name])
    return

def get_current_time():
    ts = time.time()
    return datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d-%H:%M:%S')


def get_date():
    ts = time.time()
    return datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d')

#
# Find all xml files and put it in a list and send it back
#
def find_all_xml_files_in_hdfs(hdfs_dir_path):

    # To fetch the recent xml use the command - hdfs dfs -ls -C -t /metro*
    list_of_xml_files = []
    all_xml_files = sp.Popen(["hdfs", "dfs", "-ls", "-C", "/*.xml"], stdout=sp.PIPE).communicate()[0]

    for each_file in all_xml_files.splitlines():
        list_of_xml_files.append(each_file.replace("/",""))

    return list_of_xml_files



#
#
#
def get_process_id_of_application(application_name):
        #process_id = sp.Popen(["ps", "-aux", "|",  "grep", "-i", "hdfs", "|",  "awk" , "{'print $2'}"], stdout=sp.PIPE).communicate()[0]
        # list_of_processes = sp.Popen(["ps", "-aux"], stdout=sp.PIPE).communicate()[0]
        # for each_line in list_of_processes.splitlines():
        #     if (each_line.__contains__("hdfs")):
        #         print each_line.split(" ")[1]
        return



def execute_hdfs(filename):

    #result = sp.check_output('hdfs dfs -cat /metrolinux_metadatav2.xml')

    result = sp.Popen(["hdfs", "dfs", "-cat", HDFS_XML_FILE_LOCATION], stdout=sp.PIPE).communicate()[0]

    return result

#
# FILE_TO_BE_WRITTEN =
#
def write_file_to_hdfs(FILE_TO_BE_WRITTEN, HDFS_PATH):

    sp.Popen(["hdfs", "dfs", "-put", FILE_TO_BE_WRITTEN, HDFS_PATH], stdout=sp.PIPE).communicate()[0]

    return


def fetchRowCountFromCSV(CSV_HDFS_PATH):


        result = sp.Popen(["hdfs", "dfs", "-cat", CSV_HDFS_PATH], stdout=sp.PIPE).communicate()[0]
        no_of_rows = len(result.split('\n'))

        #import os
        #FILE_NAME = os.path.basename(CSV_HDFS_PATH)
        #file = open(TEMP_CSV_FILE_LOCATION+FILE_NAME, "w")
        #file.write(result)
        #file.close()

        return no_of_rows - 1


#
#
#
def fetchRowCountFromHiveTable(table_name):

        complete_query = "hive -e 'SELECT COUNT(*) FROM %s'" % (table_name)
        output = execute_query_and_fetch_output(complete_query)

        return output

#
# This is used to get the row count of managed table
#
def execute_query_and_fetch_output(query):

    print "\nFetching row count of : " + query
    result = sp.Popen(query, shell=True,stdout=sp.PIPE,stderr=sp.PIPE).communicate()[0]
    result = result.replace('\n\n', '')
    return result.split('\n', 1)[0]

def execute_query(query):

    sp.Popen(query, shell=True, stdout=sp.PIPE)
        #sp.call(query, shell=True)
    return

#
# Returns the current user name
#
def get_current_linux_user_name():
    import getpass
    return getpass.getuser()



def get_current_process_id():
    return os.getpid()
#
# Returns the parent process name
#
def get_parent_process_id():
    return os.getppid()

class MetadataValue:

    def __init__(self, **args):
        self.__dict__= args


class MetadataCleanerService:

    def cleanFiles(self):

        import os
        os.remove(TEMP_XML_FILE_LOCATION)
        return


class MetadataJobDetailComputingManager:

    # python metadata.egg
    def fetchJOBDetail(self, status, table, oracle_table_name, csv_file_name):

        metadatavalue = MetadataValue()

        get_process_id_of_application("")

        metadatavalue.op_name = ""  # op_name is populated at a later stage
        metadatavalue.process_id= get_current_process_id()

        metadatavalue.op_start_time_stamp = ''
        metadatavalue.op_end_time_stamp = ''
        metadatavalue.op_parent_process_name = ''
        if (status == "0"):
            metadatavalue.op_status = "STARTED"
            metadatavalue.op_start_time_stamp = get_current_time()

        elif (status == "1"):
            metadatavalue.op_status = "RUNNING"
        elif (status == "2"):
            metadatavalue.op_status = "SUCCESS"
            metadatavalue.op_end_time_stamp = get_current_time()


        metadatavalue.op_owner = get_current_linux_user_name()


        if (table != 'NONE'):
            metadatavalue.src_entity_name = "NONE"
            metadatavalue.src_record_count = 0
            metadatavalue.target_entity_name = table
            metadatavalue.target_record_count = fetchRowCountFromHiveTable(table)
        elif (oracle_table_name != 'NONE'):
            oracle = OracleDataManager()
            metadatavalue.src_entity_name = oracle_table_name
            metadatavalue.src_record_count = oracle.fetch_no_of_rows_from_oracle(oracle_table_name)
            metadatavalue.target_entity_name = "NONE"
            metadatavalue.target_record_count = 0
        if (csv_file_name != 'NONE'):
            metadatavalue.src_entity_name = csv_file_name
            metadatavalue.src_record_count = fetchRowCountFromCSV(csv_file_name)
            metadatavalue.target_entity_name = "NONE"
            metadatavalue.target_record_count = 0
        else:
            metadatavalue.src_entity_name = "NONE"
            metadatavalue.src_record_count = 0
            metadatavalue.target_entity_name = "NONE"
            metadatavalue.target_record_count = 0

        if (status == "0"):
            metadatavalue.op_parent_process_name = "OOZIE"
        elif (status == "1"):
            metadatavalue.op_parent_process_name = "BASH_SCRIPT"



        return metadatavalue
