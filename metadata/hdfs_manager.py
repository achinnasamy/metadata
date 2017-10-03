import subprocess

from metadata.metadata_util import find_all_xml_files_in_hdfs, execute_hdfs, TEMP_XML_FILE_LOCATION


class HDFSManager:

    def fetchMetaDataFromHDFS(self):
        file_content_hdfs = execute_hdfs("metrolinx.xml")

        file = open(TEMP_XML_FILE_LOCATION, "w")
        file.write(file_content_hdfs)
        file.close()

        return


    def fetch_the_recent_xml_file_from_hdfs(self):

        list_of_xml_files =self.fetch_all_xml_files_from_hdfs("")
        print list_of_xml_files

        return

    def fetch_all_xml_files_from_hdfs(self, hdfs_dir_path):
        return find_all_xml_files_in_hdfs(hdfs_dir_path)

