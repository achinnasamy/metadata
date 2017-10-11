from metadata import business_metadata_reckoner
from metadata.metadata_util import get_date, write_file_to_hdfs


def start_main():

    todays_date = get_date()

    LOCAL_XML_FILE_PATH = "/tmp/metadata_RAMS_tablename_%s.xml" % (todays_date)
    HDFS_XML_PATH = "/user/devuser/"

    business_file = open("xml/business_tag.xml", "r")
    business_contents_tag = business_file.read()
    xml = business_contents_tag
    business_file.close()

    file = open("xml/metrolinux_metadata_static.xml", "r")
    file_contents = file.read()

    new_xml_file_contents = file_contents % xml

    file.close()

    # Write the xml to the local file system
    file = open(LOCAL_XML_FILE_PATH, "w")
    file.write(new_xml_file_contents)
    file.close()

    # From the local file system, write it to HDFS
    write_file_to_hdfs(LOCAL_XML_FILE_PATH, HDFS_XML_PATH)


    #Remove the local file after writing to the HDFS
    import os
    os.remove(LOCAL_XML_FILE_PATH)

    business_metadata_reckoner.start_main()

    # Return a clean exit. If rough exit return -1
    return "0"