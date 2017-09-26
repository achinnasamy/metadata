
from xml.dom.minidom import parse
import xml.dom.minidom

from metadata.metadata_util import MetadataValue


class MetadataXMLParser:


    def parseXML(self):

        metadatavalue = MetadataValue()

        DOMTree = xml.dom.minidom.parse("metadata/metrolinux_metadata.xml")
        collection = DOMTree.documentElement

        if collection.hasAttribute("signal"):
           print "Root element : %s" % collection.getAttribute("signal")

           if (collection.getAttribute("signal") == 'metrolinx'):
                metadata_entity = collection.getElementsByTagName("metadata_entity")



                for me in metadata_entity:

                    if  me.hasAttribute("entityname"):

                       type = me.getElementsByTagName('op_type')[0]
                       metadatavalue.op_type = type.childNodes[0].data

                       type = me.getElementsByTagName('op_name')[0]
                       metadatavalue.op_name = type.childNodes[0].data

                       type = me.getElementsByTagName('target_path')[0]
                       metadatavalue.target_path = type.childNodes[0].data


        return metadatavalue

