from lxml import etree


class MetaDataValidator:

    def validateInput(self):

        return True



    # Validate XML using XSD
    def validateXMLUsingXSD(self, xsd_file, xml_file):

        xmlschema_doc = etree.parse(xsd_file)
        xmlschema = etree.XMLSchema(xmlschema_doc)

        xml_doc = etree.parse(xml_file)
        result = xmlschema.validate(xml_doc)

        #return result

        # TODO : XSD Validation is disabled for now
        return True






