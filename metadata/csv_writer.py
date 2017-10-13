

class CSVWriter:


    def writeToCSV(self, list_of_lines, file_location):

        with open(file_location, 'w') as file:
            for line in list_of_lines:
                file.write(line)
                file.write("\n")


        return