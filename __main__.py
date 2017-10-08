from metadata import metadata_reckoner, operationaldata_reckoner, hive_manager, business_metadata_reckoner, \
    technical_metadata_reckoner

import sys
import sys,getopt

from metadata.metadata_enum import METROLINX_TABLES


def help():
    help_statement = "The METADATA job has to be started using the following protocols: \n\n\n" \
                     "  Metadata Ingestion           : python metadata-XXX.egg --job=M \n \n  Operational Data Ingestion   : python metadata-XXX.egg --job=O --status=0 --jobtype=ingestion --table=dev_bd_pilot.OPERATIONAL_DATA --csv=/user/hive/query_result.csv --oracle=OPERATIONAL_METADATA\n\n" \
                     " \n \n Creation of Business Metadata : python metadata-XXX.egg --job=B \n \n Creation of Technical Metadata : python metadata-XXX.egg --job=T \n\n" \
                     "  Creation of hive tables      : python metadata-XXX.egg --job=create-hive \n \n  Help                         : python metadata-XXX.egg --help \n\n " \
                     " Status : 0 - STARTED, 1 - RUNNING, 2 - SUCCESS \n Jobtype : ingestion, integration, consumption, curation"
    return help_statement



def fetch_table_name(table_short):

    complete_table_name = "NONE"
    for each in METROLINX_TABLES:
        if(each.name == table_short):
            complete_table_name = each.get_value()

    return complete_table_name



if __name__ == "__main__":


    job = ''
    table = 'NONE'
    oracle = 'NONE'
    csv = 'NONE'
    status = 0
    ingestion_param = []

    options, remainder = getopt.getopt(sys.argv[1:], 'o:h', ['job=','status=', 'jobtype=', 'table=', 'oracle=', 'csv=', 'help'])

    for opt, arg in options:

        if opt in ('-j', '--job'):
            job = arg
        elif opt in ('-s', '--status'):
            status = arg
        elif opt in ('-t', '--table'):
            table = arg
        elif opt in ('-o', '--oracle'):
            oracle = arg
        elif opt in ('-c', '--csv'):
            csv = arg
        elif opt in ('-jt', '--jobtype'):
            ingestion_param =  arg.split(",")
        elif opt in ('-h', '--help'):
            print "\n\n\n HELP \n\n\n"
            print help()
            sys.exit()
        else:
            print "\n\n\n HELP \n\n\n"
            print help()
            sys.exit()

    if job == 'M':
        metadata_reckoner.start_main(ingestion_param)
    elif job == 'O':
        operationaldata_reckoner.start_main(status, ingestion_param, fetch_table_name(table), fetch_table_name(oracle), fetch_table_name(csv))
    elif job == 'B':
        business_metadata_reckoner.start_main()
    elif job == 'T':
        technical_metadata_reckoner.start_main()
    elif job == 'create-hive':
        hive_manager.start_main(sys.argv[1:])
    else:
        print "\n\n Metadata Ingestion cannot start with options - " + job + "\n\n"
        print help()
        sys.exit()




