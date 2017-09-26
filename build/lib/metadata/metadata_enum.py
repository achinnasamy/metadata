from enum import Enum

class OPTYPE(Enum):

    INGESTION           =   'Ingestion',
    CURATION            =   'Curation',
    TRANSFORMATION      =   'Transformation',
    CONSUMPTION         =   'Consumption'