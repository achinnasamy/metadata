from enum import Enum

class OPTYPE(Enum):

    H0   = 'RVN_PROD.RAMS_PRESTO_USAGE_TEMP',
    H1   = 'RVN_PROD.RAMS_PRESTO_SALESTRAN',
    H2   = 'RVN_PROD.RAMS_PRESTO_USAGE'

    def get_value(self):
        return self.value[0]


class OPNAME(Enum):
    HIVE    =   "Hive"
    BASH    =   "Bash"



