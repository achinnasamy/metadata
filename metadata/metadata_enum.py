from enum import Enum

class METROLINX_TABLES(Enum):

    OR0   = 'RVN_PROD.RAMS_PRESTO_USAGE_TEMP',
    OR1   = 'RVN_PROD.RAMS_PRESTO_SALESTRAN',
    OR2   = 'RVN_PROD.RAMS_PRESTO_USAGE'

    HI1     = "RAMS_PRESTO_USAGE_TEMP",
    HI2     = "PROD.RAMS_PRESTO_SALESTRAN",
    HI3     = "PROD.RAMS_PRESTO_USAGE",

    HI4     = "bi_opd_stop_ext",
    HI5     = "bi_opd_trip_ext",
    HI6     = "nom_vehicle_ext",
    HI7     = "nom_vehicle_type_ext",
    HI8     = "veh_stop_detail_ext",
    HI9     = "veh_stop_ext",
    HI10    = "veh_trip_ext",

    C1      = "BI_OPD_STOP_Modified",
    C2      = "BI_OPD_TRIP_Modified",
    C3      = "BI_OPD_TRIP_Modified",
    C4      = "NOM_VEHICLE_Modified",
    C5      = "NOM_VEHICLE_TYPE_Modified",
    C6      =   "VEH_STOP_Modified",
    C7      =   "VEH_STOP_DETAIL_Modified",
    C8      =   "VEH_TRIP_Modified",
    C9      =   "NOM_VEHICLE_TYPE_Modified"

    def get_value(self):
        return self.value[0]




