from typing import Optional, List, Union
from enum import Enum
from pydantic import BaseModel, Field, validator
from typing_extensions import TypedDict



#############################
#### DEFINE ENUMERATIONS ####
#############################
class FileType(str, Enum):
    CSV = "csv"
    EXCEL = "excel"
    NETCDF = "netcdf"
    GEOTIFF = "geotiff"


class ColumnType(str, Enum):
    DATE = "date"
    GEO = "geo"
    FEATURE = "feature"


class DateType(str, Enum):
    YEAR = "year"
    MONTH = "month"
    DAY = "day"
    EPOCH = "epoch"
    DATE = "date"


class GeoType(str, Enum):
    LATITUDE = "latitude"
    LONGITUDE = "longitude"
    COORDINATES = "coordinates"
    COUNTRY = "country"
    ISO2 = "iso2"
    ISO3 = "iso3"
    STATE = "state/territory"
    COUNTY = "county/district"
    CITY = "municipality/town"


class FeatureType(str, Enum):
    INT = "int"
    FLOAT = "float"
    STR = "str"
    BINARY = "binary"
    BOOLEAN = "boolean"


class CoordFormat(str, Enum):
    LONLAT = "lonlat"
    LATLON = "latlon"


#################################
#### DEFINE ANNOTATION TYPES ####
#################################
class GeoAnnotation(BaseModel):
    name: str
    display_name: Optional[str]
    description: Optional[str]
    type: ColumnType = "geo"
    geo_type: GeoType
    primary_geo: Optional[bool]
    resolve_to_gadm: Optional[bool]
    is_geo_pair: Optional[str] = Field(
        title="Geo Pair",
        description="If present, this is the name of a paired coordinate column.",
        example="Lon_",
    )
    coord_format: Optional[CoordFormat] = Field(
        title="Coordinate Format",
        description="If geo type is COORDINATES, then provide the coordinate format",
    )
    qualifies: Optional[List[str]] = Field(
        title="Qualifies Columns",
        description="An array of the column names which this qualifies",
        example=["crop_production", "malnutrition_rate"],
    )

class TimeField(str, Enum):
    YEAR = "year"
    MONTH = "month"
    DAY = "day"
    HOUR = "hour"
    MINUTE = "minute"

class DateAnnotation(BaseModel):
    name: str = Field(example="year_column")
    display_name: Optional[str]
    description: Optional[str]
    type: ColumnType = "date"
    primary_date: Optional[bool]
    time_format: Optional[str] = Field(
        title="Time Format",
        description="The strftime formatter for this field",
        example="%y",
    )
    date_type: DateType
    # The following line
    # associated_columns: Optional[dict[TimeField, str]] = Field(
    # throws Exception "TypeError: 'type' object is not subscriptable"
    # Solve by not subscripting the dicionary; instead, add a validator.
    associated_columns: Optional[dict] = Field(
        title="Associated datetime column",
        description="the type of time as the key with the column name being the value",
        example={"day": "day_column", "hour": "hour_column"},
    )
    qualifies: Optional[List[str]] = Field(
        title="Qualifies Columns",
        description="An array of the column names which this qualifies",
        example=["crop_production", "malnutrition_rate"],
    )

    #@validator('date_type')
    #def time_format_optional(cls, v, values):
    #    if v == DateType.DATE and values["time_format"] == None:
    #        raise ValueError('time_format is required for when date_type is date')
    #    return v

    @validator('associated_columns')
    def validate_associated_columns(cls, v):
        # v is type dict
        # validate it is dict[TimeField, str]
        timefield_list = [e.value for e in TimeField]
        for kk, vv in v.items():
            if kk not in timefield_list:
                raise ValueError(str.format('Date associated_columns dictionary key must be of type TimeField: {} in {}', kk, v))
            elif type(vv) is not str:
                raise ValueError(str.format('Date associated_columns dictionary value must be of type string: {} in {}', vv, v))

class FeatureAnnotation(BaseModel):
    name: str
    display_name: Optional[str]
    description: Optional[str]
    type: ColumnType = "feature"
    feature_type: FeatureType
    units: Optional[str]
    units_description: Optional[str]
    qualifies: Optional[List[str]] = Field(
        title="Qualifies Columns",
        description="An array of the column names which this qualifies",
        example=["crop_production", "malnutrition_rate"],
    )


#########################
#### DEFINE METADATA ####
#########################
class Meta(BaseModel):
    ftype: FileType
    band: Optional[str]
    sheet: Optional[str]
    date: Optional[str]
    # info needed for multi-band geotiffs:
    feature_name: Optional[str]   # what the values represent e.g. wealth, flooding, headcount
    null_value: Optional[float]   # usually mixmasta will just determine this from the .tif file
    band_name: Optional[str]      # this will be the column name for the bands e.g. "year", "month_year"
    bands: Optional[dict] = Field(
        title="Geotiff Bands",
        description="dictionary str:object of band number:band value (value can be float, str, etc.)",
        example={
			"1": 2018,
			"2": 2019,
			"3": 2020,
			"4": 2021
		}

    )

############################
#### DEFINE FINAL MODEL ####
############################
class SpaceModel(BaseModel):
    geo: List[Optional[GeoAnnotation]]
    date: List[Optional[DateAnnotation]]
    feature: List[FeatureAnnotation]
    meta: Meta