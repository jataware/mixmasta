"""Main module."""
import json
import logging
import click
import os
import sys
from datetime import datetime
from typing import List
from distutils.util import strtobool

import geofeather as gf
import geopandas as gpd
import numpy as np
import pandas as pd
from pandas.core.frame import DataFrame
import requests
import xarray as xr
from osgeo import gdal, gdalconst
from shapely import speedups
from shapely.geometry import Point

from pathlib import Path
import pkg_resources

import fuzzywuzzy
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import timeit
#from .spacetag_schema import SpaceModel

import click

# Constants
COL_ORDER = [
        "timestamp",
        "country",
        "admin1",
        "admin2",
        "admin3",
        "lat",
        "lng",
        "feature",
        "value",
    ]

GEO_TYPE_COUNTRY = "country"
GEO_TYPE_ADMIN1  = "state/territory"
GEO_TYPE_ADMIN2  = "county/district"
GEO_TYPE_ADMIN3  = "municipality/town"

if not sys.warnoptions:
    import warnings
    warnings.simplefilter("ignore")

logger = logging.getLogger(__name__)

def audit_renamed_col_dict(dct: dict) -> dict:
    """
    Description
    -----------
    Handle edge cases where a col could be renamed back to itself.
    example: no primary_geo, but country is present. Because it is a protected
    col name, it would be renamed country_non_primary. Later, it would be set
    as primary_geo country, and the pair added to renamed_col_dict again:
    {'country_non_primary' : ['country'], "country": ['country_non_primary'] }

    Parameters
    ----------
    dct: dict
        renamed_col_dict of key: new column name, value: list of old columns

    Output
    ------
    dict:
        The modified parameter dict.
    """
    remove_these = set()
    for k, v in dct.items():
        vstr = "".join(v)
        if vstr in dct.keys() and [k] in dct.values():
            remove_these.add(vstr)
            remove_these.add(k)

    for k in remove_these:
        dct.pop(k, None)

    return dct

def build_date_qualifies_field(qualified_col_dict: dict, assoc_fields: list) -> str:
    """
    Description
    -----------
    Handle edge case of each date field in assoc_fields qualifying the same
    column e.g. day/month/year are associated and qualify a field. In this
    case, the new_column_name.

    if assoc_fields is found as a value in qualified_col_dict, return the key

    Parameters
    ----------
    qualified_col_dict: dict
        {'pop': ['month_column', 'day_column', 'year_column']}

    assoc_fields: list
        ['month_column', 'day_column', 'year_column']

    """
    for k, v in qualified_col_dict.items():
        if v == assoc_fields:
            return k

    return None

def format_time(t: str, time_format: str, validate: bool = True) -> int:
    """
    Description
    -----------
    Converts a time feature (t) into epoch time using `time_format` which is a strftime definition

    Parameters
    ----------
    t: str
        the time string
    time_format: str
        the strftime format for the string t
    validate: bool, default True
        whether to error check the time string t. Is set to False, then no error is raised if the date fails to parse, but None is returned.

    Examples
    --------

    >>> epoch = format_time('5/12/20 12:20', '%m/%d/%y %H:%M')
    """

    try:
        t_ = int(datetime.strptime(t, time_format).timestamp()) * 1000 # Want milliseonds
        return t_
    except Exception as e:
        if t.endswith(' 00:00:00'):
            # Depending on the date format, pandas.read_excel will read the
            # date as a Timestamp, so here it is a str with format
            # '2021-03-26 00:00:00'. For now, handle this single case until
            # there is time for a more comprehensive solution e.g. add a custom
            # date_parser function that doesn't parse diddly/squat to
            # pandas.read_excel() in process().
            return format_time(t.replace(' 00:00:00', ''), time_format, validate)
        print(e)
        if validate:
            raise Exception(e)
        else:
            return None

def geocode(
    admin: str, df: pd.DataFrame, x: str = "longitude", y: str = "latitude", gadm: gpd.GeoDataFrame = None,
    df_geocode: pd.DataFrame = pd.DataFrame()
) -> pd.DataFrame:
    """
        Description
        -----------
        Takes a dataframe containing coordinate data and geocodes it to GADM (https://gadm.org/)

        GEOCODES to ADMIN 0, 1, 2 OR 3 LEVEL

        Parameters
        ----------
        admin: str
            the level to geocode to. 'admin0' to 'admin3'
        df: pd.DataFrame
            a pandas dataframe containing point data
        x: str, default 'longitude'
            the name of the column containing longitude information
        y: str, default 'latitude'
            the name of the column containing latitude data
        gadm: gpd.GeoDataFrame, default None
            optional specification of a GeoDataFrame of GADM shapes of the appropriate
            level (admin2/3) for geocoding
        df_geocode: pd.DataFrame, default pd.DataFrame()
            cached lat/long geocode library

        Examples
        --------
        Geocoding a dataframe with columns named 'lat' and 'lon'

        >>> df = geocode(df, x='lon', y='lat')

    """

    flag = speedups.available
    if flag == True:
        speedups.enable()

    cdir = os.path.expanduser("~")
    download_data_folder = f"{cdir}/mixmasta_data"

    # Only load GADM if it wasn't explicitly passed to the function.
    if gadm is not None:
        logging.info("GADM geo dataframe has been provided.")
    else:
        logging.info("GADM has not been provided; loading now.")

        if admin in ['admin0','country']:
            gadm_fn = f"gadm36_2.feather"
            gadmDir = f"{download_data_folder}/{gadm_fn}"
            gadm = gf.from_geofeather(gadmDir)
            gadm["country"] = gadm["NAME_0"]
            gadm = gadm[["geometry", "country"]]

        elif admin == "admin1":
            gadm_fn = f"gadm36_2.feather"
            gadmDir = f"{download_data_folder}/{gadm_fn}"
            gadm = gf.from_geofeather(gadmDir)
            gadm["country"] = gadm["NAME_0"]
            #gadm["state"] = gadm["NAME_1"]
            gadm["admin1"] = gadm["NAME_1"]
            #gadm = gadm[["geometry", "country", "state", "admin1"]]
            gadm = gadm[["geometry", "country", "admin1"]]

        elif admin == "admin2":
            gadm_fn = f"gadm36_2.feather"
            gadmDir = f"{download_data_folder}/{gadm_fn}"
            gadm = gf.from_geofeather(gadmDir)
            gadm["country"] = gadm["NAME_0"]
            #gadm["state"] = gadm["NAME_1"]
            gadm["admin1"] = gadm["NAME_1"]
            gadm["admin2"] = gadm["NAME_2"]
            #gadm = gadm[["geometry", "country", "state", "admin1", "admin2"]]
            gadm = gadm[["geometry", "country", "admin1", "admin2"]]

        elif admin == "admin3":
            gadm_fn = f"gadm36_3.feather"
            gadmDir = f"{download_data_folder}/{gadm_fn}"
            gadm = gf.from_geofeather(gadmDir)
            gadm["country"] = gadm["NAME_0"]
            #gadm["state"] = gadm["NAME_1"]
            gadm["admin1"] = gadm["NAME_1"]
            gadm["admin2"] = gadm["NAME_2"]
            gadm["admin3"] = gadm["NAME_3"]
            #gadm = gadm[["geometry", "country", "state", "admin1", "admin2", "admin3"]]
            gadm = gadm[["geometry", "country", "admin1", "admin2", "admin3"]]

    start_time = timeit.default_timer()

    # 1) Drop x,y duplicates from data frame.
    df_drop_dup_geo = df[[x,y]].drop_duplicates(subset=[x,y])
    
    # 2) Get x,y not in df_geocode.
    if not df_geocode.empty and not df_drop_dup_geo.empty:
        df_drop_dup_geo = df_drop_dup_geo.merge(df_geocode, on=[x,y], how='left', indicator=True)
        df_drop_dup_geo = df_drop_dup_geo[ df_drop_dup_geo['_merge'] == 'left_only']
        df_drop_dup_geo = df_drop_dup_geo[[x,y]]

    if not df_drop_dup_geo.empty:
        # dr_drop_dup_geo contains x,y not in df_geocode; so, these need to be
        # geocoded and added to the df_geocode library.

        # 3) Apply Point() to create the geometry col.
        df_drop_dup_geo.loc[:, "geometry"] = df_drop_dup_geo.apply(lambda row: Point(row[x], row[y]), axis=1) 

        # 4) Sjoin unique geometries with GADM.
        gdf = gpd.GeoDataFrame(df_drop_dup_geo)
        
        # Spatial merge on GADM to obtain admin areas.
        gdf = gpd.sjoin(gdf, gadm, how="left", op="within", lsuffix="mixmasta_left", rsuffix="mixmasta_geocoded")
        del gdf["geometry"]
        del gdf["index_mixmasta_geocoded"]

        # 5) Add the new geocoding to the df_geocode lat/long geocode library.
        if not df_geocode.empty:
            df_geocode = df_geocode.append(gdf)
        else:
            df_geocode = gdf

    # 6) Merge df and df_geocode on x,y
    gdf = df.merge(df_geocode, how='left', on=[x,y])

    return pd.DataFrame(gdf), df_geocode

def generate_column_name(field_list: list) -> str:
    """
    Description
    -----------
    Contatenate a list of column fields into a single column name.

    Parameters
    ----------
    field_list: list[str] of column names

    Returns
    -------
    str: new column name

    """
    return ''.join(sorted(field_list))

def generate_timestamp_column(df: pd.DataFrame, date_mapper: dict, column_name: str) -> pd.DataFrame:
    """
    Description
    -----------
    Efficiently add a new timestamp column to a dataframe. It avoids the use of df.apply
    which appears to be much slower for large dataframes. Defaults to 1/1/1970 for
    missing day/month/year values.

    Parameters
    ----------
    df: pd.DataFrame
        our data
    date_mapper: dict
        a schema mapping (JSON) for the dataframe filtered for "date_type" equal to
        Day, Month, or Year. The format is screwy for our purposes here and could
        be reafactored.
    column_name: str
        name of the new column e.g. timestamp for primary_time, year1month1day1
        for a concatneated name from associated date fields.

    Examples
    --------
    This example adds the generated series to the source dataframe.
    >>> df = df.join(df.apply(generate_timestamp, date_mapper=date_mapper,
            column_name="year1month1day", axis=1))
    """

    # Identify which date values are passed.
    dayCol = None
    monthCol = None
    yearCol = None

    for kk, vv in date_mapper.items():
        if vv and vv["date_type"] == "day":
            dayCol = kk
        elif vv and vv["date_type"] == "month":
           monthCol = kk
        elif vv and vv["date_type"] == "year":
            yearCol = kk

    # For missing date values, add a column to the dataframe with the default
    # value, then assign that to the day/month/year var. If the dataframe has
    # the date value, assign day/month/year to it after casting as a str.
    if dayCol:
        day = df[dayCol].astype(str)
    else:
       df.loc[:, 'day_generate_timestamp_column'] = "1"
       day = df['day_generate_timestamp_column']

    if monthCol:
        month = df[monthCol].astype(str)
    else:
       df.loc[:, 'month_generate_timestamp_column'] = "1"
       month = df['month_generate_timestamp_column']

    if yearCol:
        year = df[yearCol].astype(str)
    else:
        df.loc[:, 'year_generate_timestamp_column'] = "01"
        year = df['year_generate_timestamp_column']

    # Add the new column
    df.loc[:, column_name] = month + '/' + day + '/' + year

    # Delete the temporary columns
    if not dayCol:
        del(df['day_generate_timestamp_column'])

    if not monthCol:
        del(df['month_generate_timestamp_column'])

    if not yearCol:
        del(df['year_generate_timestamp_column'])

    return df

def generate_timestamp_format(date_mapper: dict) -> str:
    """
    Description
    -----------
    Generates a the time format for day,month,year dates based on each's
    specified time_format.

    Parameters
    ----------
    date_mapper: dict
        a dictionary for the schema mapping (JSON) for the dataframe filtered
        for "date_type" equal to Day, Month, or Year.

    Output
    ------
    e.g. "%m/%d/%Y"
    """

    day = "%d"
    month = "%m"
    year = "%y"

    for kk, vv in date_mapper.items():
        if vv["date_type"] == "day":
            day = vv["time_format"]
        elif vv["date_type"] == "month":
            month = vv["time_format"]
        elif vv["date_type"] == "year":
            year = vv["time_format"]

    return str.format("{}/{}/{}", month, day, year)

def get_iso_country_dict(iso_list: list) -> dict:
    """
    Description
    -----------
    iso2 or iso3 is used as primary_geo and therefore the country column.
    Load the custom iso lookup table and return a dictionary of the iso codes
    as keys and the country names as values. Assume all list items are the same
    iso type.

    Parameters
    ----------
    iso_list:
        list of iso2 or iso3 codes

    Returns
    -------
    dict:
        key: iso code; value: country name
    """

    dct = {}
    if iso_list:
        iso_df = pd.DataFrame
        try:
            # The necessary code to load from pkg doesn't currently work in VS
            # Code Debug, so wrap in try/except.
            #iso_df = pd.read_csv(pkg_resources.resource_stream(__name__, 'data/iso_lookup.csv'))
            with pkg_resources.resource_stream(__name__, 'data/iso_lookup.csv') as f:
                iso_df = pd.read_csv(f)
            #path = Path(__file__).parent / "data/iso_lookup.csv"
            #iso_df = pd.read_csv(path)
        except:
            # Local VS Code load.
            path = Path(__file__).parent / "data/iso_lookup.csv"
            iso_df = pd.read_csv(path)

        if iso_df.empty:
            return dct

        if len(iso_list[0]) == 2:
            for iso in iso_list:
                if iso in iso_df["iso2"].values:
                    dct[iso] = iso_df.loc[iso_df["iso2"] == iso]["country"].item()
        else:
            for iso in iso_list:
                if iso in iso_df["iso3"].values:
                    dct[iso] = iso_df.loc[iso_df["iso3"] == iso]["country"].item()

    return dct

def handle_colname_collisions(df: pd.DataFrame, mapper: dict, protected_cols: list) -> (pd.DataFrame, dict, dict):
    """
    Description
    -----------
    Identify mapper columns that match protected column names. When found,
    update the mapper and dataframe, and keep a dict of these changes
    to return to the caller e.g. SpaceTag.

    Parameters
    ----------
    df: pd.DataFrame
        submitted data
    mapper: dict
        a dictionary for the schema mapping (JSON) for the dataframe.
    protected_cols: list
        protected column names i.e. timestamp, country, admin1, feature, etc.

    Output
    ------
    pd.DataFame:
        The modified dataframe.
    dict:
        The modified mapper.
    dict:
        key: new column name e.g. "day1month1year1" or "country_non_primary"
        value: list of old column names e.g. ['day1','month1','year1'] or ['country']
    """

    # Get names of geo fields that collide and are not primary_geo = True
    non_primary_geo_cols = [d["name"] for d in mapper["geo"] if d["name"] in protected_cols and ("primary_geo" not in d or d["primary_geo"] == False)]

    # Get names of date fields that collide and are not primary_date = True
    non_primary_time_cols = [d['name'] for d in mapper['date'] if d["name"] in protected_cols and ('primary_date' not in d or d['primary_date'] == False)]

    # Only need to change a feature column name if it qualifies another field,
    # and therefore will be appended as a column to the output.
    feature_cols = [d["name"] for d in mapper['feature'] if d["name"] in protected_cols and "qualifies" in d and d["qualifies"]]

    # Verbose build of the collision_list, could have combined above.
    collision_list = non_primary_geo_cols + non_primary_time_cols + feature_cols

    # Bail if no column name collisions.
    if not collision_list:
        return df, mapper, {}

    # Append any collision columns with the following suffix.
    suffix = "_non_primary"

    # Build output dictionary and update df.
    renamed_col_dict = {}
    for col in collision_list:
        df.rename(columns={col: col + suffix}, inplace=True)
        renamed_col_dict[col + suffix] = [col]

    # Update mapper
    for k, vlist in mapper.items():
        for dct in vlist:
            if dct["name"] in collision_list:
                dct["name"] = dct["name"] + suffix
            elif "qualifies" in dct and dct["qualifies"]:
                # change any instances of this column name qualified by another field
                dct["qualifies"] = [w.replace(w, w + suffix) if w in collision_list else w for w in dct["qualifies"] ]
            elif "associated_columns" in dct and dct["associated_columns"]:
                # change any instances of this column name in an associated_columns dict
                dct["associated_columns"] = {k: v.replace(v, v + suffix) if v in collision_list else v for k, v in dct["associated_columns"].items() }

    return df, mapper, renamed_col_dict

def match_geo_names(admin: str, df: pd.DataFrame, resolve_to_gadm_geotypes: list, gadm: gpd.GeoDataFrame = None) -> pd.DataFrame:
    """
    Assumption
    ----------
    Country was selected by drop-down on file submission, column "country"
    is present in the data frame, and lng/lat is not being used for geocoding.

    Parameters
    ----------
    admin: str
        the level to geocode to. Either 'admin2' or 'admin3'
    df: pandas.DataFrame
        the uploaded dataframe
    resolve_to_gadm_geotypes:
        list of geotypes marked resolve_to_gadm = True e.g. ["admin1", "country"]
    gadm: gpd.GeoDataFrame, default None
        optional specification of a GeoDataFrame of GADM shapes of the appropriate
        level (admin2/3) for geocoding

    Result
    ------
    A pandas.Dataframe produced by modifying the parameter df.

    """
    print('geocoding ...')
    flag = speedups.available
    if flag == True:
        speedups.enable()

    cdir = os.path.expanduser("~")
    download_data_folder = f"{cdir}/mixmasta_data"

    # only load GADM if it wasn't explicitly passed to the function.
    if gadm is not None:
        #logging.info("GADM geo dataframe has been provided.")
        pass
    else:
        logging.info("GADM has not been provided; loading now.")

        if admin == "admin2":
            gadm_fn = f"gadm36_2.feather"
        else:
            gadm_fn = f"gadm36_3.feather"

        gadmDir = f"{download_data_folder}/{gadm_fn}"
        gadm = gf.from_geofeather(gadmDir)

        gadm["country"] = gadm["NAME_0"]
        gadm["state"]   = gadm["NAME_1"]
        gadm["admin1"]  = gadm["NAME_1"]
        gadm["admin2"]  = gadm["NAME_2"]

        if admin == "admin2":
            gadm = gadm[["country", "state", "admin1", "admin2"]]
        else:
            gadm["admin3"] = gadm["NAME_3"]
            gadm = gadm[["country", "state", "admin1", "admin2", "admin3"]]

    # Filter GADM for countries in df.
    countries = df["country"].unique()

    # Correct country names.
    if GEO_TYPE_COUNTRY in resolve_to_gadm_geotypes:
        gadm_country_list = gadm["country"].unique()
        unknowns = df[~df.country.isin(gadm_country_list)].country.tolist()
        for unk in unknowns:
            match = fuzzywuzzy.process.extractOne(unk, gadm_country_list, scorer=fuzz.partial_ratio)
            if match != None:
                df.loc[df.country == unk, 'country'] = match[0]

    # Filter GADM dicitonary for only those countries (ie. speed up)
    gadm = gadm[gadm["country"].isin(countries)]

    # Loop by country using gadm dict filtered for that country.
    for c in countries:
        # The following ignores admin1 / admin2 pairs; it only cares if those
        # values exist for the appropriate country.

        # Get list of admin1 values in df but not in gadm. Reduce list for country.
        if GEO_TYPE_ADMIN1 in resolve_to_gadm_geotypes:
            admin1_list = gadm[gadm.country==c]["admin1"].unique()
            if admin1_list is not None and all(admin1_list) and 'admin1' in df:
                unknowns = df[(df.country == c) & ~df.admin1.isin(admin1_list)].admin1.tolist()
                unknowns = [x for x in unknowns if pd.notnull(x) and x.strip()] # remove Nan
                for unk in unknowns:
                    match = fuzzywuzzy.process.extractOne(unk, admin1_list, scorer=fuzz.partial_ratio)
                    if match != None:
                        df.loc[df.admin1 == unk, 'admin1'] = match[0]

        # Get list of admin2 values in df but not in gadm. Reduce list for country.
        if GEO_TYPE_ADMIN2 in resolve_to_gadm_geotypes:
            admin2_list = gadm[gadm.country==c ]["admin2"].unique()
            if admin2_list is not None and all(admin2_list) and 'admin2' in df:
                unknowns = df[(df.country == c) & ~df.admin2.isin(admin2_list)].admin2.tolist()
                unknowns = [x for x in unknowns if pd.notnull(x) and x.strip()] # remove Nan
                for unk in unknowns:
                    match = fuzzywuzzy.process.extractOne(unk, admin2_list, scorer=fuzz.partial_ratio)
                    if match != None:
                        df.loc[df.admin2 == unk, 'admin2'] = match[0]

        if admin =='admin3' and GEO_TYPE_ADMIN3 in resolve_to_gadm_geotypes:
            # Get list of admin3 values in df but not in gadm. Reduce list for country.
            admin3_list = gadm[gadm.country==c]["admin3"].unique()
            if admin3_list is not None and all(admin3_list) and 'admin3' in df:
                unknowns = df[(df.country == c) & ~df.admin3.isin(admin3_list)].admin3.tolist()
                unknowns = [x for x in unknowns if pd.notnull(x) and x.strip()] # remove Nan
                for unk in unknowns:
                    match = fuzzywuzzy.process.extractOne(unk, admin3_list, scorer=fuzz.partial_ratio)
                    if match != None:
                        df.loc[df.admin3 == unk, 'admin3'] = match[0]

    return df

def netcdf2df(netcdf: str) -> pd.DataFrame:
    """
    Produce a dataframe from a NetCDF4 file.

    Parameters
    ----------
    netcdf: str
        Path to the netcdf file

    Returns
    -------
    DataFrame
        The resultant dataframe
    """
    try:        
        ds = xr.open_dataset(netcdf)
    except:
        raise AssertionError(f"improperly formatted netCDF file ({netcdf})")

    data = ds.to_dataframe()
    df = data.reset_index()

    return df

def normalizer(df: pd.DataFrame, mapper: dict, admin: str, gadm: gpd.GeoDataFrame = None, df_geocode: pd.DataFrame = pd.DataFrame()) -> (pd.DataFrame, dict, pd.DataFrame):
    """
    Description
    -----------
    Converts a dataframe into a CauseMos compliant format.

    Parameters
    ----------
    df: pd.DataFrame
        a pandas dataframe containing point data
    mapper: dict
        a schema mapping (JSON) for the dataframe
        a dict where keys will be geo, feaure, date, and values will be lists of dict
        example:
        { 'geo': [
             {'name': 'country', 'type': 'geo', 'geo_type': 'country', 'primary_geo': False},
             {'name': 'state', 'type': 'geo', 'geo_type': 'state/territory', 'primary_geo': False}
           ],
           'feature': [
              {'name': 'probabilty', 'type': 'feature', 'feature_type': 'float'},
              {'name': 'color', 'type': 'feature', 'feature_type': 'str'}
            ],
            'date': [
               {'name': 'date_2', 'type': 'date', 'date_type': 'date', 'primary_date': False, 'time_format': '%m/%d/%y'},
               {'name': 'date', 'type': 'date', 'date_type': 'date', 'primary_date': True, 'time_format': '%m/%d/%y'}
            ]
        }
    admin: str, default 'admin2'
        the level to geocode to. Either 'admin2' or 'admin3'
    gadm: gpd.GeoDataFrame, default None
        optional specification of a GeoDataFrame of GADM shapes of the appropriate
        level (admin2/3) for geocoding        
    df_gecode: pd.DataFrame, default pd.DataFrame()
        lat,long geocode lookup library

    Returns
    -------
    pd.DataFrame: CauseMos compliant format ready to be written to parquet.
    dict: dictionary of modified column names; used by SpaceTag
    pd.DataFRame: update lat,long geocode looup library

    Examples
    --------
    >>> df_norm = normalizer(df, mapper, 'admin3')
    """
    col_order = COL_ORDER.copy()

    required_cols = [
        "timestamp",
        "country",
        "admin1",
        "admin2",
        "admin3",
        "lat",
        "lng",
    ]

    # List of date_types that be used to build a date.
    MONTH_DAY_YEAR  = ["day","month","year"]

    # Create a dictionary of list: colnames: new col name, and modify df and
    # mapper for any column name collisions.
    df, mapper, renamed_col_dict = handle_colname_collisions(df, mapper, col_order)

    ### mapper is a dictionary of lists of dictionaries.
    click.echo("Raw dataframe:")
    click.echo(df.head())

    # list of names of datetime columns primary_date=True
    primary_time_cols = [k['name'] for k in mapper['date'] if 'primary_date' in k and k['primary_date'] == True]

    # list of names of datetime columns no primary_date or primary_date = False
    other_time_cols   = [k['name'] for k in mapper['date'] if 'primary_date' not in k or k['primary_date'] == False]

    # list of names of geo columns primary_geo=True
    primary_geo_cols  = [k["name"] for k in mapper["geo"]  if "primary_geo"  in k and k["primary_geo"] == True]

    # list of geotypes of geo columns primary_geo=True (used for match_geo_names logic below)
    primary_geo_types = [k["geo_type"] for k in mapper["geo"]  if "primary_geo"  in k and k["primary_geo"] == True]

    # qualified_col_dict: dictionary for columns qualified by another column.
    # key: qualified column
    # value: list of columns that qualify key column
    qualified_col_dict = {}

    # subset dataframe for only columns specified in mapper schema.
    # get all named objects in the date, feature, geo schema lists.
    mapper_keys = []
    for k in mapper.items():
        mapper_keys.extend([l['name'] for l in k[1] if 'name' in l])

    df = df[mapper_keys]

    # Rename protected columns
    # and perform type conversion on the time column
    features = []
    primary_date_group_mapper = {}
    other_date_group_mapper = {}

    for date_dict in mapper["date"]:
        kk = date_dict["name"]
        if kk in primary_time_cols:
            # There should only be a single epoch or date field, or a single
            # group of year/month/day/minute/second marked as primary_time in
            # the loaded schema.
            if date_dict["date_type"] == "date":
                # convert primary_time of date_type date to epochtime and rename as 'timestamp'
                df.loc[:, kk] = df[kk].apply(lambda x: format_time(str(x), date_dict["time_format"], validate=False))
                staple_col_name = "timestamp"
                df.rename(columns={kk: staple_col_name}, inplace=True)
                # renamed_col_dict[ staple_col_name ] = [kk] # 7/2/2021 do not include primary cols
            elif date_dict["date_type"] == "epoch":
                # rename epoch time column as 'timestamp'
                staple_col_name = "timestamp"
                df.rename(columns={kk: staple_col_name}, inplace=True)
                #renamed_col_dict[ staple_col_name ] = [kk] # 7/2/2021 do not include primary cols
            elif date_dict["date_type"] in ["day","month","year"]:
                primary_date_group_mapper[kk] = date_dict

        else:
            if date_dict["date_type"] == "date":
                # Convert all date/time to epoch time if not already.
                df.loc[:, kk] = df[kk].apply(lambda x: format_time(str(x), date_dict["time_format"], validate=False))
                # If three are no assigned primary_time columns, make this the
                # primary_time timestamp column, and keep as a feature so the
                # column_name meaning is not lost.
                if not primary_time_cols and not "timestamp" in df.columns:
                    df.rename(columns={kk: "timestamp"}, inplace=True)
                    staple_col_name ="timestamp"
                    renamed_col_dict[ staple_col_name ] = [kk]
                # All not primary_time, not associated_columns fields are pushed to features.
                features.append(kk)

            elif date_dict["date_type"] in MONTH_DAY_YEAR and 'associated_columns' in date_dict and date_dict["associated_columns"]:
                # Various date columns have been associated by the user and are not primary_date.
                # convert them to epoch then store them as a feature
                # (instead of storing them as separate uncombined features).
                # handle this dict after iterating all date fields
                other_date_group_mapper[kk] = date_dict

            else:
                features.append(kk)

        if "qualifies" in date_dict and date_dict["qualifies"]:
            # Note that any "qualifier" column that is not primary geo/date
            # will just be lopped on to the right as its own column. It's
            # column name will just be the name and Uncharted will deal with
            # it. The key takeaway is that qualifier columns grow the width,
            # not the length of the dataset.
            # Want to add the qualified col as the dictionary key.
            # e.g. "name": "region", "qualifies": ["probability", "color"]
            # should produce two dict entries for prob and color, with region
            # in a list as the value for both.
            for k in date_dict["qualifies"]:
                if k in qualified_col_dict:
                    qualified_col_dict[k].append(kk)
                else:
                    qualified_col_dict[k] = [kk]

    if primary_date_group_mapper:
        # Applied when there were primary_date year,month,day fields above.
        # These need to be combined
        # into a date and then epoch time, and added as the timestamp field.

        # Create a separate df of the associated date fields. This avoids
        # pandas upcasting the series dtypes on df.apply(); e.g., int to float,
        # or a month 9 to 9.0, which breaks generate_timestamp()
        assoc_fields = primary_date_group_mapper.keys()
        date_df = df[ assoc_fields ]

        # Now generate the timestamp from date_df and add timestamp col to df.
        df = generate_timestamp_column(df, primary_date_group_mapper, "timestamp")

        # Determine the correct time format for the new date column, and
        # convert to epoch time.
        time_formatter = generate_timestamp_format(primary_date_group_mapper)
        df['timestamp'] = df["timestamp"].apply(lambda x: format_time(str(x), time_formatter, validate=False))

        # Let SpaceTag know those date columns were renamed to timestamp.
        #renamed_col_dict[ "timestamp" ] = assoc_fields # 7/2/2021 do not include primary cols

    while other_date_group_mapper:
        # Various date columns have been associated by the user and are not primary_date.
        # Convert to epoch time and store as a feature, do not store these separately in features.
        # Exception is the group is only two of day, month, year: leave as date.
        # Control for possibility of more than one set of assciated_columns.

        # Pop the first item in the mapper and begin building that date set.
        date_field_tuple = other_date_group_mapper.popitem()

        # Build a list of column names associated with the the popped date field.
        assoc_fields = [k[1] for k in date_field_tuple[1]['associated_columns'].items()]

        # Pop those mapper objects into a dict based on the column name keys in
        # assocfields list.
        assoc_columns_dict = { f : other_date_group_mapper.pop(f) for f in assoc_fields if f in other_date_group_mapper }

        # Add the first popped tuple into the assoc_columns dict where the key is the
        # first part of the tuple; the value is the 2nd part.
        assoc_columns_dict[date_field_tuple[0]] = date_field_tuple[1]

        # Add the first popped tuple column name to the list of associated fields.
        assoc_fields.append(date_field_tuple[0])

        # TODO: If day and year are associated to each other and month, but
        # month is not associated to those fields, then at this point assoc_fields
        # will be the three values, and assoc_columns will contain only day and
        # year. This will error out below. It is assumed that SpaceTag will
        # control for this instance.

        # If there is no primary_time column for timestamp, which would have
        # been created above with primary_date_group_mapper, or farther above
        # looping mapper["date"], attempt to generate from date_type = Month,
        # Day, Year features. Otherwise, create a new column name from the
        # concatenation of the associated date fields here.
        if not "timestamp" in df.columns:
            new_column_name = "timestamp"
        else:
            new_column_name = generate_column_name(assoc_fields)

        # Create a separate df of the associated date fields. This avoids
        # pandas upcasting the series dtypes on df.apply(); e.g., int to float,
        # or a month 9 to 9.0, which breaks generate_timestamp()
        date_df = df[ assoc_fields ]

        # Now generate the timestamp from date_df and add timestamp col to df.
        df = generate_timestamp_column(df, assoc_columns_dict, new_column_name)

        # Determine the correct time format for the new date column, and
        # convert to epoch time only if all three date components (day, month,
        # year) are present; otherwise leave as a date string.
        date_types = [v["date_type"] for k,v in assoc_columns_dict.items()]
        if len(frozenset(date_types).intersection(MONTH_DAY_YEAR)) == 3:
            time_formatter = generate_timestamp_format(assoc_columns_dict)
            df.loc[:, new_column_name] = df[new_column_name].apply(lambda x: format_time(str(x), time_formatter, validate=False))

        # Let SpaceTag know those date columns were renamed to a new column.
        renamed_col_dict[ new_column_name] = assoc_fields

        # timestamp is a protected column, so don't add to features.
        if new_column_name != "timestamp":
            # Handle edge case of each date field in assoc_fields qualifying
            # the same column e.g. day/month/year are associated and qualify
            # a field. In this case, the new_column_name
            qualified_col = build_date_qualifies_field(qualified_col_dict, assoc_fields)
            if qualified_col is None:
                features.append(new_column_name)
            else:
                qualified_col_dict[qualified_col] = [new_column_name]

    for geo_dict in mapper["geo"]:
        kk = geo_dict["name"]
        if kk in primary_geo_cols:
            if geo_dict["geo_type"] == "latitude":
                staple_col_name = "lat"
                df.rename(columns={kk: staple_col_name}, inplace=True)
                #renamed_col_dict[staple_col_name] = [kk] # 7/2/2021 do not include primary cols
            elif geo_dict["geo_type"] == "longitude":
                staple_col_name = "lng"
                df.rename(columns={kk: staple_col_name}, inplace=True)
                #renamed_col_dict[staple_col_name] = [kk] # 7/2/2021 do not include primary cols
            elif geo_dict["geo_type"] == "coordinates":
                c_f = geo_dict["coord_format"]
                coords = df[kk].values
                if c_f == "lonlat":
                    lats = [x for x in coords.split(",")[1]]
                    longs = [x for x in coords.split(",")[0]]
                else:
                    lats = [x for x in coords.split(",")[0]]
                    longs = [x for x in coords.split(",")[1]]
                df["lng"] = longs
                df["lat"] = lats
                del df[kk]
            elif geo_dict["geo_type"] == GEO_TYPE_COUNTRY and kk != "country":
                # force the country column to be named country
                staple_col_name = "country"
                df.rename(columns={kk: staple_col_name}, inplace=True)
                #renamed_col_dict[staple_col_name] = [kk] # 7/2/2021 do not include primary cols
            elif geo_dict["geo_type"] == GEO_TYPE_ADMIN1 and kk != "admin1":
                # force the country column to be named country
                staple_col_name = "admin1"
                df.rename(columns={kk: staple_col_name}, inplace=True)
            elif geo_dict["geo_type"] == GEO_TYPE_ADMIN2 and kk != "admin2":
                # force the country column to be named country
                staple_col_name = "admin2"
                df.rename(columns={kk: staple_col_name}, inplace=True)
            elif geo_dict["geo_type"] == GEO_TYPE_ADMIN3 and kk != "admin2":
                # force the country column to be named country
                staple_col_name = "admin3"
                df.rename(columns={kk: staple_col_name}, inplace=True)

            elif str(geo_dict["geo_type"]).lower() in ["iso2", "iso3"]:
                # use the ISO2 or ISO3 column as country

                # use ISO2/3 lookup dictionary to change ISO to country name.
                iso_list = df[kk].unique().tolist()
                dct = get_iso_country_dict(iso_list)
                df.loc[:, kk] = df[kk].apply(lambda x: dct[x] if x in dct else x)

                # now rename that column as "country"
                staple_col_name = "country"
                df.rename(columns={kk: staple_col_name}, inplace=True)
                #renamed_col_dict[staple_col_name] = [kk] # 7/2/2021 do not include primary cols

        elif "qualifies" in geo_dict and geo_dict["qualifies"]:
            # Note that any "qualifier" column that is not primary geo/date
            # will just be lopped on to the right as its own column. It'’'s
            # column name will just be the name and Uncharted will deal with
            # it. The key takeaway is that qualifier columns grow the width,
            # not the length of the dataset.
            # Want to add the qualified col as the dictionary key.
            # e.g. "name": "region", "qualifies": ["probability", "color"]
            # should produce two dict entries for prob and color, with region
            # in a list as the value for both.
            for k in geo_dict["qualifies"]:
                if k in qualified_col_dict:
                    qualified_col_dict[k].append(kk)
                else:
                    qualified_col_dict[k] = [kk]
        else:
            # only push geo columns to the named columns
            # in the event there is no primary geo
            # otherwise they are features and we geocode lat/lng
            if len(primary_geo_cols) == 0:
                if geo_dict["geo_type"] == GEO_TYPE_COUNTRY:
                    df["country"] = df[kk]
                    renamed_col_dict["country"] = [kk]
                    continue
                if geo_dict["geo_type"] == GEO_TYPE_ADMIN1:
                    df["admin1"] = df[kk]
                    renamed_col_dict["admin1"] = [kk]
                    continue
                if geo_dict["geo_type"] == GEO_TYPE_ADMIN2:
                    df["admin2"] = df[kk]
                    renamed_col_dict["admin2"] = [kk]
                    continue
                if geo_dict["geo_type"] == GEO_TYPE_ADMIN3:
                    df["admin3"] = df[kk]
                    renamed_col_dict["admin3"] = [kk]
                    continue
            features.append(kk)

    # Append columns annotated in feature dict to features list (if not a
    # qualifies column)
    #features.extend([k["name"] for k in mapper["feature"]])
    for feature_dict in mapper["feature"]:
        if "qualifies" not in feature_dict or not feature_dict["qualifies"]:
            features.append(feature_dict["name"])
        elif "qualifies" in feature_dict and feature_dict["qualifies"]:
            # Note that any "qualifier" column that is not primary geo/date
            # will just be lopped on to the right as its own column. It's
            # column name will just be the name and Uncharted will deal with
            # it. The key takeaway is that qualifier columns grow the width,
            # not the length of the dataset.
            # Want to add the qualified col as the dictionary key.
            # e.g. "name": "region", "qualifies": ["probability", "color"]
            # should produce two dict entries for prob and color, with region
            # in a list as the value for both.
            for k in feature_dict["qualifies"]:
                kk = feature_dict["name"]
                if k in qualified_col_dict:
                    qualified_col_dict[k].append(kk)
                else:
                    qualified_col_dict[k] = [kk]

        # Convert aliases based on user annotations
        aliases = feature_dict.get("aliases", {})
        if aliases:
            click.echo(f"Pre-processed aliases are: {aliases}")
            type_ = df[feature_dict["name"]].dtype.type
            click.echo(f"Detected column type is: {type_}")
            aliases_ = {}
            # The goal below is to identify the data type and then to cast the 
            # alias key from string into that type so that it will match
            # if that fails, just cast it as a string
            for kk, vv in aliases.items():
                try:
                    if issubclass(type_, (int, np.integer)):
                        click.echo("Aliasing: integer detected")
                        aliases_[int(kk)] = vv
                    elif issubclass(type_, (float, np.float16, np.float32, np.float64, np.float128)):
                        click.echo("Aliasing: float detected")
                        aliases_[float(kk)] = vv
                    elif issubclass(type_, (bool, np.bool, np.bool_)):
                        click.echo("Aliasing: boolean detected")
                        if strtobool(kk) == 1:
                            aliases_[True] = vv
                            click.echo("Converted true string to boolean")
                        else:
                            click.echo("Converted false string to boolean")
                            aliases_[False] = vv
                    # Fall back on string
                    else:
                        click.echo("Aliasing: string detected")
                        aliases_[kk] = vv
                except ValueError as e:
                    # Fall back on string
                    click.echo(f"Error: {e}")
                    aliases_[kk] = vv
            click.echo(f"Aliases for {feature_dict['name']} are {aliases_}.")
            df[[feature_dict["name"]]] = df[[feature_dict["name"]]].replace(aliases_)
            
            # Since the user has decided to apply categorical aliases to this feature, we must coerce
            # the entire feature to a string, even if they did not alias every value within the feature
            # the reason for this is to avoid mixed types within the feature (e.g. half int/half string) 
            # since this makes it difficult to visualize
            df[[feature_dict["name"]]] = df[[feature_dict["name"]]].astype(str)

    # perform geocoding if lat/lng are present
    if "lat" in df and "lng" in df:
        df, df_geocode = geocode(admin, df, x="lng", y="lat", gadm=gadm, df_geocode=df_geocode)
    elif "country" in primary_geo_types or ("country" in df and not primary_geo_types):
        # Correct any misspellings etc. in state and admin areas when not
        # geocoding lat and lng above, and country is the primary_geo.
        # This doesn't match names if iso2/iso3 are primary, and when country
        # admin1-3 are moved to features. Exception is when country is present,
        # but nothing is marked as primary.

        # Only geo_code resolve_to_gadm = True fields.
        # Used below when match_geocode_names
        resolve_to_gadm_geotypes = [k["geo_type"] for k in mapper["geo"]  if "resolve_to_gadm" in k and k["resolve_to_gadm"] == True]
        if resolve_to_gadm_geotypes:
            df = match_geo_names(admin, df, resolve_to_gadm_geotypes)

    df_geo_cols = [i for i in df.columns if 'mixmasta_geocoded' in i]
    for c in df_geo_cols:
        df.rename(columns={c: c.replace('_mixmasta_geocoded','')}, inplace=True)

    # protected_cols are the required_cols present in the submitted dataframe.
    protected_cols = list(set(required_cols) & set(df.columns))

    # if a field qualifies a protected field like country, it should have data
    # in each row, unlike features below where the qualifying data appears
    # only on those rows.
    # k: qualified column (str)
    # v: list of columns (str) that qualify k
    for k,v in qualified_col_dict.items():
        if k in protected_cols:
            # k is qualified by the columns in v, and k is a protected column,
            # so extend the width of the output dataset with v for each row.
            protected_cols.extend(v)
            col_order.extend(v)

    # Prepare output by
    # 1. if there are no features, simply reduce the dataframe.
    # or, 2.iterating features to add to feature adn value columns.
    if not features:
        df_out = df[protected_cols]
    else:
        df_out = pd.DataFrame()
        for feat in features:
            using_cols = protected_cols.copy()

            if feat in qualified_col_dict:
                # dict value is a list, so extend.
                using_cols.extend(qualified_col_dict[feat])

                # add a qualifying column name only if not in col_order already
                for c in qualified_col_dict[feat]:
                    if c not in col_order:
                        col_order.append(c)

            join_overlap = False
            try:
                df_ = df[using_cols + [feat+'_mixmasta_left']].copy()
                join_overlap = True
            except:
                df_ = df[using_cols + [feat]].copy()

            try:
                if mapper[feat]["new_col_name"] == None:
                    df_["feature"] = feat
                else:
                    df_["feature"] = mapper[feat]["new_col_name"]
            except:
                df_["feature"] = feat

            if join_overlap:
                df_.rename(columns={f"{feat}_mixmasta_left": "value"}, inplace=True)
            else:
                df_.rename(columns={feat: "value"}, inplace=True)

            # Add feature/value for epochtime as object adds it without decimal
            # places, but it is still saved as a double in the parquet file.
            if len(df_out) == 0:
                if feat in other_time_cols:
                    df_out = df_.astype({'value': object})
                else:
                    df_out = df_
            else:
                if feat in other_time_cols:
                    df_out = df_out.append(df_.astype({'value': object}))
                else:
                    df_out = df_out.append(df_)

    for c in col_order:
        if c not in df_out:
            df_out[c] = None

    # Drop rows with nulls in value column.
    df_out.dropna(axis=0, subset=['value'], inplace=True)

    # Handle any renamed cols being renamed.
    renamed_col_dict = audit_renamed_col_dict(renamed_col_dict)
    
    click.echo("Processed dataframe:")
    click.echo(df_out.head())
    return df_out[col_order], renamed_col_dict, df_geocode

def optimize_df_types(df: pd.DataFrame):
    """
    Pandas will upcast essentially everything. This will use the built-in
    Pandas function to_numeeric to downcast dataframe series to types that use
    less memory e.g. float64 to float32.

    For very large dataframes the memory reduction should translate into
    increased efficieny.
    """
    floats = df.select_dtypes(include=['float64']).columns.tolist()
    df[floats] = df[floats].apply(pd.to_numeric, downcast='float')

    ints = df.select_dtypes(include=['int64']).columns.tolist()
    df[ints] = df[ints].apply(pd.to_numeric, downcast='integer')

    #for col in df.select_dtypes(include=['object']):
    #    num_unique_values = len(df[col].unique())
    #    num_total_values = len(df[col])
    #    if float(num_unique_values) / num_total_values < 0.5:
    #        df[col] = df[col].astype('category')

    return df

def process(fp: str, mp: str, admin: str, output_file: str, write_output = True, gadm=None):
    """
    Parameters
    ----------
    mp: str
        Filename for JSON mapper from spacetag.
        Schema: https://github.com/jataware/spacetag/blob/schema/schema.py
        Example: https://github.com/jataware/spacetag/blob/schema/example.json
    
    gadm: gpd.GeoDataFrame, default None
        optional specification of a GeoDataFrame of GADM shapes of the appropriate
        level (admin2/3) for geocoding
    """

    # Read JSON schema to be mapper.
    mapper = dict
    with open(mp) as f:
        mapper = json.loads(f.read())

    # Validate JSON mapper schema against SpaceTag schema.py model.
    #model = SpaceModel(geo=mapper['geo'], date=mapper['date'], feature=mapper['feature'], meta=mapper['meta'])

    # "meta" portion of schema specifies transformation type
    transform = mapper["meta"]

    # Check transform for meta.geocode_level. Update admin to this if present.
    if (admin == None and "geocode_level" in transform):
        admin = transform["geocode_level"]

    ftype = transform["ftype"]
    if ftype == "geotiff":
        if transform["date"] == "":
            d = None
        else:
            d = transform["date"]

        df = raster2df(
            InRaster = fp,
            feature_name = transform["feature_name"],
            band = int(transform["band"] if "band" in transform and transform["band"] != "" else "0"),
            nodataval = int(transform["null_val"]),
            date = d,
            band_name = transform["band_name"],
            bands = transform["bands"] if "bands" in transform else None
        )
    elif ftype == 'excel':
        df = pd.read_excel(fp, transform['sheet'])
    elif ftype != "csv":
        df = netcdf2df(fp)
    else:
        df = pd.read_csv(fp)

    ## Make mapper contain only keys for date, geo, and feature.
    mapper = { k: mapper[k] for k in mapper.keys() & {"date", "geo", "feature"} }

    ## To speed up normalize(), reduce the memory size of the dataframe by:
    # 1. Optimize the dataframe types.
    # 2. Reset the index so it is a RangeIndex instead of Int64Index.
    df = optimize_df_types(df)
    df.reset_index(inplace=True, drop=True)

    ## Run normalizer.
    norm, renamed_col_dict, df_geocode = normalizer(df, mapper, admin, gadm=gadm)

    # Normalizer will add NaN for missing values, e.g. when appending
    # dataframes with different columns. GADM will return None when geocoding
    # but not finding the entity (e.g. admin3 for United States).
    # Replace None with NaN for consistency.
    norm.fillna(value=np.nan, inplace=True)

    if write_output:
        # If any qualify columns were added, the feature_type must be enforced
        # here because pandas will have cast strings as ints etc.
        qualify_cols = set(norm.columns).difference(set(COL_ORDER))
        for col in qualify_cols:
            for feature_dict in mapper["feature"]:
                if feature_dict["name"] == col and feature_dict["feature_type"] == 'string':
                    norm[col] = norm[col].astype(str)

        # Separate string from other dtypes in value column.
        # This is predicated on the assumption that qualifying feature columns
        # are of a single dtype.

        norm['type'] = norm[['value']].applymap(type)
        norm_str = norm[norm['type']==str]
        norm = norm[norm['type']!=str]
        del(norm_str['type'])
        del(norm['type'])

        # Write parquet files
        norm.to_parquet(f"{output_file}.parquet.gzip", compression="gzip")
        if len(norm_str) > 0:
            norm_str.to_parquet(f"{output_file}_str.parquet.gzip", compression="gzip")

        norm = norm.append(norm_str)

    # Reduce memory size of returned dataframe.
    norm = optimize_df_types(norm)
    norm.reset_index(inplace=True, drop=True)

    return norm, renamed_col_dict

def raster2df(
    InRaster: str,
    feature_name: str = "feature",
    band: int = 0,
    nodataval: int = -9999,
    date: str = None,
    band_name: str = "feature2",
    bands: dict = None
) -> pd.DataFrame:
    """
    Description
    -----------
    Takes the path of a raster (.tiff) file and produces a Geopandas Data Frame.

    Parameters
    ----------
    InRaster: str
        the path of the input raster file
    feature_name: str
        the name of the feature represented by the pixel values
    band: int, default 1
        the band to operate on
    nodataval: int, default -9999
        the value for no data pixels
    date: str, default None
        date associated with the raster (if any)
    band_name: str, default feature2
        the name of the band data e.g. head_count, flooding
    bands: dict, default None
        passed in meta; dictionary of band identifiers and specifies bands to 
        be processed.

    Examples
    --------
    Converting a geotiff of rainfall data into a geopandas dataframe

    >>> df = raster2df('path_to_raster.geotiff', 'rainfall', band=1)

    """
    # open the raster and get some properties
    ds = gdal.OpenShared(InRaster, gdalconst.GA_ReadOnly)
    GeoTrans = ds.GetGeoTransform()
    ColRange = range(ds.RasterXSize)
    RowRange = range(ds.RasterYSize)

    if band == 0 and bands:
        # TODO Will someone import a raster but not use all bands?
        #assert(len(bands) == ds.RasterCount)
        pass

    # Cache the dataframe and value data type.
    df = pd.DataFrame()
    row_data_type = None

    if bands == None:
        columns = ["longitude", "latitude", feature_name]
    else:
        columns=["longitude", "latitude", feature_name, band_name]

    for x in range(1, ds.RasterCount+1):
        # If band has a value, then limit import to single band.
        if band > 0 and band != x:
            continue

        # If no bands in meta, then single-band and use band_name
        # If bands, then process only those in the meta.
        if bands == None:
            band_value = band_name
        elif str(x) in bands:
            band_value = bands[str(x)] 
        else:
            continue
                
        rBand = ds.GetRasterBand(x)  # (band) # first band
        nData = rBand.GetNoDataValue()

        if nData == None:
            logging.warning(f"No nodataval found, setting to {nodataval}")
            nData = np.float32(nodataval)  # set it to something if not set
        else:
            logging.info(f"Nodataval is: {nData} type is : {type(nData)}")

        # specify the center offset (takes the point in middle of pixel)
        HalfX = GeoTrans[1] / 2
        HalfY = GeoTrans[5] / 2

        # Check that NoDataValue is of the same type as the raster data
        RowData = rBand.ReadAsArray(0, 0, ds.RasterXSize, 1)[0]
        row_data_type = type(RowData[0])
        if type(nData) != row_data_type:
            logging.info(
                f"NoData type mismatch: NoDataValue is type {type(nData)} and raster data is type {row_data_type}"
            )
            # e.g. NoDataValue is type <class 'float'> and raster data is type <class 'numpy.float32'>
            # Fix float type mismatches so comparison works below (row_value != nData)
            if row_data_type == np.float32:
                nData = np.float32(nData)
            elif row_data_type == np.float64:
                nData = np.float64(nData)
            elif row_data_type == np.float16:
                nData = np.float16(nData)

        points = []
      
        for ThisRow in RowRange:
            RowData = rBand.ReadAsArray(0, ThisRow, ds.RasterXSize, 1)[0]
            for ThisCol in ColRange:
                # need to exclude NaN values since there is no nodataval
                row_value = RowData[ThisCol]

                if (row_value > nData) and not (np.isnan(row_value)):

                    # TODO: implement filters on valid pixels
                    # for example, the below would ensure pixel values are between -100 and 100
                    # if (RowData[ThisCol] <= 100) and (RowData[ThisCol] >= -100):

                    X = GeoTrans[0] + (ThisCol * GeoTrans[1])
                    Y = GeoTrans[3] + (
                        ThisRow * GeoTrans[5]
                    )  # Y is negative so it's a minus
                    # this gives the upper left of the cell, offset by half a cell to get centre
                    X += HalfX
                    Y += HalfY

                    if bands == None:
                        points.append([X, Y, row_value])
                    else:
                        points.append([X, Y, row_value, band_value])

        # This will make all floats float64, but will be optimized in process().
        new_df = pd.DataFrame(points, columns=columns)

        if df.empty:
            df = new_df
        else:
            #df = df.merge(new_df, left_on=["longitude", "latitude"], right_on=["longitude", "latitude"])
            df = df.append(new_df)

    # Add the date from the mapper.
    if date:
        df["date"] = date

    df.sort_values(by=columns, inplace=True)

    return df

class mixdata:
    def load_gadm2(self):
        cdir = os.path.expanduser("~")
        download_data_folder = f"{cdir}/mixmasta_data"

        # Admin 0 - 2
        gadm_fn = f"gadm36_2.feather"
        gadmDir = f"{download_data_folder}/{gadm_fn}"
        gadm = gf.from_geofeather(gadmDir)
        gadm["country"] = gadm["NAME_0"]
        #gadm["state"] = gadm["NAME_1"]
        gadm["admin1"] = gadm["NAME_1"]
        gadm["admin2"] = gadm["NAME_2"]    
        gadm0 = gadm[["geometry", "country"]]
        #gadm1 = gadm[["geometry", "country", "state", "admin1"]]
        #gadm2 = gadm[["geometry", "country", "state", "admin1", "admin2"]]
        gadm1 = gadm[["geometry", "country", "admin1"]]
        gadm2 = gadm[["geometry", "country", "admin1", "admin2"]]
        
        self.gadm0 = gadm0
        self.gadm1 = gadm1
        self.gadm2 = gadm2

    def load_gadm3(self):
        # Admin 3
        cdir = os.path.expanduser("~")
        download_data_folder = f"{cdir}/mixmasta_data"        
        gadm_fn = f"gadm36_3.feather"
        gadmDir = f"{download_data_folder}/{gadm_fn}"
        gadm3 = gf.from_geofeather(gadmDir)
        gadm3["country"] = gadm3["NAME_0"]
        #gadm3["state"] = gadm3["NAME_1"]
        gadm3["admin1"] = gadm3["NAME_1"]
        gadm3["admin2"] = gadm3["NAME_2"]
        gadm3["admin3"] = gadm3["NAME_3"]
        #gadm3 = gadm3[["geometry", "country", "state", "admin1", "admin2", "admin3"]]
        gadm3 = gadm3[["geometry", "country", "admin1", "admin2", "admin3"]]
        self.gadm3 = gadm3
        

# Testing

# iso testing:
#mp = 'examples/causemosify-tests/mixmasta_ready_annotations_timestampfeature.json'
#fp = 'examples/causemosify-tests/raw_excel_timestampfeature.xlsx'
# build a date qualifier

#mp = 'examples/causemosify-tests/build-a-date-qualifier.json'
#fp = 'examples/causemosify-tests/build-a-date-qualifier_xyzz.csv'

#fp = "examples/causemosify-tests/november_tests_atlasai_assetwealth_allyears_2km.tif"
#mp = "examples/causemosify-tests/november_tests_atlasai_assetwealth_allyears_2km.json"

#fp = "examples/causemosify-tests/flood_monthly.tif"
#mp = "examples/causemosify-tests/flood_monthly.json"

#fp = "examples/causemosify-tests/Kenya - Admin1_Pasture_NDVI_2019-2021 converted.csv"
#mp = "examples/causemosify-tests/kenya_pasture.json"

#fp = "examples/causemosify-tests/rainfall_error.xlsx"
#mp = "examples/causemosify-tests/rainfall_error.json"
#geo = 'admin2'
#outf = 'examples/causemosify-tests/testing'

#mp = 'tests/inputs/test3_qualifies.json'
#fp = 'tests/inputs/test3_qualifies.csv'
#geo = 'admin2'
#outf = 'tests/outputs/unittests'

#fp = "examples/causemosify-tests/hoa_conflict.csv"
#mp = "examples/causemosify-tests/hoa_conflict.json"
#geo = 'admin2'
#outf = 'examples/causemosify-tests'

#fp = "examples/causemosify-tests/maxent_Ethiopia_precipChange.0.8tempChange.-0.3.tif"
#mp = "examples/causemosify-tests/maxent_Ethiopia_precipChange.0.8tempChange.-0.3.json"

#fp = 'examples/causemosify-tests/SouthSudan_2017_Apr_hires_masked_malnut.tiff'
#mp = 'examples/causemosify-tests/SouthSudan_2017_Apr_hires_masked_malnut.json'
#geo = 'admin2'
#outf = 'examples/causemosify-tests'

#fp = "examples/causemosify-tests/example.csv"
#mp = "examples/causemosify-tests/example.json"
#geo = 'admin2'
#outf = 'examples/causemosify-tests'

#start_time = timeit.default_timer()
#df = pd.DataFrame()
#print('processing...')
#df, dct = process(fp, mp, geo, outf)
#print('process time', timeit.default_timer() - start_time)
#cols = ['timestamp','country','admin1','admin2','admin3','lat','lng','feature','value']
#df.sort_values(by=cols, inplace=True)
#df.reset_index(drop=True, inplace=True)
#df.to_csv("tests/outputs/test7_single_band_tif_output.csv", index = False)
#print('\n', df.head())
#print('\n', df.tail())
#print('\n', df.shape)
#print('\nrenamed column dictionary\n', dct)
#print(f"shape\n{df.shape}\ninfo\n{df.info()}\nmemory usage\n{df.memory_usage(index=True, deep=True)}\n{df.memory_usage(index=True, deep=True).sum()}")
