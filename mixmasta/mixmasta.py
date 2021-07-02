"""Main module."""
import json
import logging
import os
import sys
from datetime import datetime
from typing import List

import geofeather as gf
import geopandas as gpd
import numpy as np
import pandas as pd
import requests
import xarray as xr
from osgeo import gdal, gdalconst
from shapely import speedups
from shapely.geometry import Point

from pathlib import Path

import fuzzywuzzy
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
#import timeit
#from .spacetag_schema import SpaceModel

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
        t_ = int(datetime.strptime(t, time_format).timestamp())
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
    admin: str, df: pd.DataFrame, x: str = "longitude", y: str = "latitude"
) -> pd.DataFrame:
    """
    Description
    -----------
    Takes a dataframe containing coordinate data and geocodes it to GADM (https://gadm.org/)

    GEOCODES to ADMIN 2 OR 3 LEVEL

    Parameters
    ----------
    admin: str
        the level to geocode to. Either 'admin2' or 'admin3'
    df: pd.DataFrame
        a pandas dataframe containing point data
    x: str, default 'longitude'
        the name of the column containing longitude information
    y: str, default 'latitude'
        the name of the column containing latitude data

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

    if admin == "admin2":

        gadm_fn = f"gadm36_2.feather"
        gadmDir = f"{download_data_folder}/{gadm_fn}"
        gadm = gf.from_geofeather(gadmDir)

        gadm["country"] = gadm["NAME_0"]
        gadm["state"] = gadm["NAME_1"]
        gadm["admin1"] = gadm["NAME_1"]
        gadm["admin2"] = gadm["NAME_2"]
        gadm = gadm[["geometry", "country", "state", "admin1", "admin2"]]
    elif admin == "admin3":

        gadm_fn = f"gadm36_3.feather"
        gadmDir = f"{download_data_folder}/{gadm_fn}"
        gadm = gf.from_geofeather(gadmDir)

        gadm["country"] = gadm["NAME_0"]
        gadm["state"] = gadm["NAME_1"]
        gadm["admin1"] = gadm["NAME_1"]
        gadm["admin2"] = gadm["NAME_2"]
        gadm["admin3"] = gadm["NAME_3"]
        gadm = gadm[["geometry", "country", "state", "admin1", "admin2", "admin3"]]


    df["geometry"] = df.apply(lambda row: Point(row[x], row[y]), axis=1)
    gdf = gpd.GeoDataFrame(df)

    # Spatial merge on GADM to obtain admin areas
    gdf = gpd.sjoin(gdf, gadm, how="left", op="within", lsuffix="mixmasta_left", rsuffix="mixmasta_geocoded")
    del gdf["geometry"]
    del gdf["index_mixmasta_geocoded"]

    return pd.DataFrame(gdf)

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

def generate_timestamp(series: pd.Series, date_mapper: dict, column_name: str) -> pd.Series:
    """
    Description
    -----------
    Generates a pandas series in M/D/Y H:M format from collected Month, Day,
    Year, Hour, Minute values in a parameter pandas series. Fills Month, Day,
    Year, Hour, Minute if missing, but at least one should be present to
    generate the value. Used to generate a timestamp in the absence of one in
    the data.

    Parameters
    ----------
    row: pd.Series
        a pandas series containing date data
    date_mapper: dict
        a schema mapping (JSON) for the dataframe filtered for "date_type" equal to
        Day, Month, or Year.
    column_name: str
        name of the new column e.g. timestamp for primary_time, year1month1day1
        for a concatneated name from associated date fields.

    Examples
    --------
    This example adds the generated series to the source dataframe.
    >>> df = df.join(df.apply(generate_timestamp, date_mapper=date_mapper,
            column_name="year1month1day", axis=1))
    """

    # Default to 1/1/1970
    day = 1
    month = 1
    year = 70

    for kk, vv in date_mapper.items():
        if vv["date_type"] == "day":
            day = series[kk]
        elif vv["date_type"] == "month":
            month = series[kk]
        elif vv["date_type"] == "year":
            year = str(series[kk])

    timestamp =  '/'.join([str(month),str(day),str(year)])
    return pd.Series(timestamp, index=[column_name])

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

def match_geo_names(admin: str, df: pd.DataFrame) -> pd.DataFrame:
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

    Result
    ------
    A pandas.Dataframe produced by modifying the parameter df.

    """
    flag = speedups.available
    if flag == True:
        speedups.enable()

    cdir = os.path.expanduser("~")
    download_data_folder = f"{cdir}/mixmasta_data"

    #start_time = timeit.default_timer()

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

    #print('load time', timeit.default_timer() - start_time)
    #start_time = timeit.default_timer()

    # Filter GADM for countries in df.
    countries = df["country"].unique()

    # Correct country names.
    gadm_country_list = gadm["country"].unique()
    unknowns = df[~df.country.isin(gadm_country_list)].country.tolist()
    for unk in unknowns:
        match = fuzzywuzzy.process.extractOne(unk, gadm_country_list, scorer=fuzz.ratio)
        if match != None:
            df.loc[df.country == unk, 'country'] = match[0]

    # Filter GADM dicitonary for only those countries (ie. speed up)
    gadm = gadm[gadm["country"].isin(countries)]

    # Loop by country using gadm dict filtered for that country.
    for c in countries:
        # The following ignores admin1 / admin2 pairs; it only cares if those
        # values exist for the appropriate country.

        # Get list of admin1 values in df but not in gadm. Reduce list for country.
        admin1_list = gadm[gadm.country==c]["admin1"].unique()
        if admin1_list is not None and all(admin1_list) and 'admin1' in df:
            unknowns = df[(df.country == c) & ~df.admin1.isin(admin1_list)].admin1.tolist()
            unknowns = [x for x in unknowns if pd.notnull(x) and x.strip()] # remove Nan
            for unk in unknowns:
                match = fuzzywuzzy.process.extractOne(unk, admin1_list, scorer=fuzz.ratio)
                if match != None:
                    df.loc[df.admin1 == unk, 'admin1'] = match[0]

        # Get list of admin2 values in df but not in gadm. Reduce list for country.
        admin2_list = gadm[gadm.country==c ]["admin2"].unique()
        if admin2_list is not None and all(admin2_list) and 'admin2' in df:
            unknowns = df[(df.country == c) & ~df.admin2.isin(admin2_list)].admin2.tolist()
            unknowns = [x for x in unknowns if pd.notnull(x) and x.strip()] # remove Nan
            for unk in unknowns:
                match = fuzzywuzzy.process.extractOne(unk, admin2_list, scorer=fuzz.ratio)
                if match != None:
                    df.loc[df.admin2 == unk, 'admin2'] = match[0]

        if admin =='admin3':
            # Get list of admin3 values in df but not in gadm. Reduce list for country.
            admin3_list = gadm[gadm.country==c]["admin3"].unique()
            if admin3_list is not None and all(admin3_list) and 'admin3' in df:
                unknowns = df[(df.country == c) & ~df.admin3.isin(admin3_list)].admin3.tolist()
                unknowns = [x for x in unknowns if pd.notnull(x) and x.strip()] # remove Nan
                for unk in unknowns:
                    match = fuzzywuzzy.process.extractOne(unk, admin3_list, scorer=fuzz.ratio)
                    if match != None:
                        df.loc[df.admin3 == unk, 'admin3'] = match[0]

    #print('processing time', timeit.default_timer() - start_time)

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

def normalizer(df: pd.DataFrame, mapper: dict, admin: str) -> (pd.DataFrame, dict):
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

    Examples
    --------
    >>> df_norm = normalizer(df, mapper, 'admin3')
    """
    col_order = [
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

    required_cols = [
        "timestamp",
        "country",
        "admin1",
        "admin2",
        "admin3",
        "lat",
        "lng",
    ]

    # Create a dictionary of list: colnames: new col name, and modify df and
    # mapper for any column name collisions.
    df, mapper, renamed_col_dict = handle_colname_collisions(df, mapper, col_order)

    # mapper is a dictionary of lists of dictionaries.
    primary_time_cols = [k['name'] for k in mapper['date'] if 'primary_date' in k and k['primary_date'] == True]
    other_time_cols   = [k['name'] for k in mapper['date'] if 'primary_date' not in k or k['primary_date'] == False]
    primary_geo_cols  = [k["name"] for k in mapper["geo"]  if "primary_geo"  in k and k["primary_geo"] == True]

    # dictionary for columns qualified by another column.
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
                df[kk] = df[kk].apply(lambda x: format_time(str(x), date_dict["time_format"], validate=False))
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
                df[kk] = df[kk].apply(lambda x: format_time(str(x), date_dict["time_format"], validate=False))
                # If three are no assigned primary_time columns, make this the
                # primary_time timestamp column, and keep as a feature so the
                # column_name meaning is not lost.
                if not primary_time_cols and not "timestamp" in df.columns:
                    df.rename(columns={kk: "timestamp"}, inplace=True)
                    renamed_col_dict[ staple_col_name ] = [kk]
                # All not primary_time, not associated_columns fields are pushed to features.
                features.append(kk)

            elif date_dict["date_type"] in ["day","month","year"] and 'associated_columns' in date_dict and date_dict["associated_columns"]:
                # Various date columns have been associated by the user and are not primary_date.
                # convert them to epoch then store them as a feature
                # (instead of storing them as separate uncombined features).
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
        new_series = date_df.apply(generate_timestamp, date_mapper=primary_date_group_mapper, column_name="timestamp", axis=1)
        df = df.join(new_series)

        # Determine the correct time format for the new date column, and
        # convert to epoch time.
        time_formatter = generate_timestamp_format(primary_date_group_mapper)
        df['timestamp'] = df["timestamp"].apply(lambda x: format_time(str(x), time_formatter, validate=False))

        # Let SpaceTag know those date columns were renamed to timestamp.
        #renamed_col_dict[ "timestamp" ] = assoc_fields # 7/2/2021 do not include primary cols

    while other_date_group_mapper:
        # Various date columns have been associated by the user and are not primary_date.
        # Convert to epoch time and store as a feature, do not store these separately in features.
        # Control for possibility of more than one set of assciated_columns.
        date_field_tuple = other_date_group_mapper.popitem()
        assoc_fields = [k[1] for k in date_field_tuple[1]['associated_columns'].items()]
        assoc_columns = { f : other_date_group_mapper.pop(f, None) for f in assoc_fields }
        assoc_columns[date_field_tuple[0]] = date_field_tuple[1]
        assoc_fields.append(date_field_tuple[0])

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

        # Now generate the timestamp from date_df and add the new_column to df.
        new_series = date_df.apply(generate_timestamp, date_mapper=assoc_columns, column_name=new_column_name, axis=1)
        df = df.join(new_series)

        # Determine the correct time format for the new date column, and
        # convert to epoch time.
        time_formatter = generate_timestamp_format(assoc_columns)
        df[new_column_name] = df[new_column_name].apply(lambda x: format_time(str(x), time_formatter, validate=False))

        # Let SpaceTag know those date columns were renamed to a new column.
        renamed_col_dict[ new_column_name] = assoc_fields

        # timestamp is a protected column, so don't add to features.
        if new_column_name != "timestamp":
            features.append(new_column_name)

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
            elif geo_dict["geo_type"] == "country" and kk != "country":
                # force the country column to be named country
                staple_col_name = "country"
                df.rename(columns={kk: staple_col_name}, inplace=True)
                #renamed_col_dict[staple_col_name] = [kk] # 7/2/2021 do not include primary cols
            elif str(geo_dict["geo_type"]).lower() in ["iso2", "iso3"]:
                # use the ISO2 or ISO3 column as country

                # use ISO2/3 lookup dictionary to change ISO to country name.
                iso_list = df[kk].unique().tolist()
                dct = get_iso_country_dict(iso_list)
                df[kk] = df[kk].apply(lambda x: dct[x] if x in dct else x)

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
                if geo_dict["geo_type"] == "country":
                    df["country"] = df[kk]
                    renamed_col_dict["country"] = [kk]
                    continue
                if geo_dict["geo_type"] == "state/territory":
                    df["admin1"] = df[kk]
                    renamed_col_dict["admin1"] = [kk]
                    continue
                if geo_dict["geo_type"] == "county/district":
                    df["admin2"] = df[kk]
                    renamed_col_dict["admin2"] = [kk]
                    continue
                if geo_dict["geo_type"] == "municipality/town":
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

    # perform geocoding if lat/lng are present
    if "lat" in df and "lng" in df:
        df = geocode(admin, df, x="lng", y="lat")
    #elif "country" in df:
    elif "country" in primary_geo_cols or ("country" in df and not primary_geo_cols):
        # Correct any misspellings etc. in state and admin areas when not
        # geocoding lat and lng above, and country is the primary_geo.
        # This don't match names if iso2/iso3 are primary, and when country
        # admin1-3 are moved to features. Exception is when country is present,
        # but nothing is marked as primary.
        df = match_geo_names(admin, df)

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
                col_order.extend(qualified_col_dict[feat])

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

    # Handle any renamed cols being renamed.
    renamed_col_dict = audit_renamed_col_dict(renamed_col_dict)

    return df_out[col_order], renamed_col_dict

def process(fp: str, mp: str, admin: str, output_file: str):
    """
    Parameters
    ----------
    mp: str
        Filename for JSON mapper from spacetag.
        Schema: https://github.com/jataware/spacetag/blob/schema/schema.py
        Example: https://github.com/jataware/spacetag/blob/schema/example.json

    """

    # Read JSON schema to be mapper.
    mapper = json.loads(open(mp).read())

    # Validate JSON mapper schema against SpaceTag schema.py model.
    #model = SpaceModel(geo=mapper['geo'], date=mapper['date'], feature=mapper['feature'], meta=mapper['meta'])

    # "meta" portion of schema specifies transformation type
    transform = mapper["meta"]

    ftype = transform["ftype"]
    if ftype == "geotiff":
        if transform["date"] == "":
            d = None
        else:
            d = transform["date"]
        df = raster2df(
            fp,
            transform["feature_name"],
            int(transform["band"]),
            int(transform["null_val"]),
            d,
        )
    elif ftype == 'excel':
        df = pd.read_excel(fp, transform['sheet'])
    elif ftype != "csv":
        df = netcdf2df(fp)
    else:
        df = pd.read_csv(fp)

    # Make mapper contain only keys for date, geo, and feature.
    mapper = { k: mapper[k] for k in mapper.keys() & {"date", "geo", "feature"} }

    # Run normailizer.
    norm, renamed_col_dict = normalizer(df, mapper, admin)
    #norm = normalizer(df, mapper, admin)

    # Normalizer will add NaN for missing values, e.g. when appending
    # dataframes with different columns. GADM will return None when geocoding
    # but not finding the entity (e.g. admin3 for United States).
    # Replace None with NaN for consistency.
    norm.fillna(value=np.nan, inplace=True)

    # Separate string values from others
    norm['type'] = norm[['value']].applymap(type)
    norm_str = norm[norm['type']==str]
    norm = norm[norm['type']!=str]
    del(norm_str['type'])
    del(norm['type'])

    # Write parquet files
    norm.to_parquet(f"{output_file}.parquet.gzip", compression="gzip")
    if len(norm_str) > 0:
        norm_str.to_parquet(f"{output_file}_str.parquet.gzip", compression="gzip")

    # Testing

    #print('\n', norm.append(norm_str).head(50))
    #print('\n', norm.append(norm_str).tail(50))
    """
    print('\n', norm.head(50))
    print('\n', norm.tail(50))
    print('\n', norm_str.head(50))
    print('\n', renamed_col_dict)
    """

    return norm.append(norm_str), renamed_col_dict

def raster2df(
    InRaster: str,
    feature_name: str = "feature",
    band: int = 1,
    nodataval: int = -9999,
    date: str = None,
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
    rBand = ds.GetRasterBand(band)  # first band
    nData = rBand.GetNoDataValue()
    if nData == None:
        logging.info(f"No nodataval found, setting to {nodataval}")
        nData = np.float32(nodataval)  # set it to something if not set
    else:
        logging.info(f"Nodataval is: {nData}")

    # specify the center offset (takes the point in middle of pixel)
    HalfX = GeoTrans[1] / 2
    HalfY = GeoTrans[5] / 2

    # Check that NoDataValue is of the same type as the raster data
    RowData = rBand.ReadAsArray(0, 0, ds.RasterXSize, 1)[0]
    if type(nData) != type(RowData[0]):
        logging.warning(
            f"NoData type mismatch: NoDataValue is type {type(nData)} and raster data is type {type(RowData[0])}"
        )

    points = []
    for ThisRow in RowRange:
        RowData = rBand.ReadAsArray(0, ThisRow, ds.RasterXSize, 1)[0]
        for ThisCol in ColRange:
            # need to exclude NaN values since there is no nodataval
            if (RowData[ThisCol] != nData) and not (np.isnan(RowData[ThisCol])):

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

                points.append([X, Y, RowData[ThisCol]])

    df = pd.DataFrame(points, columns=["longitude", "latitude", feature_name])
    if date:
        df["date"] = date
    return df

# Testing
"""
mp = 'examples/causemosify-tests/mixmasta_ready_annotations_timestampfeature.json'
fp = 'examples/causemosify-tests/raw_excel_timestampfeature.xlsx'
geo = 'admin3'
outf = 'examples/causemosify-tests/testing'

process(fp, mp, geo, outf)

mapper = json.loads(open(mp).read())
mapper = { k: mapper[k] for k in mapper.keys() & {"date", "geo", "feature"} }
df = pd.read_csv(fp)
norm, changed_cols = normalizer(df, mapper, geo)
print('\n', norm.head(50))
print('\n', norm.tail(50))

"""