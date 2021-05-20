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
import xarray as xr
from osgeo import gdal, gdalconst
from shapely import speedups
from shapely.geometry import Point

if not sys.warnoptions:
    import warnings

    warnings.simplefilter("ignore")

logger = logging.getLogger(__name__)


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

    if admin == "admin3":

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
    gdf = gpd.sjoin(gdf, gadm, how="left", op="within")
    del gdf["geometry"]
    del gdf["index_right"]

    return pd.DataFrame(gdf)


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
        print(e)
        if validate:
            raise Exception(e)
        else:
            return None


def normalizer(df: pd.DataFrame, mapper: dict, admin: str) -> pd.DataFrame:
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

    # subset dataframe for only columns in mapper
    time_cols = [kk for kk, vv in mapper.items() if vv["Primary_time"] == "true"]
    geo_cols = [kk for kk, vv in mapper.items() if vv["Primary_geo"] == "true"]
    df = df[list(mapper.keys())]

    # Rename protected columns
    # and perform type conversion on the time column
    features = []
    for kk, vv in mapper.items():

        if kk in time_cols:
            df[kk] = df[kk].apply(
                lambda x: format_time(str(x), vv["Time_format"], validate=False)
            )
            staple_col_name = "timestamp"
            df.rename(columns={kk: staple_col_name}, inplace=True)
        elif kk in geo_cols:
            if vv["Geo"] == "Latitude":
                staple_col_name = "lat"
                df.rename(columns={kk: staple_col_name}, inplace=True)
            elif vv["Geo"] == "Longitude":
                staple_col_name = "lng"
                df.rename(columns={kk: staple_col_name}, inplace=True)
            elif vv["Geo"] == "Coordinates":
                c_f = vv["Coordinate_format"]
                cords = df[kk].values
                if c_f == "Longitude,Latitude":
                    lats = [x for x in cords.split(",")[1]]
                    longs = [x for x in cords.split(",")[0]]
                else:
                    lats = [x for x in cords.split(",")[0]]
                    longs = [x for x in cords.split(",")[1]]
                df["lng"] = longs
                df["lat"] = lats
                del df[kk]
        else:
            features.append(kk)

    # perform geocoding if lat/lng are present
    if "lat" in df and "lng" in df:
        df = geocode(admin, df, x="lng", y="lat")

    # reshape the dataframe into a "long" format
    protected_cols = list(set(df.columns) - set(features))
    df_out = pd.DataFrame()
    for feat in features:
        df_ = df[protected_cols + [feat]].copy()
        try:
            if mapper[feat]["new_col_name"] == None:
                df_["feature"] = feat
            else:
                df_["feature"] = mapper[feat]["new_col_name"]
        except:
            df_["feature"] = feat
        df_.rename(columns={feat: "value"}, inplace=True)
        if len(df_out) == 0:
            df_out = df_
        else:
            df_out = df_out.append(df_)

    for c in col_order:
        if c not in df_out:
            df_out[c] = None
    print(df_out.head())
    return df_out[col_order]


def process(fp: str, mp: str, admin: str, output_file: str):
    mapper = json.loads(open(mp).read())
    transform = mapper["meta"]
    mapper = mapper["annotations"]

    if transform["ftype"] == "Geotiff":
        if transform["Date"] == "":
            d = None
        else:
            d = transform["Date"]
        df = raster2df(
            fp,
            transform["Feature_name"],
            int(transform["Band"]),
            int(transform["Null_val"]),
            d,
        )

    elif transform["ftype"] != "csv":
        df = netcdf2df(fp)
    else:
        df = pd.read_csv(fp)

    norm = normalizer(df, mapper, admin)

    # Separate string values from others
    norm['type'] = norm[['value']].applymap(type)   
    norm_str = norm[norm['type']==str]
    norm = norm[norm['type']!=str]
    del(norm_str['type'])
    del(norm['type'])    
              
    norm.to_parquet(f"{output_file}.parquet.gzip", compression="gzip")
    norm_str.to_parquet(f"{output_file}_str.parquet.gzip", compression="gzip")
