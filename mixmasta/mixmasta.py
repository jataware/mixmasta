"""Main module."""
import logging
from typing import List
import pandas as pd
import xarray as xr
from shapely.geometry import Point
import geopandas as gpd
import numpy as np
from osgeo import gdal
from osgeo import gdalconst

logger = logging.getLogger(__name__)

def netcdf2df(
    netcdf: str
) -> pd.DataFrame:
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
	InRaster: str, feature_name: str='feature', band: int=1, nodataval: int=-9999
) -> pd.DataFrame:
    '''
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

   	Examples
    --------
    Converting a geotiff of rainfall data into a geopandas dataframe

	>>> gdf = raster2gpd('path_to_raster.geotiff', 'rainfall', band=1)

    '''

    # open the raster and get some properties
    ds       = gdal.OpenShared(InRaster,gdalconst.GA_ReadOnly)
    GeoTrans = ds.GetGeoTransform()
    ColRange = range(ds.RasterXSize)
    RowRange = range(ds.RasterYSize)
    rBand    = ds.GetRasterBand(band) # first band
    nData    = rBand.GetNoDataValue()
    if nData == None:
        logging.info(f"No nodataval found, setting to {nodataval}")
        nData = np.float32(nodataval) # set it to something if not set
    else:
        logging.info(f"Nodataval is: {nData}")

    # specify the center offset (takes the point in middle of pixel)
    HalfX    = GeoTrans[1] / 2
    HalfY    = GeoTrans[5] / 2

    # Check that NoDataValue is of the same type as the raster data
    RowData = rBand.ReadAsArray(0,0,ds.RasterXSize,1)[0]
    if type(nData) != type(RowData[0]):
        logging.warning(f"NoData type mismatch: NoDataValue is type {type(nData)} and raster data is type {type(RowData[0])}")
        

    points = []
    for ThisRow in RowRange:
        RowData = rBand.ReadAsArray(0,ThisRow,ds.RasterXSize,1)[0]
        for ThisCol in ColRange:
            # need to exclude NaN values since there is no nodataval
            if (RowData[ThisCol] != nData) and not (np.isnan(RowData[ThisCol])):
                
                # TODO: implement filters on valid pixels
                # for example, the below would ensure pixel values are between -100 and 100
                #if (RowData[ThisCol] <= 100) and (RowData[ThisCol] >= -100):

                X = GeoTrans[0] + ( ThisCol * GeoTrans[1] )
                Y = GeoTrans[3] + ( ThisRow * GeoTrans[5] ) # Y is negative so it's a minus
                # this gives the upper left of the cell, offset by half a cell to get centre
                X += HalfX
                Y += HalfY

                points.append([X,Y,RowData[ThisCol]])

    return pd.DataFrame(points, columns=['longitude','latitude',feature_name])

