"""File processor functions that return dataframes when fed a file in order to enable downstream processing.

Raises:
    Exception: _description_
    Exception: _description_

Returns:
    pandas.Dataframe: A dataframe of the input data to be used downstream
"""

import logging

import numpy
from osgeo import gdal, gdalconst
import pandas
import xarray


def process_file_by_filetype(filepath, file_type, transformation_metadata):
    dataframe = None
    if file_type == "geotiff":
        dataframe = raster2df(
            InRaster=filepath,
            feature_name=transformation_metadata["feature_name"],
            band=int(
                transformation_metadata["band"]
                if "band" in transformation_metadata
                and transformation_metadata["band"] != ""
                else "0"
            ),
            nodataval=int(transformation_metadata["null_val"]),
            date=transformation_metadata["date"]
            if (
                "date" in transformation_metadata
                and transformation_metadata["date"] != ""
            )
            else None,
            band_name=transformation_metadata["band_name"],
            bands=transformation_metadata["bands"]
            if "bands" in transformation_metadata
            else None,
            band_type=transformation_metadata["band_type"]
            if "band_type" in transformation_metadata
            else "category",
        )
    elif file_type == "excel":
        dataframe = pandas.read_excel(filepath, transformation_metadata["sheet"])
    elif file_type != "csv":
        dataframe = netcdf2df(filepath)
    else:
        dataframe = pandas.read_csv(filepath)

    if dataframe is None:
        raise TypeError(
            "File failed to process, dataframe returned as None type object"
        )
    return dataframe


def raster2df(
    InRaster: str,
    feature_name: str = "feature",
    band: int = 0,
    nodataval: int = -9999,
    date: str = None,
    band_name: str = "feature2",
    bands: dict = None,
    band_type: str = "category",
) -> pandas.DataFrame:
    """
    Description
    -----------
    Takes the path of a raster (.tiff) file and produces a Geopandas Data Frame.

    Parameters
    ----------
    InRaster: str
        the path of the inumpyut raster file
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
    band_type: str, default category
        Specifies band type e.g. category or datetime. If datetime, this data goes into the date column.

    Examples
    --------
    Converting a geotiff of rainfall data into a geopandas dataframe

    >>> df = raster2df('path_to_raster.geotiff', 'rainfall', band=1)

    """
    # open the raster and get some properties
    data_source = gdal.OpenShared(InRaster, gdalconst.GA_ReadOnly)
    GeoTrans = data_source.GetGeoTransform()
    ColRange = range(data_source.RasterXSize)
    RowRange = range(data_source.RasterYSize)

    # Creating variables for the dataframe and value data type.
    dataframe = pandas.DataFrame()

    for x in range(1, data_source.RasterCount + 1):
        # If band has a value, then limit import to the single specified band.
        if band > 0 and band != x:
            continue

        # If no bands in meta, then single-band and use band_name
        # If bands, then process only those in the meta.
        if not bands:
            band_value = band_name
            logging.info(
                f"Single band detected. Bands: {bands}, band_name: {band_name}, feature_name: {feature_name}"
            )
        elif str(x) in bands:
            band_value = bands[str(x)]
            logging.info(
                f"Multi-band detected Bands: {bands}, band_name: {band_name}, feature_name: {feature_name}"
            )
        elif str(x) not in bands:
            # Processing a band not specified in the meta, so skip it
            logging.info(f"Skipping band {x} since it is not specified in {bands}.")
            continue
        else:
            raise Exception(
                f"Neither single nor multiple bands specified in meta. Current band: {x}, Bands: {bands}, band_name: {band_name}, feature_name: {feature_name}"
            )

        # Create columns for the dataframe.
        if not bands:
            columns = ["longitude", "latitude", feature_name]
            logging.info(f"Single band detected. Columns are: {columns}")
        elif band_type == "datetime":
            columns = ["longitude", "latitude", "date", feature_name]
            logging.info(f"Datetime multiband detected. Columns are: {columns}")
        elif band_type == "category":
            # categorical multi-band; add columns during processing.
            columns = ["longitude", "latitude", band_value]
            logging.info(f"Categorical multiband detected. Columns are: {columns}")
        else:
            raise Exception(
                f"During column processing, neither single nor multiple bands specified in meta. Bands: {bands}, band_name: {band_name}, feature_name: {feature_name}"
            )

        rBand = data_source.GetRasterBand(x)
        nData = rBand.GetNoDataValue()

        if nData == None:
            logging.warning(f"No nodataval found, setting to {nodataval}")
            nData = numpy.float32(nodataval)  # set it to something if not set
        else:
            logging.info(f"Nodataval is: {nData} type is : {type(nData)}")

        # specify the center offset (takes the point in middle of pixel)
        HalfX = GeoTrans[1] / 2
        HalfY = GeoTrans[5] / 2

        # Check that NoDataValue is of the same type as the raster data
        RowData = rBand.ReadAsArray(0, 0, data_source.RasterXSize, 1)[0]
        row_data_type = type(RowData[0])
        if type(nData) != row_data_type:
            logging.info(
                f"NoData type mismatch: NoDataValue is type {type(nData)} and raster data is type {row_data_type}"
            )
            # e.g. NoDataValue is type <class 'float'> and raster data is type <class 'numpy.float32'>
            # Fix float type mismatches so comparison works below (row_value != nData)
            if row_data_type == numpy.float32:
                nData = numpy.float32(nData)
            elif row_data_type == numpy.float64:
                nData = numpy.float64(nData)
            elif row_data_type == numpy.float16:
                nData = numpy.float16(nData)

        points = []

        for ThisRow in RowRange:
            RowData = rBand.ReadAsArray(0, ThisRow, data_source.RasterXSize, 1)[0]
            for ThisCol in ColRange:
                # need to exclude NaN values since there is no nodataval
                row_value = RowData[ThisCol]

                if (row_value > nData) and not (numpy.isnan(row_value)):

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

                    # Add the data row to the dataframe.
                    if bands == None:
                        points.append([X, Y, row_value])
                    elif band_type == "datetime":
                        points.append([X, Y, band_value, row_value])
                    else:
                        points.append([X, Y, row_value])

        # This will make all floats float64, but will be optimized in process().
        new_dataframe = pandas.DataFrame(points, columns=columns)

        if dataframe.empty:
            dataframe = new_dataframe
        else:
            # df = df.merge(new_df, left_on=["longitude", "latitude"], right_on=["longitude", "latitude"])
            if bands and band_type != "datetime":
                # df.join(new_df, on=["longitude", "latitude"])
                dataframe = dataframe.merge(
                    new_dataframe,
                    left_on=["longitude", "latitude"],
                    right_on=["longitude", "latitude"],
                )
            else:
                dataframe = dataframe.append(new_dataframe)

    # Add the date from the mapper.
    if date and band_type != "datetime":
        dataframe["date"] = date

    dataframe.sort_values(by=columns, inplace=True)

    return dataframe


def netcdf2df(netcdf: str) -> pandas.DataFrame:
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
        data_source = xarray.open_dataset(netcdf)
    except:
        raise AssertionError(
            f"Improperly formatted netCDF file ({netcdf}), xarray could not convert it to a dataframe."
        )

    dataframe = data_source.to_dataframe()
    final_dataframe = dataframe.reset_index()

    return final_dataframe
