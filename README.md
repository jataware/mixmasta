# mixmasta

A library for common scientific model transforms. This library enables fast and intuitive transforms including:

* Converting a `geotiff` to a `csv`
* Converting a `NetCDF` to a `csv`
* Geocoding `csv` data that contains latitude and longitude


## Setup

Ensure you have a working installation of [GDAL](https://trac.osgeo.org/gdal/wiki/FAQInstallationAndBuilding#FAQ-InstallationandBuilding)

You also need to ensure that `numpy` is installed prior to `mixmasta` installation. This is an artifact of GDAL, which will build incorrectly if `numpy` is not already configured:

```
pip install numpy==1.20.1
pip install mixmasta
```

> Note: if you had a prior installation of GDAL you may need to run `pip install mixmasta --no-cache-dir` in a clean environment.

You must install the GADM2 data with:

```
mixmasta download
```

## Usage


Examples can be found in the `examples` directory.

Convert a geotiff to a dataframe with:

```
from mixmasta import mixmasta as mix
df = mix.raster2df('chirps-v2.0.2021.01.3.tif', feature_name='rainfall', band=1)
```

Note that you should specify the data band of the geotiff to process if it is multi-band. You may also specify the name of the feature column to produce. You may optionally specify a `date` if the geotiff has an associated date. For example:

Convert a NetCDF to a dataframe with:

```
from mixmasta import mixmasta as mix
df = mix.netcdf2df('tos_O1_2001-2002.nc')
```

Geocode a dataframe:

```
from mixmasta import mixmasta as mix

# First, load in the geotiff as a dataframe
df = mix.raster2df('chirps-v2.0.2021.01.3.tif', feature_name='rainfall', band=1)

# next, we can geocode the dataframe by specifying the names of the x and y columns
# in this case, they are 'longitude' and 'latitude'
df_g = mix.geocode(df, x='longitude', y='latitude')
```

## Credits

This package was created with [Cookiecutter](https://github.com/audreyr/cookiecutter) and the [audreyr/cookiecutter-pypackage](https://github.com/audreyr/cookiecutter-pypackage) project template.
