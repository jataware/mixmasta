# mixmasta

![image](https://img.shields.io/pypi/v/mixmasta.svg%0A%20%20%20%20%20:target:%20https://pypi.python.org/pypi/mixmasta)

![image](https://img.shields.io/travis/brandomr/mixmasta.svg%0A%20%20%20%20%20:target:%20https://travis-ci.com/brandomr/mixmasta)

![image](https://readthedocs.org/projects/mixmasta/badge/?version=latest%0A%20%20%20%20%20:target:%20https://mixmasta.readthedocs.io/en/latest/?badge=latest%0A%20%20%20%20%20:alt:%20Documentation%20Status)

A library for common scientific model transforms

-   Free software: MIT license
-   Documentation: <https://mixmasta.readthedocs.io>.

## Setup

Ensure you have a working installation of [GDAL](https://trac.osgeo.org/gdal/wiki/FAQInstallationAndBuilding#FAQ-InstallationandBuilding)

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

Note that you should specify the data band of the geotiff to process if it is multi-band. You may also specify the name of the feature column to produce.

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
