# mixmasta

A library for common scientific model transforms. This library enables fast and intuitive transforms including:

* Converting a `geotiff` to a `csv`
* Converting a `NetCDF` to a `csv`
* Geocoding `csv`, `xls`, and `xlsx` data that contains latitude and longitude


## Setup

See `docs/docker.md` for instructions on running Mixmasta in Docker (easiest!).

Ensure you have a working installation of [GDAL](https://trac.osgeo.org/gdal/wiki/FAQInstallationAndBuilding#FAQ-InstallationandBuilding)

You also need to ensure that `numpy` is installed prior to `mixmasta` installation. This is an artifact of GDAL, which will build incorrectly if `numpy` is not already configured:

```
pip install numpy==1.20.1
pip install mixmasta
```

> Note: if you had a prior installation of GDAL you may need to run `pip install mixmasta --no-cache-dir` in a clean environment.

You must install the GADM2 and GADM3 data with:

```
mixmasta download
```

## Usage


Examples can be found in the `input` directory.

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

# next, we can geocode the dataframe to the admin-level desired (`admin2` or `admin3`)
# by specifying the names of the x and y columns
# in this case, we will geocode to admin2 where x,y are are 'longitude' and 'latitude', respectively.
df_g = mix.geocode("admin2", df, x='longitude', y='latitude')
```

## Running with CLI

After cloning the repository and changing to the `mixmasta` directory, you can run mixmasta via the command line.

Set-up:

While you can point `mixmasta` to any file you would like to transform, the examples below assume your file is in the `inputs` folder; the transformed `.csv` file will be written to the `outputs` folder.

- Transform geotiff to geocoded csv:
```
mixmasta mix --xform=geotiff --input_file=chirps-v2.0.2021.01.3.tif --output_file=geotiffTEST.csv --geo=admin2 --feature_name=rainfall --band=1 --date='5/4/2010' --x=longitude --y=latitude
```

- Transform geotiff to csv:
```
mixmasta mix --xform=geotiff --input_file=maxhop1.tif --output_file=maxhopOUT.csv --geo=admin2 --feature_name=probabilty --band=1 --x=longitude --y=latitude
```

- Transform netcdf to geocoded csv:

```
mixmasta mix --xform=netcdf --input_file=tos_O1_2001-2002.nc --output_file=netcdf.csv --geo=admin2 --x=lon --y=lat
```

- Transform netcdf to csv:
```
mixmasta mix --xform=netcdf --input_file=tos_O1_2001-2002.nc --output_file=netcdf.csv
```

-geocode an existing csv file:

```
mixmasta mix --xform=geocode --input_file=no_geo.csv --geo=admin3 --output_file=geoed_no_geo.csv --x=longitude --y=latitude
```

## World Modelers Specific Normalization

For the World Modelers program, it is necessary to convert arbitrary `csv`, `geotiff`, and `netcdf` files into a CauseMos compliant format. This can be accomplished by leveraging a `mapping` annotation file and the `causemosify` command. The output is a `gzipped` `parquet` file. This may be invoked with:

```
mixmasta causemosify --input_file=chirps-v2.0.2021.01.3.tif --mapper=mapper.json --geo=admin3 --output_file=causemosified_example
```

This will produce a file called `causemosified_example.parquet.gzip` which can be read using Pandas with:

```
pd.read_parquet('causemosified_example.parquet.gzip')
```

## Other Documents
- Docker Instructions: `docs/docker.md`
- Geo Entity Resolution Description: `docs/geo-tentity-resolution.md`
- Package Testing in SpaceTag Env: `docs/spacetag-test.md`

## Credits

This package was created with [Cookiecutter](https://github.com/audreyr/cookiecutter) and the [audreyr/cookiecutter-pypackage](https://github.com/audreyr/cookiecutter-pypackage) project template.
