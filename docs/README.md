## mixmasta Docker Container

## Contents
- [Set-Up](#set-up)
- [Convert netcdf to csv](#convert-netcdf-to-csv)
- [Convert netcdf to *Geocoded* csv](#convert-netcdf-to-geocoded-csv)
- [Convert geotiff to csv](#convert-geotiff-to-csv)
- [Convert geotiff to *Geocoded* csv](#convert-geotiff-to-geocoded-csv)
- [Convert a csv file to *Geocoded* csv](#convert-a-csv-file-to-geocoded-csv)
- [Available CLI Parameters](#available-cli-parameters)

### Set-Up

To transform your file and access the resulting csv file:

  1. mount your folder with your file-to-transform to the Docker container
  2. mount your output folder to the Docker container
 
To run the examples below:

  1. From a CLI in your top-level directory `mkdir in && mkdir out`
  2. Copy your file-to-transform into the `in` folder
  3. From top-level directory run any of the docker examples below.
  4. Your-transformed-file is written to your `out` folder.
 
From the examples you mount your `in` and `out`  folders to the container's `input` and `output` folders. Your folder names can be of your choosing; the Docker container folders cannot. **You must use `input` and `output` for the container folders.** 

For the CLI Parameters, you only need to include the parameters related to your mixmasta job; defining unused parameters is not required.

### Convert netcdf to csv

```
docker run -v $PWD/in:/inputs \
           -v $PWD/out:/outputs \
           jataware/mixmasta:0.1 \
           -xform netcdf \
           -input_file tos_O1_2001-2002.nc \
           -output_file netcdf.csv
```

### Convert netcdf to *Geocoded* csv

Note: Geocoding takes some time...

```
docker run -v $PWD/in:/inputs \
           -v $PWD/out:/outputs \
            jataware/mixmasta:0.1 \
           -xform netcdf \
           -input_file tos_O1_2001-2002.nc \
           -output_file netcdf_geo.csv \
           -geo True \
           -x lon \
           -y lat
```


### Convert geotiff to csv

NOTE: `-date` is the last argument to avoid issues with the single quote

```
docker run -v $PWD/in:/inputs \
           -v $PWD/out:/outputs \
           jataware/mixmasta:0.1 \
           -xform geotiff \
           -input_file chirps-v2.0.2021.01.3.tif \
           -output_file geotiff.csv \
           -feature_name rainfall \
           -band 1 \
           -date '5/4/2010'
```

### Convert geotiff to *Geocoded* csv

NOTE: `-date` is the last argument to avoid issues with the single quote

Note: Geocoding takes some time...

```
docker run -v $PWD/in:/inputs \
           -v $PWD/out:/outputs \
           jataware/mixmasta:0.1 \
           -xform geotiff \
           -input_file chirps-v2.0.2021.01.3.tif \
           -output_file geotiff_geo.csv \
           -feature_name rainfall \
           -band 1 \
           -geo True \
           -x longitude \
           -y latitude \
           -date '5/4/2010' 
```

### Convert a csv file to *Geocoded* csv

Note: Geocoding takes some time...

```
docker run -v $PWD/in:/inputs \
           -v $PWD/out:/outputs \
           jataware/mixmasta:0.1 \
           -xform geocode \
           -input_file test_geocode.csv \
           -output_file geocodeONLY.csv \
           -x lon \
           -y lat 
```

### Available CLI Parameters

`-xform`: type of transform desired
  
  - options: `geotiff`, `netcdf`, `geocode` 
  - type=str
  - default=None

`-geo`: Boolean flag. If `-geo True` return a transformed and geocoded csv file. If `-geo False` return a transformed csv file. 

  - options: `True` or `False`
  - type=bool
  - default=False
 
`-input_file`: Filename of the file to transform. 

  - type=str
  - default=None
  
`-output_file`: Transformed file name. In the container, transformed files are written to the `outputs/` folder. 

  - type=str
  - default=None
  
`-feature_name`: Feature name in your geotiff file

  - type=str
  - default=None
 
`-band`: geotiff band desired

  - type=int
  - default=None
  
`-nodataval`: No data value

  - type=int
  - default=-9999
  
`-date`: You may optionally specify a date if the geotiff has an associated date; the date must be in single quotes: `-date '5/4/2010'`

  - type=str
  - default=None
  
`-x`: The naming convention of your file to indicate: Longitude

  - type=str
  - default=None
  
`-y`: The naming convention of your file to indicate: Latitude

  - type=str
  - default=None
