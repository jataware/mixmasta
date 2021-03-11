## mixmasta

For the CLI Parameters below, you only need to include the parameters related to your mixmasta job; defining unused parameters is not required.

NOTE: For the examples below you mount the `in` and `out`  folders to the container's `input` and `output` folders. The `in` folder will have your input file to be transformed and results will be written to your `out` folder.

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


### Convert geotiff to csv: note `-date` is the last argument to avoid issues with the single quote

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

### Convert geotiff to *Geocoded* csv: note `-date` is the last argument to avoid issues with the single quote

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

#### Available CLI Parameters:

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