## mixmasta Docker Container

## Contents
- [Set-Up](#set-up)
- [Convert netcdf to csv](#convert-netcdf-to-csv)
- [Convert netcdf to *Geocoded* csv](#convert-netcdf-to-geocoded-csv)
- [Convert geotiff to csv](#convert-geotiff-to-csv)
- [Convert geotiff to *Geocoded* csv](#convert-geotiff-to-geocoded-csv)
- [Convert a csv file to *Geocoded* csv](#convert-a-csv-file-to-geocoded-csv)
- [Causemosify a file](#causemosify-a-file)
- [Available CLI Parameters](#available-cli-parameters)

### Set-Up

You will mount your current directory (`$PWD`) to `/tmp` in the container. This is somewhat arbitrary; you can mount `$PWD` more or less anywhere in the container that is not reserved. We assume that the file you want to `mix` is in your current directory; if not, you need to mount that directory or move the file to your current directory. Output will be written to `$PWD` or whichever directory you mount. In other words, `/tmp` exists as the mount on the container, not on your localhost.

### Convert netcdf to csv

```
docker run -v $PWD:/tmp \
           jataware/mixmasta:latest \
           mix \
           --xform netcdf \
           --input_file /tmp/tos_O1_2001-2002.nc \
           --output_file /tmp/netcdf.csv
```

### Convert netcdf to *Geocoded* csv

Note: Geocoding takes some time...in this case we geocode down to admin2; you can also geocode to the admin3 level with `-geo admin3`

```
docker run -v $PWD:/tmp \
           jataware/mixmasta:latest \
           mix \
           --xform netcdf \
           --input_file /tmp/tos_O1_2001-2002.nc \
           --output_file /tmp/netcdf_geo.csv \
           --geo admin2 \
           --x lon \
           --y lat
```


### Convert geotiff to csv

NOTE: `--date` is the last argument to avoid issues with the single quote

```
docker run -v $PWD:/tmp \
           jataware/mixmasta:latest \
           mix \
           --xform geotiff \
           --input_file /tmp/chirps-v2.0.2021.01.3.tif \
           --output_file /tmp/geotiff.csv \
           --feature_name rainfall \
           --band 1 \
           --date '5/4/2010'
```

### Convert geotiff to *Geocoded* csv

NOTE: `-date` is the last argument to avoid issues with the single quote

Note: Geocoding takes some time...

```
docker run -v $PWD:/tmp \
           jataware/mixmasta:latest \
           mix \
           --xform geotiff \
           --input_file /tmp/chirps-v2.0.2021.01.3.tif \
           --output_file /tmp/geotiff_geo.csv \
           --feature_name rainfall \
           --band 1 \
           --geo admin2 \
           --x longitude \
           --y latitude \
           --date '5/4/2010' 
```

### Convert a csv file to *Geocoded* csv

Note: Geocoding takes some time...in this case we geocode down to admin2; you can also geocode to the admin3 level with `-geo admin3`

```
docker run -v $PWD:/tmp \
           jataware/mixmasta:latest \
           mix \
           --xform geocode \
           --geo admin2 \
           --input_file /tmp/test_geocode.csv \
           --output_file /tmp/geocodeONLY.csv \
           --x lon \
           --y lat 
```

### Causemosify a file

This is a special case where Mixmasta is used to make a CauseMos compliant file. It requires a mapper file which contains instructions for how to perform this transformation. Note that the `input_file` can contain the `*` wildcard character. This is designed to support models which may produce non-deterministic filenames for their outputs. For example, if a model produces an output that appends an epoch timestamp to the filename (e.g. `sample_output_1625711342.csv`) this is problematic and should be abstracted to `sample_output_*.csv` by the Dojo system. This is handled by the `causemosify` function in `cli.py`.

```
docker run -v $PWD:/tmp \
           jataware/mixmasta:latest \
           causemosify \
           --input_file=/tmp/chirps-v2.0.2021.01.3.tif \
           --mapper=/tmp/mapper.json \
           --geo=admin3 \
           --output_file=/tmp/example_output
```

### mix command

This command exposes the full mixmasta API to convert and geocode data.

`--xform`: type of transform desired
  
  - options: `geotiff`, `netcdf`, `geocode` 
  - type=str
  - default=None

`--geo`: If you wish to geocode, choose the lowest admin level: `admin2` or `admin3`. Defualt is `None` so your file will only be be geocoded, to the admin-level you select, if you include the `-geo` tag.

  - options: `admin2` or `admin3`
  - type=str
  - default=None
 
`--input_file`: Filename of the file to transform. 

  - type=str
  - default=None
  
`--output_file`: Transformed file name. In the container, transformed files are written to the `outputs/` folder. 

  - type=str
  - default=None
  
`--feature_name`: Feature name in your geotiff file

  - type=str
  - default=None
 
`--band`: geotiff band desired

  - type=int
  - default=None
  
`--nodataval`: No data value

  - type=int
  - default=-9999
  
`--date`: You may optionally specify a date if the geotiff has an associated date; the date must be in single quotes: `-date '5/4/2010'`

  - type=str
  - default=None
  
`--x`: The naming convention of your file to indicate: Longitude

  - type=str
  - default=None
  
`--y`: The naming convention of your file to indicate: Latitude

  - type=str
  - default=None

### causemosify command

This command takes in a data file and a mapper file and converts it into a CauseMos compliant format.

`--input_file`: Filename of the file to transform. 

  - type=str
  - default=None

`--geo`: If you wish to geocode, choose the lowest admin level: `admin2` or `admin3`. Defualt is `None` so your file will only be be geocoded, to the admin-level you select, if you include the `-geo` tag.

  - options: `admin2` or `admin3`
  - type=str
  - default=None 
  
`--output_file`: Transformed file name. In the container, transformed files are written to the `outputs/` folder. 

  - type=str
  - default=`mixmasta_output` (which writes a file to `mixmasta_output.parquet.gzip`)

### Testing

In `examples/causemosify-tests` you can run bash `bash test_file_1.sh` and `bash test_file_2.sh` to Causemosify two files. This assumes you have a container called `mixmasta` locally. You can build this from the top of the repo with `docker build -t mixmasta .`