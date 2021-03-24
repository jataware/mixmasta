#!/usr/bin/python3

import click
import sys
import pandas as pd
from mixmasta import netcdf2df, raster2df, geocode
from download import download_and_clean
import os

from datetime import datetime

'''
EXAMPLE PYTHON TEST RUNS:

-geotiff
python3 mixmasta/cli.py -xform geotiff -input_file chirps-v2.0.2021.01.3.tif -output_file geotiffTEST.csv -geo admin2 -feature_name rainfall -band=1 -date='5/4/2010' -x longitude -y latitude
python3 mixmasta/cli.py -xform geotiff -input_file maxhop1.tif -output_file maxhopOUT.csv -geo admin2 -feature_name probabilty -band=1 -x longitude -y latitude

-netcfd
python3 mixmasta/cli.py -xform netcdf -input_file tos_O1_2001-2002.nc -output_file netcdf.csv
python3 mixmasta/cli.py -xform netcdf -input_file tos_O1_2001-2002.nc -output_file netcdf.csv -geo admin2 -x lon -y lat

-geocode
python3 mixmasta/cli.py -xform geocode -input_file no_geo.csv -geo admin2 -output_file no_geo_geo.csv -x longitude -y latitude
'''

'''
EXAMPLE DOCKER TEST RUNS:
Where:
 <in>  = local folder with file to transform
 <out> = local folder to write outouts to

 outputs/inputs folders are built into the Docker image

# netcdf
docker run -v $PWD/in:/inputs -v $PWD/out:/outputs mixmasta:0.2 -xform netcdf -input_file tos_O1_2001-2002.nc -output_file netcdf.csv
docker run -v $PWD/in:/inputs -v $PWD/out:/outputs mixmasta -xform netcdf -input_file tos_O1_2001-2002.nc -output_file netcdf_geo.csv -geo admin2 -x lon -y lat

# geotiff
docker run -v $PWD/inputs:/inputs -v $PWD/outputs:/outputs mixmasta:0.2 -xform geotiff -input_file chirps-v2.0.2021.01.3.tif -output_file geotiff.csv -feature_name rainfall -band 1 -date '5/4/2010'
docker run -v $PWD/inputs:/inputs -v $PWD/outputs:/outputs mixmasta:0.2 -xform geotiff -input_file chirps-v2.0.2021.01.3.tif -output_file geotiff_geo.csv -feature_name rainfall -band 1 -date '5/4/2010' -geo admin2 -x longitude -y latitude

#geocode ONLY
docker run -v $PWD/in:/inputs -v $PWD/out:/outputs mixmasta -xform geocode -input_file test_geocode.csv -output_file geocodeONLY.csv -x lon -y lat 

'''

@click.command()
@click.option('-xform', type=str, default=None)
@click.option('-geo', type=str, default=None)
@click.option('-input_file', type=str, default=None)
@click.option('-output_file', type=str, default=None)
@click.option('-feature_name', type=str, default=None)
@click.option('-band', type=int, default=None)
@click.option('-nodataval', type=int, default=-9999)
@click.option('-date', type=str, default=None)
@click.option('-x', type=str, help = "longitude Column", default=None)
@click.option('-y', type=str, help = "latitude Column", default=None)

def main(xform, geo, input_file, output_file, feature_name, band, nodataval, date, x, y):
    """Console script for mixmasta."""
    
    wrkDir = os.getcwd()
    
    # If geocoding: check if gadm feather file exists; if not, download it
    if geo != None:
        download_and_clean(geo)

    if xform == "netcdf":
        
        print(f"Transforming {input_file} netcdf to csv")
        filePath = f'inputs/{input_file}'
        df = netcdf2df(filePath)
        
        if geo != None:
            print(f"Geocoding {input_file} to {geo}")
            df_geo = geocode(geo, df, x, y)
            df_geo.to_csv(f"{wrkDir}/outputs/{output_file}", index=False)
            print(df_geo.head()) 

        else:
            print("Writing netcdf to csv")
            df.to_csv(f"{wrkDir}/outputs/{output_file}", index=False)
            print(df.head())

    elif xform == "geotiff":

        filePath = f'inputs/{input_file}'
        print(f"Transforming {input_file} geotiff to csv")
        df = raster2df(filePath, feature_name, band, nodataval, date)
        
        if geo != None:

            print(f"Geocoding {input_file} to {geo}")
            df_geo = geocode(geo, df, x, y)
            df_geo.to_csv(f"{wrkDir}/outputs/{output_file}", index=False)
            print(df_geo.head()) 
            
        else:
            print("Writing geotiff to csv")
            df.to_csv(f"{wrkDir}/outputs/{output_file}", index=False)
            print(df.head())
    
    elif xform == "geocode":

        filePath = f'inputs/{input_file}'
        df = pd.read_csv(filePath)

        if geo != None:
            print(f"Geocoding {input_file} to {geo}")
            df_geo = geocode(geo, df, x, y)
            df_geo.to_csv(f"{wrkDir}/outputs/{output_file}", index=False)
            print(df_geo.head())

if __name__ == '__main__':
    sys.exit(main())
