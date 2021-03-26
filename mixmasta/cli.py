#!/usr/bin/python3

import click
import sys
import pandas as pd
from .mixmasta import netcdf2df, raster2df, geocode
from .download import download_and_clean
import os

from datetime import datetime

@click.command()
@click.argument('command')
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

def main(command, xform, geo, input_file, output_file, feature_name, band, nodataval, date, x, y):
    """Console script for mixmasta."""
    
    if command == 'download':
        download_and_clean('admin2')
        download_and_clean('admin3')
    else:
        # If geocoding: check if gadm feather file exists; if not, download it
        if geo != None:
            download_and_clean(geo)

        if xform == "netcdf":
            
            print(f"Transforming {input_file} netcdf to csv")
            df = netcdf2df(input_file)
            
            if geo != None:
                print(f"Geocoding {input_file} to {geo}")
                df_geo = geocode(geo, df, x, y)
                df_geo.to_csv(output_file, index=False)
                print(df_geo.head()) 

            else:
                print("Writing netcdf to csv")
                df.to_csv(output_file, index=False)
                print(df.head())

        elif xform == "geotiff":

            print(f"Transforming {input_file} geotiff to csv")
            df = raster2df(input_file, feature_name, band, nodataval, date)
            
            if geo != None:

                print(f"Geocoding {input_file} to {geo}")
                df_geo = geocode(geo, df, x, y)
                df_geo.to_csv(output_file, index=False)
                print(df_geo.head()) 
                
            else:
                print("Writing geotiff to csv")
                df.to_csv(output_file, index=False)
                print(df.head())
        
        elif xform == "geocode":

            df = pd.read_csv(input_file)

            if geo != None:
                print(f"Geocoding {input_file} to {geo}")
                df_geo = geocode(geo, df, x, y)
                df_geo.to_csv(output_file, index=False)
                print(df_geo.head())

if __name__ == '__main__':
    sys.exit(main())