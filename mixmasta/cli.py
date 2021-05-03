import os
import sys
from datetime import datetime

import click
import pandas as pd

from .download import download_and_clean
from .mixmasta import geocode, netcdf2df, process, raster2df


@click.group()
def cli():
    pass


@cli.command()
@click.option("--input_file", type=str, default=None)
@click.option("--mapper", type=str, default=None)
@click.option("--geo", type=str, default=None)
@click.option("--output_file", type=str, default="mixmasta_output")
def causemosify(input_file, mapper, geo, output_file):
    """Processor for generating CauseMos compliant datasets."""
    click.echo("Causemosifying data...")
    return process(input_file, mapper, geo, output_file)


@cli.command()
@click.option("--xform", type=str, default=None)
@click.option("--geo", type=str, default=None)
@click.option("--input_file", type=str, default=None)
@click.option("--output_file", type=str, default="mixmasta_output.csv")
@click.option("--feature_name", type=str, default=None)
@click.option("--band", type=int, default=None)
@click.option("--nodataval", type=int, default=-9999)
@click.option("--date", type=str, default=None)
@click.option("--x", type=str, help="longitude Column", default=None)
@click.option("--y", type=str, help="latitude Column", default=None)
def mix(xform, geo, input_file, output_file, feature_name, band, nodataval, date, x, y):
    """Console script to flexibly run mixmasta."""
    click.echo("Mixing...")
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


@cli.command()
def download():
    """Download mixmasta data."""
    click.echo("Downloading data...")
    download_and_clean("admin2")
    download_and_clean("admin3")


if __name__ == "__main__":
    cli()
