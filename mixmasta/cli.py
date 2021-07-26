import os
import sys
from datetime import datetime

import click
import pandas as pd

from .download import download_and_clean
from .mixmasta import geocode, netcdf2df, process, raster2df

from glob import glob

import json


@click.group()
def cli():
    pass


def glob_input_file(input_file: str) -> str:
    # Enable wild card in file path
    if "*" in input_file:
        try:
            input_file = glob(input_file)[0]
            click.echo(
                f'Wildcard character "*" detected; input file resolved to {input_file}'
            )
        except:
            click.echo(
                f'Unable to use wildcard character "*" to identify file; assuming {input_file} is actual file path.'
            )
            input_file = input_file
    return input_file

@cli.command()
@click.option("--input_file", type=str, default=None)
@click.option("--mapper", type=str, default=None)
@click.option("--geo", type=str, default=None)
@click.option("--output_file", type=str, default="mixmasta_output")
def causemosify(input_file, mapper, geo, output_file):
    """Processor for generating CauseMos compliant datasets."""
    click.echo("Causemosifying data...")

    input_file =  glob_input_file(input_file)
    return process(input_file, mapper, geo, output_file)


@cli.command()
@click.option("--inputs", type=str, default=None)
@click.option("--geo", type=str, default=None)
@click.option("--output-file", type=str, default="mixmasta_output")
def causemosify_multi(inputs, geo, output_file):
    """Process multiple input files to generate a single CauseMos compliant dataset."""

    # --inputs is one long gnarly string with interior quotations escaped e.g.:
    # "[{\"input_file\": \"build-a-date-qualifier_*.csv\",
    #        \"mapper\": \"build-a-date-qualifier.json\"}]"

    input_array = json.loads(inputs)

    df = pd.DataFrame()
    renamed_col_dict = {}
    for item in input_array:
        # Handle filename wildcards.
        input_file =  glob_input_file(item["input_file"])
        mapper = item["mapper"]

        # Call process without writing parquet files.
        result_df, result_dict = process(input_file, mapper, geo, output_file = None, write_output=False)

        # Combine outputs to return single file.
        df = result_df if df.empty else df.append(result_df)
        renamed_col_dict = result_dict if not renamed_col_dict else {**renamed_col_dict, **result_dict}

    # Separate string values from others
    df['type'] = df[['value']].applymap(type)
    df_str = df[df['type']==str]
    df = df[df['type']!=str]
    del(df_str['type'])
    del(df['type'])

    # Write parquet files
    df.to_parquet(f"{output_file}.parquet.gzip", compression="gzip")
    if len(df_str) > 0:
        df_str.to_parquet(f"{output_file}_str.parquet.gzip", compression="gzip")

    return df.append(df_str), renamed_col_dict

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
