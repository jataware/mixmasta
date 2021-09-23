import os
import sys
from datetime import datetime

import click
import pandas as pd

from .download import download_and_clean
from .mixmasta import geocode, netcdf2df, process, raster2df, normalizer, optimize_df_types, mixdata

#from download import download_and_clean
#from mixmasta import geocode, netcdf2df, process, raster2df, normalizer, optimize_df_types, mixdata

from glob import glob
import numpy as np

import json
import timeit


# Constants
CHUNK_SIZE = 100000
DATA_TEMP_FILENAME = 'causmosify_multi_tmp'
PROCESSED_TEMP_FILENAME = 'processed_tmp'

@click.group()
def cli():
    pass


def chunk_normalize(input_file: str, mapper: dict, renamed_col_dict: dict, geo: str, gadm, df_geocode: pd.DataFrame) -> dict:
    """
        Description
        ----------
            Normalize input_file in chunks written to pkl files.

        Parameters
        ----------
            input_file: str
                Filename of file to normalize.
            mapper: dict
                Mapper dict for filename.
            renamed_col_dict: dict
                Dict maintained to track columns renamed during normalize()
            gadm: 
                GADM geopandas object passed as param so it is loaded only once
            df_geocode: pd.DataFrame
                lat/lng geocode library to cache geocoding.
        
        Returns
        -------
            dict: 
                updated renamed_col_dict

    """

    df_datatypes = {}
    df_columns = []
    first_norm_time = 0

    click.echo(f'\nLoading {input_file} ...')
    start_time = timeit.default_timer()
    
    # Set the transformation type, and reduce the mapper to date, geo and feature keys.
    transform = mapper["meta"]
    mapper = { k: mapper[k] for k in mapper.keys() & {"date", "geo", "feature"} }

    # File type-specific pre-processing
    ftype = transform["ftype"]
    if ftype == "geotiff":
        if transform["date"] == "":
            d = None
        else:
            d = transform["date"]

        df = raster2df(
            InRaster = input_file,
            feature_name = transform["feature_name"],
            band = int(transform["band"] if "band" in transform and transform["band"] != "" else "0"),
            nodataval = int(transform["null_val"]),
            date = d,
            band_name = transform["band_name"],
            bands = transform["bands"] if "bands" in transform else None
        )
    elif ftype == 'excel':
        df = pd.read_excel(input_file, transform['sheet'])
    elif ftype != "csv":
        df = netcdf2df(input_file)
    else:
        df = pd.read_csv(input_file)

    df.reset_index(inplace=True, drop=True)
    
    # Set the number of chunks based on CHUNK_SIZE
    chunks = 1 + (df.shape[0] // CHUNK_SIZE) 
    
    # Entire dataset is loaded into df. Write in chunks to pkl files.
    for i in range(1, chunks+1): 
        filename = f'{DATA_TEMP_FILENAME}.{i}.pkl'
        stop_row = i*CHUNK_SIZE
        start_row = stop_row - CHUNK_SIZE
        df.iloc[start_row:stop_row,:].to_pickle(filename)
        
    click.echo(f'Processing {input_file} ...')
    start_time = timeit.default_timer()
    
    # Iterate chunk tmp files and normalized each loaded df.
    for i in range(1, chunks+1): 
        read_filename = f'{DATA_TEMP_FILENAME}.{i}.pkl' 
        df_temp = pd.read_pickle(read_filename)

        ## Run normalizer.
        norm_start_time = timeit.default_timer()
        norm, result_dict, df_geocode  = normalizer(df_temp, mapper, geo, gadm=gadm, df_geocode=df_geocode)
   
        # Normalizer will add NaN for missing values, e.g. when appending
        # dataframes with different columns. GADM will return None when geocoding
        # but not finding the entity (e.g. admin3 for United States).
        # Replace None with NaN for consistency.
        norm.fillna(value=np.nan, inplace=True)

        # In edge cases where the input files have different columns, keep 
        # list of all columns for reading the tmp file.
        for col in norm.columns.values.tolist():
            if col not in df_columns:
                df_columns.append(col)
        
        if len(df_datatypes) == 0:
            # Record datatypes of normalized df for setting on read.
            norm = optimize_df_types(norm)
            df_datatypes = dict(norm.dtypes)

        #norm.to_csv(chunked_temp_filename, mode='a', index=False, header=False)
        write_filename = f'{PROCESSED_TEMP_FILENAME}.{os.path.basename(input_file)}.{i}.pkl'
        norm.to_pickle(write_filename)

        # A little cleanup, probably ineffective.
        del(norm)
        del(df_temp)

        # Delete tmp files as they are read.
        os.remove(read_filename)

        # Combine dict to return single file.
        renamed_col_dict = result_dict if not renamed_col_dict else {**renamed_col_dict, **result_dict}

    click.echo(f'{input_file} processing completed in {timeit.default_timer() - start_time} seconds')

    return renamed_col_dict


def get_gadm(geo: str):
    # Cache GADM for normalize() loops.
    md = mixdata()
    if geo.lower() == 'admin2':
        click.echo('Loading GADM2 ...')
        md.load_gadm2()
        gadm = md.gadm2
    else:
        click.echo('Loading GADM3 ...')
        md.load_gadm3()
        gadm = md.gadm3

    return gadm


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
def causemosify(input_file, mapper, geo: str, output_file):
    """Processor for generating CauseMos compliant datasets."""
    click.echo("Causemosifying data...")

    input_file =  glob_input_file(input_file)
    gadm = get_gadm(geo)
    with open(mapper) as f:
        mapper = json.loads(f.read())

    # Chunk normalized, return the renamed_dict. The data will be read from pkl
    # files beelow.
    renamed_col_dict = chunk_normalize(input_file, mapper, {}, geo, gadm)

    # Reassemble tmp pkl files.
    df_final = pd.concat([pd.read_pickle(fl) for fl in glob(f'{PROCESSED_TEMP_FILENAME}.*.pkl')])
    df_final.reset_index(inplace=True, drop=True)
    
    # Clean up reassembly. 
    for fl in glob(f'{PROCESSED_TEMP_FILENAME}*'):
        os.remove(fl)
    
    # Write separate parquet files.
    df_final['type'] = df_final[['value']].applymap(type)
    df_final_str = df_final[df_final['type']==str]
    df_final = df_final[df_final['type']!=str]
    del(df_final_str['type'])
    del(df_final['type'])

    df_final.to_parquet(f"{output_file}.parquet.gzip", compression="gzip")
    if not df_final_str.empty:
        df_final_str.to_parquet(f"{output_file}_str.parquet.gzip", compression="gzip")
    
    # Rebuild and reduce memory size of returned dataframe.
    df_final = df_final.append(df_final_str)
    df_final = optimize_df_types(df_final)
    df_final.reset_index(inplace=True, drop=True)

    return df_final, renamed_col_dict
    

@cli.command()
@click.option("--inputs", type=str, default=None)
@click.option("--geo", type=str, default=None)
@click.option("--output-file", type=str, default="mixmasta_output")
def causemosify_multi(inputs, geo, output_file):
    """Process multiple input files to generate a single CauseMos compliant dataset."""

    """
        Description
        -----------
        Usurps mixmasta.process() to control memory use.
    
        --inputs is one long gnarly string with interior quotations escaped e.g.:
        "[{\"input_file\": \"build-a-date-qualifier_*.csv\",
                \"mapper\": \"build-a-date-qualifier.json\"}]"  
    

        Writes separate parquet files for each input file.
    """

    input_array = json.loads(inputs)   
    click.echo(f"Causemosifying {len(input_array)} file(s) ...")
    
    # Cache GADM for normalize() loops.
    gadm = get_gadm(geo)

    # Setup variables.
    renamed_col_dict = {}
    df_geocode = pd.DataFrame()

    # Delete any previous tmp files.
    for fl in glob(f'{PROCESSED_TEMP_FILENAME}*'):
        os.remove(fl)
    
    for fl in glob(f'{DATA_TEMP_FILENAME}*'):
        os.remove(fl)
  
    # Iterate each item_item in the --inputs JSON.
    item_counter = 0
    for item_item in input_array:        
        item_counter += 1
        # Handle filename wildcards with glob_input_file().
        input_file =  glob_input_file(item_item["input_file"])
       
        with open(item_item ["mapper"]) as f:
            mapper = json.loads(f.read())

        renamed_col_dict = chunk_normalize(input_file = input_file, 
            mapper = mapper, 
            renamed_col_dict=renamed_col_dict, 
            geo=geo,
            gadm=gadm,
            df_geocode=df_geocode)

        # Reassemble tmp files into df ...)
        df_final = pd.concat([pd.read_pickle(fl) for fl in glob(f'{PROCESSED_TEMP_FILENAME}.*.pkl')])
        df_final.reset_index(inplace=True, drop=True)
    
        # ... then clean up reassembly. 
        for fl in glob(f'{PROCESSED_TEMP_FILENAME}*'):
            os.remove(fl)
       
        # Write separate parquet files depending on type of value column ...
        # ... by creating a separting df for value col of type str ...
        df_final['type'] = df_final[['value']].applymap(type)
        df_final_str = df_final[df_final['type']==str]
        df_final = df_final[df_final['type']!=str]
        del(df_final_str['type'])
        del(df_final['type'])

        # ... now write the files.
        df_final.to_parquet(f"{output_file}.{item_counter}.parquet.gzip", compression="gzip")
        if not df_final_str.empty:
            df_final_str.to_parquet(f"{output_file}_str.{item_counter}.parquet.gzip", compression="gzip")
    
    # Causemosify-multi does not return the dataframe or dict.

    click.echo('Done.')


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
    '''
    Testing
    if os.name == 'nt':
        sep = '\\'
    else:
        sep = '/'
        

    inputs = "[ {\"input_file\":\"Test1_2D-Q.nc\", "
    inputs = inputs + "\"mapper\":\"mapper_e489d023-58c3-4544-8ae1-bb9cfa987e23.json\"},"

    inputs = inputs + "{\"input_file\":\"Test1_2D-d-flood.nc\", \"mapper\":\"mapper_25d1d6ab-1e74-4084-8c18-692e6542aa49.json\"},"
    inputs = inputs + "{\"input_file\":\"Test1_2D-d.nc\", \"mapper\":\"mapper_7302d901-85b8-4fab-a3b2-b9e0b9646458.json\"},"
    inputs = inputs + "{\"input_file\":\"Test1_2D-u.nc\", \"mapper\":\"mapper_c925ad6e-5275-4a2c-b288-ba7c450a4b8f.json\"}"
    
    inputs = inputs + "]"

    #inputs = "[ {\"input_file\":\"lpjml_sample.nc\", "
    #inputs = inputs + "\"mapper\":\"lpml_mapper.json\"}]"

    inputs = "[{\"input_file\": \"inputs" + f"{sep}test1_input.csv\",\"mapper\": \"inputs{sep}test1_input.json\"" + "},{\"input_file\": \""
    inputs = inputs + f"inputs{sep}test3_qualifies.csv\",\"mapper\": \"inputs{sep}test3_qualifies.json\"" + "}]"



    causemosify_multi(inputs, geo='admin2', output_file='testing')
    #causemosify(input_file="lpjml_sample.nc", mapper="lpml_mapper.json", geo="admin2", output_file="output.tmp")
    '''