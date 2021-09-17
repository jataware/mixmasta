import os
import sys
from datetime import datetime

import click
import pandas as pd

#from .download import download_and_clean
#from .mixmasta import geocode, netcdf2df, process, raster2df

from download import download_and_clean
from mixmasta import geocode, netcdf2df, process, raster2df, normalizer, optimize_df_types
from mixmasta import mixdata

from glob import glob
import numpy as np

import json
import timeit


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
def causemosify(input_file, mapper, geo: str, output_file):
    """Processor for generating CauseMos compliant datasets."""
    click.echo("Causemosifying data...")

    input_file =  glob_input_file(input_file)
    return process(input_file, mapper, geo, output_file)


#@cli.command()
#@click.option("--inputs", type=str, default=None)
#@click.option("--geo", type=str, default=None)
#@click.option("--output-file", type=str, default="mixmasta_output")
def causemosify_multi(inputs, geo, output_file):
    """Process multiple input files to generate a single CauseMos compliant dataset."""

    """
        Usurps mixmasta.process() to control memory use.
    
        --inputs is one long gnarly string with interior quotations escaped e.g.:
        "[{\"input_file\": \"build-a-date-qualifier_*.csv\",
                \"mapper\": \"build-a-date-qualifier.json\"}]"  
    
    
    """

    input_array = json.loads(inputs)   
    click.echo(f"Causemosifying {len(input_array)} file(s) ...")
    
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

    # Setup variables.
    renamed_col_dict = {}
    df_datatypes = {}
    df_columns = []
    chunk_size = 100000
    data_temp_filename = 'causmosify_multi_tmp'
    processed_temp_filename = 'processed_tmp'
    first_norm_time = 0

    # Delete any previous tmp files.
    for fl in glob(f'{processed_temp_filename}*'):
        os.remove(fl)
    
    for fl in glob(f'{data_temp_filename}*'):
        os.remove(fl)
  
    # Iterate each item in the --inputs JSON.
    for item in input_array:
        # Handle filename wildcards with glob_input_file().
        input_file =  glob_input_file(item["input_file"])
        click.echo(f'loading {input_file} ...')
        start_time = timeit.default_timer()
        
        # Assign mapper, set the transformation type, and reduce the mapper
        # to date, geo and feature keys.
        mapper = dict
        with open(item["mapper"]) as f:
            mapper = json.loads(f.read())
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
        click.echo(f'{input_file} loading completed in {timeit.default_timer() - start_time} seconds')
       
        # Set the number of chunks based on chunk_size
        chunks = 1 + (df.shape[0] // chunk_size) 
        
        # Entire dataset is loaded into df. Write in chunks to pkl files.
        write_start = timeit.default_timer()
        for i in range(1, chunks+1): 
            filename = f'{data_temp_filename}.{i}.pkl'
            stop_row = i*chunk_size
            start_row = stop_row - chunk_size
            df.iloc[start_row:stop_row,:].to_pickle(filename)
          
        click.echo(f'{input_file} pickle files writing completed in {timeit.default_timer() - write_start} seconds')
        click.echo(f'processing {input_file} ...')
        start_time = timeit.default_timer()
        
        # Iterate chunk tmp files and normalized each loaded df.
        for i in range(1, chunks+1): 
            read_filename = f'{data_temp_filename}.{i}.pkl' 
            df_temp = pd.read_pickle(read_filename)

            ## Run normalizer.
            norm_start_time = timeit.default_timer()
            norm, result_dict = normalizer(df_temp, mapper, geo, gadm=gadm)
            norm_stop_time = timeit.default_timer() - norm_start_time

            #click.echo(f'i: {i} chunk_size: {chunk_size} skiprows: {range(1, i*chunk_size)} iter_time:{iter_time} {iter_time  / chunk_size} {iter_time - first_iter_time}')
            click.echo(f'i: {i} chunk_size: {chunk_size} skiprows: {range(1, i*chunk_size)} iter_time:{norm_stop_time}  diff:{first_norm_time - first_norm_time}')
            
            if first_norm_time == 0:
                first_norm_time = norm_stop_time

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
            write_filename = f'{processed_temp_filename}.{input_file}.{i}.pkl'
            norm.to_pickle(write_filename)

            # A little cleanup, probably ineffective.
            del(norm)
            del(df_temp)

            # Delete tmp files as they are read.
            os.remove(read_filename)

            # Combine dict to return single file.
            renamed_col_dict = result_dict if not renamed_col_dict else {**renamed_col_dict, **result_dict}
        
        click.echo(f'{input_file} processing completed in {timeit.default_timer() - start_time} seconds')


    # Reassemble tmp files into a single (massive probably) df.
    click.echo('reload tmp csv ...')
    #df_final = pd.read_csv(chunked_temp_filename, names=df_columns)
    start_time = timeit.default_timer()
    df_final = pd.concat([pd.read_pickle(fl) for fl in [f'{processed_temp_filename}.{i}.pkl' for i in range(1, chunks+1)]])
    df_final.reset_index(inplace=True, drop=True)
    
    # Clean up reassembly. 
    for fl in glob(f'{processed_temp_filename}*'):
        os.remove(fl)
    
    click.echo(f'reassembled dataframe in {timeit.default_timer() - start_time}')
    click.echo(df_final.info(memory_usage="deep"))
    click.echo(df_final.shape)
    click.echo(df_final.head())

    # Write separate parquet files.
    click.echo('writing parquet files ...')
    df_final['type'] = df_final[['value']].applymap(type)
    df_final_str = df_final[df_final['type']==str]
    df_final = df_final[df_final['type']!=str]
    del(df_final_str['type'])
    del(df_final['type'])

    df_final.to_parquet(f"{output_file}.parquet.gzip", compression="gzip")
    if not df_final_str.empty:
        df_final_str.to_parquet(f"{output_file}_str.parquet.gzip", compression="gzip")
    
    click.echo('done.')
    #return df_final.append(df_final_str), renamed_col_dict


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
    #cli()
    
    if os.name == 'nt':
        sep = '\\'
    else:
        sep = '/'
        
    inputs = "[{\"input_file\": \"inputs" + f"{sep}test1_input.csv\",\"mapper\": \"inputs{sep}test1_input.json\"" + "},{\"input_file\": \""
    inputs = inputs + f"inputs{sep}test3_qualifies.csv\",\"mapper\": \"inputs{sep}test3_qualifies.json\"" + "}]"


    inputs = "[ {\"input_file\":\"Test1_2D-Q.nc\", "
    inputs = inputs + "\"mapper\":\"mapper_e489d023-58c3-4544-8ae1-bb9cfa987e23.json\"}]"

    inputs = "[ {\"input_file\":\"lpjml_sample.nc\", "
    inputs = inputs + "\"mapper\":\"lpml_mapper.json\"}]"


    df, dict = causemosify_multi(inputs, geo='admin2', output_file='testing')
    print('\n', df.shape)
    print(df.head())
    