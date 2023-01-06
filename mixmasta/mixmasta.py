"""Main module."""
import json
import logging
import os
import sys

import geofeather as gf
import numpy as np
import pandas as pd

from . import constants
from .file_processor import process_file_by_filetype
from .normalizer import normalizer

if not sys.warnoptions:
    import warnings

    warnings.simplefilter("ignore")

logger = logging.getLogger(__name__)


def optimize_df_types(df: pd.DataFrame):
    """
    Pandas will upcast essentially everything. This will use the built-in
    Pandas function to_numeeric to downcast dataframe series to types that use
    less memory e.g. float64 to float32.

    For very large dataframes the memory reduction should translate into
    increased efficieny.
    """
    floats = df.select_dtypes(include=["float64"]).columns.tolist()
    df[floats] = df[floats].apply(pd.to_numeric, downcast="float")

    ints = df.select_dtypes(include=["int64"]).columns.tolist()
    df[ints] = df[ints].apply(pd.to_numeric, downcast="integer")

    # for col in df.select_dtypes(include=['object']):
    #    num_unique_values = len(df[col].unique())
    #    num_total_values = len(df[col])
    #    if float(num_unique_values) / num_total_values < 0.5:
    #        df[col] = df[col].astype('category')

    return df


def process(
    fp: str, mp: str, admin: str, output_file: str, write_output=True, gadm=None
):
    """
    Parameters
    ----------
    mp: str
        Filename for JSON mapper from spacetag.
        Schema: https://github.com/jataware/spacetag/blob/schema/schema.py
        Example: https://github.com/jataware/spacetag/blob/schema/example.json

    gadm: gpd.GeoDataFrame, default None
        optional specification of a GeoDataFrame of GADM shapes of the appropriate
        level (admin2/3) for geocoding
    """

    # Read JSON schema to be mapper.
    mapper = dict
    with open(mp) as f:
        mapper = json.loads(f.read())

    # Validate JSON mapper schema against SpaceTag schema.py model.
    # model = SpaceModel(geo=mapper['geo'], date=mapper['date'], feature=mapper['feature'], meta=mapper['meta'])

    # "meta" portion of schema specifies transformation type
    transform = mapper["meta"]

    # Check transform for meta.geocode_level. Update admin to this if present.
    if admin == None and "geocode_level" in transform:
        admin = transform["geocode_level"]

    ftype = transform["ftype"]
    df = process_file_by_filetype(
        filepath=fp, file_type=ftype, transformation_metadata=transform
    )

    ## Make mapper contain only keys for date, geo, and feature.
    mapper = {k: mapper[k] for k in mapper.keys() & {"date", "geo", "feature"}}

    ## To speed up normalize(), reduce the memory size of the dataframe by:
    # 1. Optimize the dataframe types.
    # 2. Reset the index so it is a RangeIndex instead of Int64Index.
    df = optimize_df_types(df)
    df.reset_index(inplace=True, drop=True)

    ## Run normalizer.
    norm, renamed_col_dict, df_geocode = normalizer(df, mapper, admin, gadm=gadm)

    # Normalizer will add NaN for missing values, e.g. when appending
    # dataframes with different columns. GADM will return None when geocoding
    # but not finding the entity (e.g. admin3 for United States).
    # Replace None with NaN for consistency.
    norm.fillna(value=np.nan, inplace=True)

    if write_output:
        # If any qualify columns were added, the feature_type must be enforced
        # here because pandas will have cast strings as ints etc.
        qualify_cols = set(norm.columns).difference(set(constants.COL_ORDER))
        for col in qualify_cols:
            for feature_dict in mapper["feature"]:
                if (
                    feature_dict["name"] == col
                    and feature_dict["feature_type"] == "string"
                ):
                    norm[col] = norm[col].astype(str)

        # Separate string from other dtypes in value column.
        # This is predicated on the assumption that qualifying feature columns
        # are of a single dtype.

        norm["type"] = norm[["value"]].applymap(type)
        norm_str = norm[norm["type"] == str]
        norm = norm[norm["type"] != str]
        del norm_str["type"]
        del norm["type"]

        # Write parquet files
        norm.to_parquet(f"{output_file}.parquet.gzip", compression="gzip")
        if len(norm_str) > 0:
            norm_str.to_parquet(f"{output_file}_str.parquet.gzip", compression="gzip")

        norm = norm.append(norm_str)

    # Reduce memory size of returned dataframe.
    norm = optimize_df_types(norm)
    norm.reset_index(inplace=True, drop=True)

    return norm, renamed_col_dict


class mixdata:
    def load_gadm2(self):
        cdir = os.path.expanduser("~")
        download_data_folder = f"{cdir}/mixmasta_data"

        # Admin 0 - 2
        gadm_fn = f"gadm36_2.feather"
        gadmDir = f"{download_data_folder}/{gadm_fn}"
        gadm = gf.from_geofeather(gadmDir)
        gadm["country"] = gadm["NAME_0"]
        # gadm["state"] = gadm["NAME_1"]
        gadm["admin1"] = gadm["NAME_1"]
        gadm["admin2"] = gadm["NAME_2"]
        gadm0 = gadm[["geometry", "country"]]
        # gadm1 = gadm[["geometry", "country", "state", "admin1"]]
        # gadm2 = gadm[["geometry", "country", "state", "admin1", "admin2"]]
        gadm1 = gadm[["geometry", "country", "admin1"]]
        gadm2 = gadm[["geometry", "country", "admin1", "admin2"]]

        self.gadm0 = gadm0
        self.gadm1 = gadm1
        self.gadm2 = gadm2

    def load_gadm3(self):
        # Admin 3
        cdir = os.path.expanduser("~")
        download_data_folder = f"{cdir}/mixmasta_data"
        gadm_fn = f"gadm36_3.feather"
        gadmDir = f"{download_data_folder}/{gadm_fn}"
        gadm3 = gf.from_geofeather(gadmDir)
        gadm3["country"] = gadm3["NAME_0"]
        # gadm3["state"] = gadm3["NAME_1"]
        gadm3["admin1"] = gadm3["NAME_1"]
        gadm3["admin2"] = gadm3["NAME_2"]
        gadm3["admin3"] = gadm3["NAME_3"]
        # gadm3 = gadm3[["geometry", "country", "state", "admin1", "admin2", "admin3"]]
        gadm3 = gadm3[["geometry", "country", "admin1", "admin2", "admin3"]]
        self.gadm3 = gadm3


# Testing

# iso testing:
# mp = 'examples/causemosify-tests/mixmasta_ready_annotations_timestampfeature.json'
# fp = 'examples/causemosify-tests/raw_excel_timestampfeature.xlsx'
# build a date qualifier

# mp = 'examples/causemosify-tests/build-a-date-qualifier.json'
# fp = 'examples/causemosify-tests/build-a-date-qualifier_xyzz.csv'

# fp = "examples/causemosify-tests/november_tests_atlasai_assetwealth_allyears_2km.tif"
# mp = "examples/causemosify-tests/november_tests_atlasai_assetwealth_allyears_2km.json"

# fp = "examples/causemosify-tests/flood_monthly.tif"
# mp = "examples/causemosify-tests/flood_monthly.json"

# fp = "examples/causemosify-tests/Kenya - Admin1_Pasture_NDVI_2019-2021 converted.csv"
# mp = "examples/causemosify-tests/kenya_pasture.json"

# fp = "examples/causemosify-tests/rainfall_error.xlsx"
# mp = "examples/causemosify-tests/rainfall_error.json"
# geo = 'admin2'
# outf = 'examples/causemosify-tests/testing'

# mp = 'tests/inputs/test3_qualifies.json'
# fp = 'tests/inputs/test3_qualifies.csv'
# geo = 'admin2'
# outf = 'tests/outputs/unittests'

# fp = "examples/causemosify-tests/hoa_conflict.csv"
# mp = "examples/causemosify-tests/hoa_conflict.json"
# geo = 'admin2'
# outf = 'examples/causemosify-tests'

# fp = "examples/causemosify-tests/maxent_Ethiopia_precipChange.0.8tempChange.-0.3.tif"
# mp = "examples/causemosify-tests/maxent_Ethiopia_precipChange.0.8tempChange.-0.3.json"

# fp = 'examples/causemosify-tests/SouthSudan_2017_Apr_hires_masked_malnut.tiff'
# mp = 'examples/causemosify-tests/SouthSudan_2017_Apr_hires_masked_malnut.json'
# geo = 'admin2'
# outf = 'examples/causemosify-tests'

# fp = "examples/causemosify-tests/example.csv"
# mp = "examples/causemosify-tests/example.json"
# geo = 'admin2'
# outf = 'examples/causemosify-tests'

# start_time = timeit.default_timer()
# df = pd.DataFrame()
# print('processing...')
# df, dct = process(fp, mp, geo, outf)
# print('process time', timeit.default_timer() - start_time)
# cols = ['timestamp','country','admin1','admin2','admin3','lat','lng','feature','value']
# df.sort_values(by=cols, inplace=True)
# df.reset_index(drop=True, inplace=True)
# df.to_csv("tests/outputs/test7_single_band_tif_output.csv", index = False)
# print('\n', df.head())
# print('\n', df.tail())
# print('\n', df.shape)
# print('\nrenamed column dictionary\n', dct)
# print(f"shape\n{df.shape}\ninfo\n{df.info()}\nmemory usage\n{df.memory_usage(index=True, deep=True)}\n{df.memory_usage(index=True, deep=True).sum()}")
