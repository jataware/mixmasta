import logging
import os

from fuzzywuzzy import fuzz
from fuzzywuzzy import process as fuzzyprocess
import geopandas as gpd
import geofeather as gf
import pandas
from pathlib import Path
import pkg_resources
from shapely import speedups
from shapely.geometry import Point
import timeit

from . import constants


def match_geo_names(
    admin: str,
    df: pandas.DataFrame,
    resolve_to_gadm_geotypes: list,
    gadm: gpd.GeoDataFrame = None,
) -> pandas.DataFrame:
    """
    Assumption
    ----------
    Country was selected by drop-down on file submission, column "country"
    is present in the data frame, and lng/lat is not being used for geocoding.

    Parameters
    ----------
    admin: str
        the level to geocode to. Either 'admin2' or 'admin3'
    df: pandas.DataFrame
        the uploaded dataframe
    resolve_to_gadm_geotypes:
        list of geotypes marked resolve_to_gadm = True e.g. ["admin1", "country"]
    gadm: gpd.GeoDataFrame, default None
        optional specification of a GeoDataFrame of GADM shapes of the appropriate
        level (admin2/3) for geocoding

    Result
    ------
    A pandas.Dataframe produced by modifying the parameter df.

    """
    print("geocoding ...")
    flag = speedups.available
    if flag == True:
        speedups.enable()

    cdir = os.path.expanduser("~")
    download_data_folder = f"{cdir}/mixmasta_data"

    # only load GADM if it wasn't explicitly passed to the function.
    if gadm is not None:
        # logging.info("GADM geo dataframe has been provided.")
        pass
    else:
        logging.info("GADM has not been provided; loading now.")

        if admin == "admin2":
            gadm_fn = f"gadm36_2.feather"
        else:
            gadm_fn = f"gadm36_3.feather"

        gadmDir = f"{download_data_folder}/{gadm_fn}"
        gadm = gf.from_geofeather(gadmDir)

        gadm["country"] = gadm["NAME_0"]
        gadm["state"] = gadm["NAME_1"]
        gadm["admin1"] = gadm["NAME_1"]
        gadm["admin2"] = gadm["NAME_2"]

        if admin == "admin2":
            gadm = gadm[["country", "state", "admin1", "admin2"]]
        else:
            gadm["admin3"] = gadm["NAME_3"]
            gadm = gadm[["country", "state", "admin1", "admin2", "admin3"]]

    # Filter GADM for countries in df.
    countries = df["country"].unique()

    # Correct country names.
    if constants.GEO_TYPE_COUNTRY in resolve_to_gadm_geotypes:
        gadm_country_list = gadm["country"].unique()
        unknowns = df[~df.country.isin(gadm_country_list)].country.tolist()
        for unk in unknowns:
            try:
                match = fuzzyprocess.extractOne(
                    unk, gadm_country_list, scorer=fuzz.partial_ratio
                )
            except Exception as e:
                match = None
                logging.error(f"Error in match_geo_names: {e}")
            if match != None:
                df.loc[df.country == unk, "country"] = match[0]

    # Filter GADM dicitonary for only those countries (ie. speed up)
    gadm = gadm[gadm["country"].isin(countries)]

    # Loop by country using gadm dict filtered for that country.
    for c in countries:
        # The following ignores admin1 / admin2 pairs; it only cares if those
        # values exist for the appropriate country.

        # Get list of admin1 values in df but not in gadm. Reduce list for country.
        if constants.GEO_TYPE_ADMIN1 in resolve_to_gadm_geotypes:
            admin1_list = gadm[gadm.country == c]["admin1"].unique()
            if admin1_list is not None and all(admin1_list) and "admin1" in df:
                unknowns = df[
                    (df.country == c) & ~df.admin1.isin(admin1_list)
                ].admin1.tolist()
                unknowns = [
                    x for x in unknowns if pandas.notnull(x) and x.strip()
                ]  # remove Nan
                for unk in unknowns:
                    match = fuzzyprocess.extractOne(
                        unk, admin1_list, scorer=fuzz.partial_ratio
                    )
                    if match != None:
                        df.loc[df.admin1 == unk, "admin1"] = match[0]

        # Get list of admin2 values in df but not in gadm. Reduce list for country.
        if constants.GEO_TYPE_ADMIN2 in resolve_to_gadm_geotypes:
            admin2_list = gadm[gadm.country == c]["admin2"].unique()
            if admin2_list is not None and all(admin2_list) and "admin2" in df:
                unknowns = df[
                    (df.country == c) & ~df.admin2.isin(admin2_list)
                ].admin2.tolist()
                unknowns = [
                    x for x in unknowns if pandas.notnull(x) and x.strip()
                ]  # remove Nan
                for unk in unknowns:
                    match = fuzzyprocess.extractOne(
                        unk, admin2_list, scorer=fuzz.partial_ratio
                    )
                    if match != None:
                        df.loc[df.admin2 == unk, "admin2"] = match[0]

        if admin == "admin3" and constants.GEO_TYPE_ADMIN3 in resolve_to_gadm_geotypes:
            # Get list of admin3 values in df but not in gadm. Reduce list for country.
            admin3_list = gadm[gadm.country == c]["admin3"].unique()
            if admin3_list is not None and all(admin3_list) and "admin3" in df:
                unknowns = df[
                    (df.country == c) & ~df.admin3.isin(admin3_list)
                ].admin3.tolist()
                unknowns = [
                    x for x in unknowns if pandas.notnull(x) and x.strip()
                ]  # remove Nan
                for unk in unknowns:
                    match = fuzzyprocess.extractOne(
                        unk, admin3_list, scorer=fuzz.partial_ratio
                    )
                    if match != None:
                        df.loc[df.admin3 == unk, "admin3"] = match[0]

    return df


def geocode(
    admin: str,
    df: pandas.DataFrame,
    x: str = "longitude",
    y: str = "latitude",
    gadm: gpd.GeoDataFrame = None,
    df_geocode: pandas.DataFrame = pandas.DataFrame(),
) -> pandas.DataFrame:
    """
    Description
    -----------
    Takes a dataframe containing coordinate data and geocodes it to GADM (https://gadm.org/)

    GEOCODES to ADMIN 0, 1, 2 OR 3 LEVEL

    Parameters
    ----------
    admin: str
        the level to geocode to. 'admin0' to 'admin3'
    df: pandas.DataFrame
        a pandas dataframe containing point data
    x: str, default 'longitude'
        the name of the column containing longitude information
    y: str, default 'latitude'
        the name of the column containing latitude data
    gadm: gpd.GeoDataFrame, default None
        optional specification of a GeoDataFrame of GADM shapes of the appropriate
        level (admin2/3) for geocoding
    df_geocode: pandas.DataFrame, default pandas.DataFrame()
        cached lat/long geocode library

    Examples
    --------
    Geocoding a dataframe with columns named 'lat' and 'lon'

    >>> df = geocode(df, x='lon', y='lat')

    """

    flag = speedups.available
    if flag == True:
        speedups.enable()

    cdir = os.path.expanduser("~")
    download_data_folder = f"{cdir}/mixmasta_data"

    # Only load GADM if it wasn't explicitly passed to the function.
    if gadm is not None:
        logging.info("GADM geo dataframe has been provided.")
    else:
        logging.info("GADM has not been provided; loading now.")

        if admin in ["admin0", "country"]:
            gadm_fn = f"gadm36_2.feather"
            gadmDir = f"{download_data_folder}/{gadm_fn}"
            gadm = gf.from_geofeather(gadmDir)
            gadm["country"] = gadm["NAME_0"]
            gadm = gadm[["geometry", "country"]]

        elif admin == "admin1":
            gadm_fn = f"gadm36_2.feather"
            gadmDir = f"{download_data_folder}/{gadm_fn}"
            gadm = gf.from_geofeather(gadmDir)
            gadm["country"] = gadm["NAME_0"]
            # gadm["state"] = gadm["NAME_1"]
            gadm["admin1"] = gadm["NAME_1"]
            # gadm = gadm[["geometry", "country", "state", "admin1"]]
            gadm = gadm[["geometry", "country", "admin1"]]

        elif admin == "admin2":
            gadm_fn = f"gadm36_2.feather"
            gadmDir = f"{download_data_folder}/{gadm_fn}"
            gadm = gf.from_geofeather(gadmDir)
            gadm["country"] = gadm["NAME_0"]
            # gadm["state"] = gadm["NAME_1"]
            gadm["admin1"] = gadm["NAME_1"]
            gadm["admin2"] = gadm["NAME_2"]
            # gadm = gadm[["geometry", "country", "state", "admin1", "admin2"]]
            gadm = gadm[["geometry", "country", "admin1", "admin2"]]

        elif admin == "admin3":
            gadm_fn = f"gadm36_3.feather"
            gadmDir = f"{download_data_folder}/{gadm_fn}"
            gadm = gf.from_geofeather(gadmDir)
            gadm["country"] = gadm["NAME_0"]
            # gadm["state"] = gadm["NAME_1"]
            gadm["admin1"] = gadm["NAME_1"]
            gadm["admin2"] = gadm["NAME_2"]
            gadm["admin3"] = gadm["NAME_3"]
            # gadm = gadm[["geometry", "country", "state", "admin1", "admin2", "admin3"]]
            gadm = gadm[["geometry", "country", "admin1", "admin2", "admin3"]]

    start_time = timeit.default_timer()

    # 1) Drop x,y duplicates from data frame.
    df_drop_dup_geo = df[[x, y]].drop_duplicates(subset=[x, y])

    # 2) Get x,y not in df_geocode.
    if not df_geocode.empty and not df_drop_dup_geo.empty:
        df_drop_dup_geo = df_drop_dup_geo.merge(
            df_geocode, on=[x, y], how="left", indicator=True
        )
        df_drop_dup_geo = df_drop_dup_geo[df_drop_dup_geo["_merge"] == "left_only"]
        df_drop_dup_geo = df_drop_dup_geo[[x, y]]

    if not df_drop_dup_geo.empty:
        # dr_drop_dup_geo contains x,y not in df_geocode; so, these need to be
        # geocoded and added to the df_geocode library.

        # 3) Apply Point() to create the geometry col.
        df_drop_dup_geo.loc[:, "geometry"] = df_drop_dup_geo.apply(
            lambda row: Point(row[x], row[y]), axis=1
        )

        # 4) Sjoin unique geometries with GADM.
        gdf = gpd.GeoDataFrame(df_drop_dup_geo)

        # Spatial merge on GADM to obtain admin areas.
        gdf = gpd.sjoin(
            gdf,
            gadm,
            how="left",
            op="within",
            lsuffix="mixmasta_left",
            rsuffix="mixmasta_geocoded",
        )
        del gdf["geometry"]
        del gdf["index_mixmasta_geocoded"]

        # 5) Add the new geocoding to the df_geocode lat/long geocode library.
        if not df_geocode.empty:
            df_geocode = df_geocode.append(gdf)
        else:
            df_geocode = gdf

    # 6) Merge df and df_geocode on x,y
    gdf = df.merge(df_geocode, how="left", on=[x, y])

    return pandas.DataFrame(gdf)


def get_iso_country_dict(iso_list: list) -> dict:
    """
    Description
    -----------
    iso2 or iso3 is used as primary_geo and therefore the country column.
    Load the custom iso lookup table and return a dictionary of the iso codes
    as keys and the country names as values. Assume all list items are the same
    iso type.

    Parameters
    ----------
    iso_list:
        list of iso2 or iso3 codes

    Returns
    -------
    dict:
        key: iso code; value: country name
    """

    dct = {}
    if iso_list:
        iso_df = pandas.DataFrame
        try:
            # The necessary code to load from pkg doesn't currently work in VS
            # Code Debug, so wrap in try/except.
            # iso_df = pandas.read_csv(pkg_resources.resource_stream(__name__, 'data/iso_lookup.csv'))
            with pkg_resources.resource_stream(__name__, "data/iso_lookup.csv") as f:
                iso_df = pandas.read_csv(f)
            # path = Path(__file__).parent / "data/iso_lookup.csv"
            # iso_df = pandas.read_csv(path)
        except:
            # Local VS Code load.
            path = Path(__file__).parent / "data/iso_lookup.csv"
            iso_df = pandas.read_csv(path)

        if iso_df.empty:
            return dct

        if len(iso_list[0]) == 2:
            for iso in iso_list:
                if iso in iso_df["iso2"].values:
                    dct[iso] = iso_df.loc[iso_df["iso2"] == iso]["country"].item()
        else:
            for iso in iso_list:
                if iso in iso_df["iso3"].values:
                    dct[iso] = iso_df.loc[iso_df["iso3"] == iso]["country"].item()

    return dct
