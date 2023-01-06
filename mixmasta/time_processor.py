from datetime import datetime

import pandas


def format_time(t: str, time_format: str, validate: bool = True) -> int:
    """
    Description
    -----------
    Converts a time feature (t) into epoch time using `time_format` which is a strftime definition

    Parameters
    ----------
    t: str
        the time string
    time_format: str
        the strftime format for the string t
    validate: bool, default True
        whether to error check the time string t. Is set to False, then no error is raised if the date fails to parse, but None is returned.

    Examples
    --------

    >>> epoch = format_time('5/12/20 12:20', '%m/%d/%y %H:%M')
    """

    try:
        t_ = (
            int(datetime.strptime(t, time_format).timestamp()) * 1000
        )  # Want milliseonds
        return t_
    except Exception as e:
        if t.endswith(" 00:00:00"):
            # Depending on the date format, pandas.read_excel will read the
            # date as a Timestamp, so here it is a str with format
            # '2021-03-26 00:00:00'. For now, handle this single case until
            # there is time for a more comprehensive solution e.g. add a custom
            # date_parser function that doesn't parse diddly/squat to
            # pandas.read_excel() in process().
            return format_time(t.replace(" 00:00:00", ""), time_format, validate)
        print(e)
        if validate:
            raise Exception(e)
        else:
            return None


def generate_timestamp_column(
    df: pandas.DataFrame, date_mapper: dict, column_name: str
) -> pandas.DataFrame:
    """
    Description
    -----------
    Efficiently add a new timestamp column to a dataframe. It avoids the use of df.apply
    which appears to be much slower for large dataframes. Defaults to 1/1/1970 for
    missing day/month/year values.

    Parameters
    ----------
    df: pandas.DataFrame
        our data
    date_mapper: dict
        a schema mapping (JSON) for the dataframe filtered for "date_type" equal to
        Day, Month, or Year. The format is screwy for our purposes here and could
        be reafactored.
    column_name: str
        name of the new column e.g. timestamp for primary_time, year1month1day1
        for a concatneated name from associated date fields.

    Examples
    --------
    This example adds the generated series to the source dataframe.
    >>> df = df.join(df.apply(generate_timestamp, date_mapper=date_mapper,
            column_name="year1month1day", axis=1))
    """

    # Identify which date values are passed.
    dayCol = None
    monthCol = None
    yearCol = None

    for kk, vv in date_mapper.items():
        if vv and vv["date_type"] == "day":
            dayCol = kk
        elif vv and vv["date_type"] == "month":
            monthCol = kk
        elif vv and vv["date_type"] == "year":
            yearCol = kk

    # For missing date values, add a column to the dataframe with the default
    # value, then assign that to the day/month/year var. If the dataframe has
    # the date value, assign day/month/year to it after casting as a str.
    if dayCol:
        day = df[dayCol].astype(str)
    else:
        df.loc[:, "day_generate_timestamp_column"] = "1"
        day = df["day_generate_timestamp_column"]

    if monthCol:
        month = df[monthCol].astype(str)
    else:
        df.loc[:, "month_generate_timestamp_column"] = "1"
        month = df["month_generate_timestamp_column"]

    if yearCol:
        year = df[yearCol].astype(str)
    else:
        df.loc[:, "year_generate_timestamp_column"] = "01"
        year = df["year_generate_timestamp_column"]

    # Add the new column
    df.loc[:, column_name] = month + "/" + day + "/" + year

    # Delete the temporary columns
    if not dayCol:
        del df["day_generate_timestamp_column"]

    if not monthCol:
        del df["month_generate_timestamp_column"]

    if not yearCol:
        del df["year_generate_timestamp_column"]

    return df


def generate_timestamp_format(date_mapper: dict) -> str:
    """
    Description
    -----------
    Generates a the time format for day,month,year dates based on each's
    specified time_format.

    Parameters
    ----------
    date_mapper: dict
        a dictionary for the schema mapping (JSON) for the dataframe filtered
        for "date_type" equal to Day, Month, or Year.

    Output
    ------
    e.g. "%m/%d/%Y"
    """

    day = "%d"
    month = "%m"
    year = "%y"

    for kk, vv in date_mapper.items():
        if vv["date_type"] == "day":
            day = vv["time_format"]
        elif vv["date_type"] == "month":
            month = vv["time_format"]
        elif vv["date_type"] == "year":
            year = vv["time_format"]

    return str.format("{}/{}/{}", month, day, year)


def build_date_qualifies_field(qualified_col_dict: dict, assoc_fields: list) -> str:
    """
    Description
    -----------
    Handle edge case of each date field in assoc_fields qualifying the same
    column e.g. day/month/year are associated and qualify a field. In this
    case, the new_column_name.

    if assoc_fields is found as a value in qualified_col_dict, return the key

    Parameters
    ----------
    qualified_col_dict: dict
        {'pop': ['month_column', 'day_column', 'year_column']}

    assoc_fields: list
        ['month_column', 'day_column', 'year_column']

    """
    for k, v in qualified_col_dict.items():
        if v == assoc_fields:
            return k

    return None
