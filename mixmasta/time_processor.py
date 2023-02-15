from datetime import datetime

import pandas

from . import constants


def format_time(time: str, time_format: str, validate: bool = True) -> int:
    """
    Description
    -----------
    Converts a time feature (time) into epoch time using `time_format` which is a strftime definition

    Parameters
    ----------
    time: str
        the time string
    time_format: str
        the strftime format for the string 'time'
    validate: bool, default True
        whether to error check the time string 'time'. Is set to False, then no error is raised if the date fails to parse, but None is returned.

    Examples
    --------

    >>> epoch = format_time('5/12/20 12:20', '%m/%d/%y %H:%M')
    """

    try:
        time_ = (
            int(datetime.strptime(time, time_format).timestamp()) * 1000
        )  # Want milliseonds
        return time_
    except Exception as e:
        if time.endswith(" 00:00:00"):
            # Depending on the date format, pandas.read_excel will read the
            # date as a Timestamp, so here it is a str with format
            # '2021-03-26 00:00:00'. For now, handle this single case until
            # there is time for a more comprehensive solution e.g. add a custom
            # date_parser function that doesn't parse diddly/squat to
            # pandas.read_excel() in process().
            return format_time(time.replace(" 00:00:00", ""), time_format, validate)
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

    for date_column_name, date_ann_dict in date_mapper.items():
        if date_ann_dict and date_ann_dict["date_type"] == "day":
            dayCol = date_column_name
        elif date_ann_dict and date_ann_dict["date_type"] == "month":
            monthCol = date_column_name
        elif date_ann_dict and date_ann_dict["date_type"] == "year":
            yearCol = date_column_name

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

    # Add the new column COLUMN NAME IS TIMESTAMP
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

    for date_column_name, date_ann_dict in date_mapper.items():
        if date_ann_dict["date_type"] == "day":
            day = date_ann_dict["time_format"]
        elif date_ann_dict["date_type"] == "month":
            month = date_ann_dict["time_format"]
        elif date_ann_dict["date_type"] == "year":
            year = date_ann_dict["time_format"]

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


def add_date_to_dataframe_as_epoch(
    dataframe, date_dict, original_date_column_name, primary_date
):
    # convert value of date_type date annotations to epochtime and rename column as 'timestamp' if it is primary date.
    dataframe.loc[:, original_date_column_name] = dataframe[
        original_date_column_name
    ].apply(lambda x: format_time(str(x), date_dict["time_format"], validate=False))

    if primary_date:
        return rename_column_to_timestamp(
            dataframe=dataframe, original_date_column_name=original_date_column_name
        )
    return dataframe


def rename_column_to_timestamp(dataframe, original_date_column_name):
    dataframe.rename(columns={original_date_column_name: "timestamp"}, inplace=True)
    return dataframe


def primary_day_month_year(primary_date_list):
    # Applied when there were primary_date year,month,day fields above.
    # These need to be combined
    # into a date and then epoch time, and added as the timestamp field.

    # Create a separate df of the associated date fields. This avoids
    # pandas upcasting the series dtypes on df.apply(); e.g., int to float,
    # or a month 9 to 9.0, which breaks generate_timestamp()
    assoc_fields = primary_date_group_mapper.keys()
    date_df = df[assoc_fields]

    # Now generate the timestamp from date_df and add timestamp col to df.
    df = generate_timestamp_column(df, primary_date_group_mapper, "timestamp")

    # Determine the correct time format for the new date column, and
    # convert to epoch time.
    time_formatter = generate_timestamp_format(primary_date_group_mapper)
    df["timestamp"] = df["timestamp"].apply(
        lambda x: format_time(str(x), time_formatter, validate=False)
    )


def day_month_year_converter(other_date_group_mapper):
    # Various date columns have been associated by the user and are not primary_date.
    # Convert to epoch time and store as a feature, do not store these separately in features.
    # Exception is the group is only two of day, month, year: leave as date.
    # Control for possibility of more than one set of assciated_columns.

    # Pop the first item in the mapper and begin building that date set.
    date_field_tuple = other_date_group_mapper.popitem()
    print(f"DEBUG: {date_field_tuple}")

    # Build a list of column names associated with the the popped date field.
    assoc_fields = [k[1] for k in date_field_tuple[1]["associated_columns"].items()]

    # Pop those mapper objects into a dict based on the column name keys in
    # assocfields list.
    assoc_columns_dict = {
        f: other_date_group_mapper.pop(f)
        for f in assoc_fields
        if f in other_date_group_mapper
    }

    # Add the first popped tuple into the assoc_columns dict where the key is the
    # first part of the tuple; the value is the 2nd part.
    assoc_columns_dict[date_field_tuple[0]] = date_field_tuple[1]

    # Add the first popped tuple column name to the list of associated fields.
    assoc_fields.append(date_field_tuple[0])

    # TODO: If day and year are associated to each other and month, but
    # month is not associated to those fields, then at this point assoc_fields
    # will be the three values, and assoc_columns will contain only day and
    # year. This will error out below. It is assumed that SpaceTag will
    # control for this instance.

    # If there is no primary_time column for timestamp, which would have
    # been created above with primary_date_group_mapper, or farther above
    # looping mapper["date"], attempt to generate from date_type = Month,
    # Day, Year features. Otherwise, create a new column name from the
    # concatenation of the associated date fields here.
    if not "timestamp" in df.columns:
        new_column_name = "timestamp"
    else:
        new_column_name = generate_column_name(assoc_fields)

    # Create a separate df of the associated date fields. This avoids
    # pandas upcasting the series dtypes on df.apply(); e.g., int to float,
    # or a month 9 to 9.0, which breaks generate_timestamp()
    date_df = df[assoc_fields]

    # Now generate the timestamp from date_df and add timestamp col to df.
    df = generate_timestamp_column(df, assoc_columns_dict, new_column_name)

    # Determine the correct time format for the new date column, and
    # convert to epoch time only if all three date components (day, month,
    # year) are present; otherwise leave as a date string.
    date_types = [v["date_type"] for k, v in assoc_columns_dict.items()]
    if len(frozenset(date_types).intersection(constants.MONTH_DAY_YEAR)) == 3:
        time_formatter = generate_timestamp_format(assoc_columns_dict)
        df.loc[:, new_column_name] = df[new_column_name].apply(
            lambda x: format_time(str(x), time_formatter, validate=False)
        )

    # Let SpaceTag know those date columns were renamed to a new column.
    renamed_col_dict[new_column_name] = assoc_fields

    # timestamp is a protected column, so don't add to features.
    if new_column_name != "timestamp":
        # Handle edge case of each date field in assoc_fields qualifying
        # the same column e.g. day/month/year are associated and qualify
        # a field. In this case, the new_column_name
        qualified_col = build_date_qualifies_field(qualified_col_dict, assoc_fields)
        if qualified_col is None:
            features.append(new_column_name)
        else:
            qualified_col_dict[qualified_col] = [new_column_name]


def generate_column_name(field_list: list) -> str:
    """
    Description
    -----------
    Contatenate a list of column fields into a single column name.

    Parameters
    ----------
    field_list: list[str] of column names

    Returns
    -------
    str: new column name

    """
    return "".join(sorted(field_list))
