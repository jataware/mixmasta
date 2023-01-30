from .time_processor import (
    add_date_to_dataframe_as_epoch,
    rename_column_to_timestamp,
    generate_timestamp_column,
    generate_timestamp_format,
    format_time,
)


def date_type_handler(dataframe, date_dict):
    """Takes the target dataframe and a date_dict from the annotation and correctly processes it.

    Args:
        dataframe (pandas.Dataframe): Target dataframe of the annotation passed to mixmasta
        date_dict (dict): A dictionary containing all the information about the date annotation

    Returns:
        pandas.Dataframe or str: Returns a processed pandas.Dataframe if the time was a date or
        epoch time, returns a str flag if the date was day month year
    """
    date_type = date_dict["date_type"]
    print(f"DATE TYPE: {date_type}")
    primary_date = date_dict.get("primary_date")
    date_column_name = date_dict["name"]
    if date_type == "date":
        return add_date_to_dataframe_as_epoch(dataframe, date_dict, date_column_name)
    if date_type == "epoch":
        return rename_column_to_timestamp(
            dataframe=dataframe, original_date_column_name=date_column_name
        )
    if date_type == "day" or date_type == "month" or date_type == "year":
        return None
    # match [date_type, primary_date]:
    #     case ["date", True]:
    #         return add_date_to_dataframe_as_epoch(dataframe, date_column_name)
    #     case ["epoch", True]:
    #         return rename_column_to_timestamp(
    #             dataframe=dataframe, original_date_column_name=date_column_name
    #         )
    #     case ["day" | "month" | "year", True]:
    #         return "build-a-date"


def build_a_date_handler(date_mapper, dataframe):

    # Now generate the timestamp from date_df and add timestamp col to df.
    result = generate_timestamp_column(dataframe, date_mapper, "timestamp")

    # Determine the correct time format for the new date column, and
    # convert to epoch time.
    time_formatter = generate_timestamp_format(date_mapper)
    result["timestamp"] = result["timestamp"].apply(
        lambda x: format_time(str(x), time_formatter, validate=False)
    )
    return result
