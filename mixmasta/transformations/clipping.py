import geopandas
import pandas
from shapely.geometry import MultiPolygon, Polygon


def construct_multipolygon(polygons_list):
    """Constructs a shapely Multipolygon to be used as a clipping file.

    Args:
        polygons_list (list): This should be a list of lists, where each
        list contains a dictionary with a "lat" key and a "lng" key representing the edges of the shape(s)
    """
    final_multipolygon_list = []
    for polygon_list in polygons_list:
        intermediate_list = Polygon(
            [edge.get("lat"), edge.get("lng")] for edge in polygon_list
        )
        final_multipolygon_list.append(intermediate_list)

    shape = MultiPolygon(final_multipolygon_list)

    return shape


def clip_dataframe(dataframe, geo_columns, mask):
    x_geo = dataframe[geo_columns[0]]
    y_geo = dataframe[geo_columns[1]]
    geo_dataframe = geopandas.GeoDataFrame(
        dataframe, geometry=geopandas.points_from_xy(x_geo, y_geo)
    )

    return pandas.DataFrame(geopandas.clip(geo_dataframe, mask))


def clip_time(dataframe, time_column, time_ranges):
    """Removes rows in a dataset that lie outside of a specified time range.

    Args:
        dataframe (pandas.Dataframe): Dataframe to drop rows from
        time_column (string): name of the column in the dataframe that represents the time to check.
        time_ranges (List[Object[start: datetime, end: datetime]]): A list of objects containing a start and end datetime to make a range.
    """
    dataframe[time_column] = pandas.to_datetime(dataframe[time_column])

    print(dataframe.dtypes)

    final_dataframe = pandas.DataFrame()
    for start_end_datetime in time_ranges:
        mask = (dataframe[time_column] > start_end_datetime["start"]) & (
            dataframe[time_column] <= start_end_datetime["end"]
        )
        intermediate_frame = dataframe.loc[mask]
        final_dataframe = final_dataframe.append(intermediate_frame)

    return final_dataframe
