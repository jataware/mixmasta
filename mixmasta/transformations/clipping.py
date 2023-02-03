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
