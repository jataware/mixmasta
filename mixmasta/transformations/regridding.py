import geopandas
import numpy
import pandas
from shapely.geometry import MultiPolygon, Polygon, box


def regrid_dataframe(dataframe, geo_columns):
    x_geo = dataframe[geo_columns[0]]
    y_geo = dataframe[geo_columns[1]]
    geo_dataframe = geopandas.GeoDataFrame(
        dataframe, geometry=geopandas.points_from_xy(x_geo, y_geo)
    )

    xmin, ymin, xmax, ymax = geo_dataframe.total_bounds

    n_cells = 30
    cell_size = (xmax - xmin) / n_cells

    grid_cells = []

    for x0 in numpy.arange(xmin, xmax + cell_size, cell_size):
        for y0 in numpy.arange(ymin, ymax + cell_size, cell_size):
            # bounds
            x1 = x0 - cell_size
            y1 = y0 + cell_size
            grid_cells.append(box(x0, y0, x1, y1))

    cell = geopandas.GeoDataFrame(grid_cells, columns=["geometry"])

    merged = geopandas.sjoin(geo_dataframe, cell, how="left", op="within")
    print(f"MERGED: {merged}")

    dissolve = merged.dissolve(by="index_right", aggfunc="sum")
    print(f"DISSOLVED: {dissolve}")

    cell.loc[dissolve.index, "fatalities"] = dissolve.fatalities.values

    print(f"REGRID DATA: {cell}")
