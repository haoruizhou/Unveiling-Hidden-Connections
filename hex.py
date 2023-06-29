import pandas as pd
import os

os.environ['USE_PYGEOS'] = '0'
import geopandas as gpd
import warnings;

warnings.filterwarnings('ignore', 'GeoSeries.notna', UserWarning)
from concurrent.futures import ProcessPoolExecutor
import h3
import os

num_cores = os.cpu_count()

"""
This script provides functions to convert geographical coordinates to H3 hexagonal indices, and add these indices to a GeoDataFrame. 

The main function is `append_hex_column`, which takes a GeoDataFrame and appends a new column 'hex' with the corresponding H3 indices. The resolution of the H3 indices can be specified, and the function supports parallel processing for speed. 

It uses the H3 library, developed by Uber, for the geographical to H3 conversion, and GeoPandas for geographical data processing.

This script also suppresses the UserWarning 'GeoSeries.notna' which is commonly raised during geographical data processing.

Example usage:
    gdf = geopandas.read_file('path')
    gdf_with_hex = append_hex_column(gdf, resolution=8)
"""


def lat_lng_to_h3(lat, lng, resolution):
    """
    Converts a latitude and longitude to an H3 index.

    Parameters:
    lat (float): The latitude.
    lng (float): The longitude.
    resolution (int): The desired H3 resolution.

    Returns:
    (str): The H3 index.
    """
    return h3.geo_to_h3(lat, lng, resolution)


def process_row(row, resolution):
    """
    Extracts the latitude and longitude from a GeoDataFrame row and converts it to an H3 index.

    Parameters:
    row (GeoSeries): The GeoDataFrame row.
    resolution (int): The desired H3 resolution.

    Returns:
    (str): The H3 index.
    """
    lat, lng = row['geometry'].y, row['geometry'].x
    hex_id = lat_lng_to_h3(lat, lng, resolution)
    return hex_id


def append_hex_column(gdf, resolution=8, n_workers=10):
    """
    Appends a column of H3 indices to a GeoDataFrame, based on the latitude and longitude in its 'geometry' column.

    Parameters:
    gdf (GeoDataFrame): The input GeoDataFrame.
    resolution (int): The desired H3 resolution. Default is 8.
    n_workers (int): The number of workers to use for parallel processing. Default is 10.

    Returns:
    gdf (GeoDataFrame): The input GeoDataFrame with the appended 'hex' column.
    """
    gdf = gdf[~gdf['geometry'].is_empty & gdf['geometry'].notna()]
    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        hexagons = list(executor.map(process_row, [row for _, row in gdf.iterrows()], [resolution] * len(gdf)))

    gdf['hex'] = hexagons
    return gdf
