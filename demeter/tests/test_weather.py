import pytest
from pyproj import CRS
from shapely.geometry import Point
from sure import expect

from demeter.weather._grid_utils import query_weather_grid


class TestUpsertGeom:
    def test_read_geom_table(self, test_db_class):
        with test_db_class.connect() as conn:
            with conn.begin():
                point = Point(-93.12345, 44.67890)
                gdf = query_weather_grid(conn, point, crs=CRS.from_epsg(4326))

        len(gdf).should.be.equal_to(1)
        list(gdf.columns).should.equal(
            [
                "world_utm_id",
                "rast_col",
                "rast_row",
                "cell_id",
                "lng_centroid",
                "lat_centroid",
                "pixel_centroid",
            ]
        )
        gdf.world_utm_id[0].should.be.equal_to(917)
        gdf.rast_col[0].should.be.equal_to(50)
        gdf.rast_row[0].should.be.equal_to(104)
        gdf.cell_id[0].should.be.equal_to(17211907)
        gdf.lng_centroid[0].should.be.equal_to(-93.10847)
        gdf.lat_centroid[0].should.be.equal_to(44.66063)
