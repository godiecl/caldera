# caldera main project: apliCacion mAchine Learning preDiccion rEmociones Rio mAipo

import logging
import os
import sys
from contextlib import contextmanager
from timeit import default_timer as timer
from typing import Iterator

# need to test the speed
os.environ['USE_PYGEOS'] = '0'

import dask_geopandas
import geopandas as gpd
import rasterio


class StreamToLogger:
    """
    Fake file-like stream object that redirects writes to a logger instance.
    """

    def __init__(self, logger, level):
        self.logger = logger
        self.level = level
        self.linebuf = ''

    def write(self, buf):
        if buf != '\n':
            for line in buf.rstrip().splitlines():
                if line.rstrip() != "^":
                    self.logger.log(self.level, line.rstrip())

    def flush(self):
        pass


# logger configuration
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s.%(msecs)03d %(levelname)s\t: %(message)s',  # format='%(asctime)s %(message)s',
                    datefmt='%Y%m%d %H:%M:%S',
                    )
log = logging.getLogger()
sys.stdout = StreamToLogger(log, logging.INFO)
sys.stderr = StreamToLogger(log, logging.ERROR)

# on/off logger
logging.getLogger().disabled = False


# this thing may accelerate the process
# gpd.options.use_pygeos = True


# timer
@contextmanager
def time_it(name='') -> Iterator[None]:
    tic: float = timer()
    try:
        yield
    finally:
        toc: float = timer()
        if name != '':
            print(f"Time({name}) = {(toc - tic):.2f}s")
        else:
            print(f"Time = {(toc - tic):.2f}s")


# import matplotlib.pyplot as plt
# from osgeo import gdal

# import rasterio
# from rasterio.plot import show

# gdal.SetConfigOption('SHAPE_RESTORE_SHX', 'YES')


# show the versions and info
gpd.show_versions()
log.info("Using pygeos: %s" % gpd.options.use_pygeos)

with time_it('main'):
    log.info("")
    log.info("Starting the process ..")

    # path: C:\Tesis_PhD\Proc_Autom_Python\POINT_FROM_AlosPalsar12m\
    shapeFile = "POINT_FROM_AlosPalsar12m.shp"
    rasterFile = "SLOPE_FROM_AlosPalsar12m_INT_.tif"  # "SLOPE_FROM_AlosPalsar12m_.tif"  # "SLOPE_FROM_AlosPalsar12m_INT_.tif"

    # opening shapefile:
    log.info("Reading data from shapefile: %s, please wait .." % shapeFile)
    with time_it('geopandas.read_file'):
        # gdf = gpd.read_file(
        gdf = dask_geopandas.read_file(
            shapeFile,
            npartitions=40
            # include_fields=["X", "Y"]
        )
        log.info("CRS from %s: %s" % (shapeFile, gdf.crs))
        # log.info("Points: %s." % len(gdf.geometry))
        # gdf.plot()

    log.info("Opening rasterio from %s, please wait .." % rasterFile)
    with time_it('rasterio.open'):
        ndviRaster = rasterio.open(
            rasterFile
        )
        log.info("Raster CRS\t: %s." % ndviRaster.crs)
        log.info("Raster count\t: %d." % ndviRaster.count)

        # show(ndviRaster)

    with time_it('points'):
        log.info("Counting points, please wait ..")
        count = len(gdf.geometry)
        log.info("%d points founded.", count)

        for idx, point in enumerate(gdf['geometry']):
            x = point.xy[0][0]
            y = point.xy[1][0]
            row, col = ndviRaster.index(x, y)
            value = ndviRaster.read(1)[row, col]
            log.info("Point %d (%.6f%%) (%.4f, %.4f) has raster value (%.2f, %.2f) of: %d" % (
                idx, idx / count, x, y, row, col, value))

    log.info("Done.")
