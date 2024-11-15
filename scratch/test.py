import os, sys
from osgeo import gdal, ogr

print(gdal.VersionInfo('–version'))
print(sys.version)

# following works
url = "/vsigs/gcp-public-data-landsat/LC08/01/044/034/LC08_L1GT_044034_20130330_20170310_01_T2/LC08_L1GT_044034_20130330_20170310_01_T2_B1.TIF"
gdal.UseExceptions()
gdal.SetConfigOption('CPL_CURL_VERBOSE', 'YES')
gdal.SetConfigOption('CPL_DEBUG', 'YES')
gdal.SetConfigOption('GS_NO_SIGN_REQUEST', 'YES')
src = gdal.Open(url)
print(gdal.Info(src))

# following doesnt work
url = "/vsigs/andrewcottam-public/MapBox_vt_14_8586_5836.geojson"
gdal.UseExceptions()
gdal.SetConfigOption('CPL_CURL_VERBOSE', 'YES')
gdal.SetConfigOption('CPL_DEBUG', 'YES')
gdal.SetConfigOption('GS_NO_SIGN_REQUEST', 'YES')
src = ogr.Open(url)
print(gdal.Info(src))
