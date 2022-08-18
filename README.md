# GeoportalTH_Imagery_Download

This package allows the user to download Digital Elevation Models and Orthophotos 
from a specified year within a specified area. The Tool utilises several python packages
which need to be installed e.g. by setting up an anaconda environment with the provided YAML 
file. The Area of Interest or AOI needs to be procided in the form of a polygon, 
preferably a ESRI shapefile. No specific directories need to be setup as they will be 
setup automatically by the tool. This tool can be used as an implementation for other 
applications by importing it. When executing the tool as a script, the execution 
parameters should be specified within the User_input function which is called by 
the if __name__ == "__main__" statement.

The required packages for this tool contain:
	- osgeo (gdal)
	- pandas
	- geopandas
	- shapely
	- zipfile
	- requests

The required packages for the Jupyter Notebook visualisation additionally contain:
	- ipyleaflet
	- ipywidgets
	- utm