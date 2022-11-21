# GeoportalTH_Imagery_Download

## Origin

This package was developed by Marcel Felix and Friederike Metz, students of M.Sc. Geoinformatik (GIScience and Remote Sensing) at the Friedrich-Schiller-Unisversität Jena, for the course GEO419 in SoSe2021.

## Function overview

This package allows the user to download Digital Elevation Models (DTM & DSM) and Orthophotos 
from a specified year within a specified area. The Tool utilises several python packages
which need to be installed e.g. by setting up an anaconda environment with the provided YAML 
file. Alternatively refer to the necessary packages in the pyproject.toml file.The Area of 
Interest or AOI needs to be provided in the form of a polygon, preferably a ESRI shapefile. 
The tool is able to create it's own directory environment given a root dir. Alternatively 
seperate directories for data can be specified. This tool can be used as an implementation 
for other applications by being imported. When executing the tool as a script, the execution 
parameters should be specified within the User_input function.

## installation

Enter the following into the console linked to your python env

pip install git+https://github.com/Marsimperator/GeoportalTH_Imagery_Download.git

## Required Packages

	- gdal
	- pandas
	- geopandas
	- shapely
	- joblib
