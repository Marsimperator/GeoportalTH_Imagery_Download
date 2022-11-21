# GeoportalTH_Imagery_Download - geothimagery

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

## Installation

Enter the following into the console linked to your python env

pip install git+https://github.com/Marsimperator/GeoportalTH_Imagery_Download.git

#### Problems with GDAL installation?

Try: conda install -c conda-forge gdal

## Required Packages

	- gdal
	- pandas
	- geopandas
	- shapely
	- joblib


## How to Use?

>"GeoportalTH_main" is the file containing all necessary modules to download and process data.
This file can be executed as a script. If so parameters should be specified in the "user_input" function.

If imported as module it should be startet using the "geoportalth_execute" function, where in it's call all parameters can be specified. 

All modules within >GeoportalTH_main< have been developed with a focus on modularity. So a user can use these modules by oneself for specific sub-tasks.

------------
>"op_tile_finder" is the file containing functions which determine the orthophoto-IDs which are needed to download orthophotos from the server. The main function "op_tilelist_creator" creates a .txt file which serves as a lookup table.

Files from previous executions are available in the id_lists directory

