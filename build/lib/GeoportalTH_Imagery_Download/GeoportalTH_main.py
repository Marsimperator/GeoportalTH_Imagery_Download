# -*- coding: utf-8 -*-
"""
### GeoportalTH_Imagery_Download ###

# Main modular executable script.
@author: Marcel Felix & Friederike Metz
"""

import os
from osgeo import ogr,osr,gdal
import geopandas
import shapely
import zipfile
from shapely.geometry import Polygon, LineString, Point
import requests
import pandas as pd


def GeoportalTh_execute(shapefile:str, year:int, dem_format:str, idfile:str):
    """Main execution function. 
    
        Executes a chain of modules which identify DEM and Op tiles, downloads
        them and uses the shapefile to clip resulting rasters to a custom extent.
        
        Parameters
        ----------
        shapefile : str
            The path to the shapefile representing the AOI
        year : int
            The year from which to download DEM and OP
        dem_format : str
            The format of the DEM data (dgm / dom)
        idfile : str
            Path to the file containing IDs, years and tile names
    """
    
    # Get the Grid file url and years
    request, dem_year = DEM_Year(year)
    
    # Automated DOWNLOAD OF TILE POLYGONS
    Tile_Grid_Download(dem_year, request)
    
    # Intersection of shapefile with dem tile polygons
    tiles = Intersection(dem_year, shapefile)
    
    # Download of dem tiles
    DEM_download(tiles, dem_format, dem_year)
    
    # Download of op tiles
    OP_download(tiles, year, idfile)
    
    # Conversion & Merge of DEM
    Merging_Tiles()
    
    # clipping DEM & OP with shapefile
    Clip_rasters("../Data/op/mergedOP.tif", 
                 "../Data/op/mergedOP_clip.tif", 
                 options = {"cutlineDSName":shapefile, "cropToCutline":True, 
                            "dstNodata":0, "dstSRS":"EPSG:25832"})
    Clip_rasters("../Data/dem/mergedDEM.tif", 
                 "../Data/dem/mergedDEM_clip.tif", 
                 options = {"cutlineDSName":shapefile, "cropToCutline":True, 
                            "dstNodata":0, "dstSRS":"EPSG:25832"})



def DEM_Year(year:int):
    """Determine dem_year.
        
        This function decides which DEM grid matches the year of interest.
        Unique OP-years thus far are:
        1997, 2008, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 
        2020
        
        Parameters
        ----------
        year : int
            The year of interest
            
        Returns
        ----------
        request : str
            The request url to send to the server
        dem_year : str
            A string defining which DEM grid basis to use
    """
    
    if year <= 2013:
        request = "https://geoportal.geoportal-th.de/hoehendaten/Uebersichten/Stand_2010-2013.zip"
        dem_year = "2010-2013"
    elif year <= 2019:
        request = "https://geoportal.geoportal-th.de/hoehendaten/Uebersichten/Stand_2014-2019.zip"
        dem_year = "2014-2019"
    elif year <= 2025:
        request = "https://geoportal.geoportal-th.de/hoehendaten/Uebersichten/Stand_2020-2025.zip"
        dem_year = "2020-2025"
    return request, dem_year



def Tile_Grid_Download(dem_year:str, request:str):
    """Downlaoding DEM tiles.
        
        This function downloads a DEM tile grid from a specified year using a 
        specified URL which is determined by DEM_Year().
        
        Parameters
        ----------
        dem_year : str
            A string defining which DEM grid basis to use
        request : str
            The request url to send to the server 
    """
    
    if not os.path.exists("../Data/"):
        os.mkdir("../Data/")
    if not os.path.exists("../Data/Stand_"+dem_year):
        os.mkdir("../Data/Stand_"+dem_year)
    
    gridfile = "../Data/Stand_"+dem_year+"/Stand_"+dem_year+".zip"
    
    # download the grid if needed
    if not os.path.exists(gridfile):
        grid_polygons = requests.get(request)
        with open(gridfile, "wb") as file:
                file.write(grid_polygons.content)
    
    # unzip the downloaded content
    with zipfile.ZipFile(gridfile, 'r') as zip_ref:
        zip_ref.extractall("../Data/Stand_"+dem_year)
         


def Intersection(dem_year:str, shapefile:str):
    """Intersection of AOI and grid.
        
        Intersects the given AOI shapefile with the grid of DEM tiles to 
        identify the DEM tile names which need to be downloaded. 
        
        Parameters
        ----------
        dem_year : str
            A string defining which DEM grid basis to use
        shapefile : str
            The path to the shapefile representing the AOI
            
        Returns
        ----------
        tiles : list
            A list of DEM tile names which intersect the AOI
    """
    
    polygon = geopandas.read_file(shapefile)
    if dem_year == "2010-2013":
        grid_path = "../Data/Stand_2010-2013/DGM2_2010-2013_Erfass-lt-Meta_UTM32-UTM_2014-12-10.shp"
    elif dem_year == "2014-2019":
        grid_path = "../Data/Stand_2014-2019/DGM1_2014-2019_Erfass-lt-Meta_UTM_2020-04-20--17127.shp"
    else:
        grid_path = "../Data/Stand_2020-2025/DGM1_2020-2025_Erfass-lt-Meta_UTM_2021-03--17127.shp"
    grid = geopandas.read_file(grid_path)
    
    tiles = []
    for poly in grid.iterrows():
        geom = poly[1].geometry
        for poly2 in polygon.iterrows():
            geom2 = poly2[1].geometry
            
            if geom.overlaps(geom2) == True:
                # alternative variant
                # end = len(poly[1].DGM_1X1)
                # tiles.append(poly[1].DGM_1X1[2:end])
                tiles.append(poly[1].NAME)
    return tiles              



def DEM_download(tiles:list, dem_format:str, dem_year:str):
    """DEM download function.
        
        Takes the identified AOI tiles and creates a proper download requests 
        for sending to the server.
        After the download the files are unzipped.
        
        Parameters
        ----------
        tiles : list
            A list of DEM tile names which intersect the AOI
        dem_format : str
            The format of the DEM data (dgm / dom)
        dem_year : str
            A string defining which DEM grid basis to use
    """
         
    for tile in tiles:
        tile = str(tile)
        
        # create dir if necessary
        if not os.path.exists("../Data/"):
            os.mkdir("../Data/")
        if not os.path.exists("../Data/dem/"):
            os.mkdir("../Data/dem/")
        
        # creating request url
        if dem_format == "dgm" or dem_format == "dom":
            request = "https://geoportal.geoportal-th.de/hoehendaten/" + dem_format.upper() + "/" + dem_format + "_" + dem_year + "/" + dem_format + "1_" + tile + "_1_th_" + dem_year + ".zip"
        #elif dem_format == "las": 
        #    request = "https://geoportal.geoportal-th.de/hoehendaten/" + dem_format.upper() + "/" + dem_format + "_" + dem_year + "/" + dem_format + "_" + tile + "_1_th_" + dem_year + ".zip+"
        
        # download the file
        current_dem = "../Data/dem/"+tile+".zip"
        # if already there, unzip and then continue with the next tile
        if os.path.exists(current_dem):
            with zipfile.ZipFile(current_dem, 'r') as zip_ref:
                zip_ref.extractall("../Data/dem/")
            continue
        
        dem_load = requests.get(request)
        demzip = []
        demzip.append(current_dem)
        
        # safe to file
        with open(current_dem, "wb") as file:
            file.write(dem_load.content)
        # with open("../Data/dem/"+tile+".zip", "wb") as file:
        #     file.write(dem_load.content)
    
        # unzipping
        with zipfile.ZipFile(current_dem, 'r') as zip_ref:
            zip_ref.extractall("../Data/dem/")



def OP_download(tiles:list, year:int, idfile:str):
    """Downloading OPs.
        
        This function takes the identified AOI tiles and searches the lookup
        table with IDs for the proper IDs to send download requests to the 
        server. The files are unzipped after download.
        
        Parameters
        ----------
        tiles : list
            A list of DEM tile names which intersect the AOI
        year : int
            The year from which to download DEM and OP
        idfile : str
            Path to the file containing IDs, years and tile names
    """
    
    # create data from lookup file
    lookup = pd.read_csv(idfile, header=0, delimiter=',')
    # unique years are: 1997, 2008, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020
    
    idlist = []
    
    # filter lookup table by year
    lookup = lookup[lookup["year"] == year]
    
    # tiles from 2018 and earlier are grouped in four; only even numbers for x and y are registered with an ID
    # therefore we need to make sure, we catch all the tiles inbetween 
    # only the lower left tile out of the four (where x&y are even) has an ID
    if year <= 2018:
        tiles_check = tiles.copy()      # a copy to iterate over while the original is being altered
        for tile in tiles_check:
            tile_x, tile_y = tile.split("_")
            tile_x, tile_y = int(tile_x), int(tile_y)
            
            if ((tile_x & 1) + (tile_y & 1)) == 1:              # if either x or y is uneven, then correct their coordinates to find the matching ID
                if tile_x & 1 == 1:
                    tile_x, tile_y = str(tile_x-1), str(tile_y)
                elif tile_y & 1 == 1:
                    tile_y, tile_x = str(tile_y-1), str(tile_x)
                    
            elif (tile_x & 1) + (tile_y & 1) == 2:              # if both are uneven
                tile_x, tile_y = str(tile_x-1), str(tile_y-1)
            
            else: continue
            
            # first make sure, the "wrong" tile is dropped from list    
            tiles.remove(tile)  
            # at last, append the newly found tile back into the list
            new_tile = tile_x + "_" + tile_y
            if not new_tile in tiles:  # but only if it's not already there!
                tiles.append(new_tile)
    
    # now find the matching IDs
    for tile in tiles:
        tile_match = lookup[lookup["tilename"] == tile]
        id_match = tile_match.values[0][0] # first row, first column
        idlist.append(id_match)
    
    # download the identified files
    for tile_id in idlist:
        # create dir if necessary
        if not os.path.exists("../Data/"):
            os.mkdir("../Data")
        if not os.path.exists("../Data/op/"):
            os.mkdir("../Data/op/")
        
        # download the file
        current_op = "../Data/op/" + str(tile_id) + ".zip"
        # if already there, unzip and then continue with the next tile
        if os.path.exists(current_op):
            with zipfile.ZipFile(current_op, 'r') as zip_ref:
                zip_ref.extractall("../Data/op/")
            continue
        
        op_load = requests.get("https://geoportal.geoportal-th.de/gaialight-th/_apps/dladownload/download.php?type=op&id=" + str(tile_id))
        op_zip = []
        op_zip.append(current_op)
        
        # safe to file
        with open(current_op, "wb") as file:
            file.write(op_load.content)
    
        with zipfile.ZipFile(current_op, 'r') as zip_ref:
            zip_ref.extractall("../Data/op/")



def Merging_Tiles():
    """Merging function.
    
        Function for merging together DEM and OP tiles into a mergedDEM.tif and
        mergedOP.tif using gdal.Translate.
    """
    
    # find the xyz files 
    demlist = []
    for file in os.scandir("../Data/dem/"):
        if file.name.endswith(".xyz"):
            demlist.append(file.path)
    
    # convert to tif 
    for xyz in demlist:
        outname = xyz.replace(".xyz","")
        gdal.Translate(outname + ".tif", xyz, outputSRS="EPSG:25832")
    
    # DEM merging
    # build virtual raster and merge
    demlist = [file.replace(".xyz",".tif") for file in demlist]
    vrt = gdal.BuildVRT("../Data/dem/merged.vrt", demlist)
    gdal.Translate("../Data/dem/mergedDEM.tif", vrt, xRes = 1, yRes = -1)
    
    # OP Merging
    oplist = []
    for file in os.scandir("../Data/op/"):
        if file.name.endswith(".tif"):
            oplist.append(file.path)
    vrt = gdal.BuildVRT("../Data/op/merged.vrt", oplist)
    gdal.Translate("../Data/op/mergedOP.tif", vrt, xRes = 0.2, yRes = -0.2)



def Clip_rasters(inp:str, outp:str, options:dict):
    """Clipping function.
    
        Clips raster files (OP and DEM) with the provided shapefile.
        
        Parameters
        ----------
        inp : str
            Path to a input raster file
        outp : str
            Path of a output raster file
        options : dict
            Additional function parameters
    """
    
    result = gdal.Warp(outp, inp, **options)
    result = None

# when executing this on it's own, specify all necessary parameters here
if __name__ == "__main__":
    # specify a root folder
    os.chdir("F:/Marcel/Backup/Dokumente/Studium/Geoinformatik/SoSe 2021/GEO419/Abschlussaufgabe/GeoportalTH_Imagery_Download/")
    
    # specify AOI via shapefile
    shapefile = "../Data/shape/shape.shp"
    
    # specify which DEM-type you want (dgm / dom)
    dem_format = "dgm"
    
    # specify the year from which data should be downloaded
    year = 2018
    
    # specify the file with Server-IDs for the grid tiles
    idfile = "../Data/idlist.txt"
    
    # execute the tool
    GeoportalTh_execute(shapefile, year, dem_format, idfile)