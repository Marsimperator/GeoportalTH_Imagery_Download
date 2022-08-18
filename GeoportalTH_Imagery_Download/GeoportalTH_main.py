# -*- coding: utf-8 -*-
"""
### GeoportalTH_Imagery_Download ###

# Main modular executable script.
@author: Marcel Felix & Friederike Metz
"""

import os
from osgeo import gdal
import geopandas
import zipfile
from shapely.geometry import Polygon, LineString, Point
import requests
import pandas as pd


def GeoportalTh_execute(root:str, shapefile:str, year:int, dem_format:str, idfile:str):
    """Main execution function. 
    
        Executes a chain of modules which identify DEM and Op tiles, downloads
        them and uses the shapefile to clip resulting rasters to a custom extent.
        
        Parameters
        ----------
        root : str
            Root directory
        shapefile : str
            The path to the shapefile representing the AOI
        year : int
            The year from which to download DEM and OP
        dem_format : str
            The format of the DEM data (dgm / dom)
        idfile : str
            Path to the file containing IDs, years and tile names
    """
 
    # provide new paths
    data_dir = os.path.join(root, "Data")
    dem_dir = os.path.join(data_dir, "dem")
    op_dir = os.path.join(data_dir, "op")   
 
    # Get the Grid file url and years
    request, dem_year = DEM_Year(year)
    
    # Automated DOWNLOAD OF TILE POLYGONS
    Tile_Grid_Download(data_dir, dem_year, request)
    
    # Intersection of shapefile with dem tile polygons
    tiles = Intersection(data_dir, dem_year, shapefile)
    
    # Download of dem tiles
    DEM_download(dem_dir, tiles, dem_format, dem_year)
    
    # Download of op tiles
    OP_download(op_dir, tiles, year, idfile)
    
    # Conversion & Merge of DEM
    Merging_Tiles(dem_dir, op_dir)
    
    # clipping DEM & OP with shapefile
    Clip_rasters(os.path.join(op_dir, "mergedOP.tif"), 
                 os.path.join(op_dir, "mergedOP_clip.tif"), 
                 options = {"cutlineDSName":shapefile, "cropToCutline":True, 
                            "dstNodata":0, "dstSRS":"EPSG:25832"})
    Clip_rasters(os.path.join(dem_dir, "mergedDEM.tif"), 
                 os.path.join(dem_dir, "mergedDEM_clip.tif"), 
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
    else: raise ValueError("The provided year is not yet supported with an URL")
    
    return request, dem_year



def Tile_Grid_Download(data_dir:str, dem_year:str, request:str):
    """Downlaoding DEM tiles.
        
        This function downloads a DEM tile grid from a specified year using a 
        specified URL which is determined by DEM_Year().
        
        Parameters
        ----------
        data_dir : str
            A directory where the grid folder & files will be saved 
        dem_year : str
            A string defining which DEM grid basis to use
        request : str
            The request url to send to the server 
    """
    
    if not os.path.exists(data_dir):
        os.mkdir(data_dir)
    if not os.path.exists(os.path.join(data_dir, "Stand_"+dem_year)):
        os.mkdir(os.path.join(data_dir, "Stand_"+dem_year))
    
    gridfile = os.path.join(data_dir, "Stand_"+dem_year, "Stand_"+dem_year+".zip")
    
    # download the grid if needed
    if not os.path.exists(gridfile):
        grid_polygons = requests.get(request)
        with open(gridfile, "wb") as file:
                file.write(grid_polygons.content)
    
    # unzip the downloaded content
    with zipfile.ZipFile(gridfile, 'r') as zip_ref:
        zip_ref.extractall(os.path.join(data_dir, "Stand_"+dem_year))
         


def Intersection(data_dir:str, dem_year:str, shapefile:str):
    """Intersection of AOI and grid.
        
        Intersects the given AOI shapefile with the grid of DEM tiles to 
        identify the DEM tile names which need to be downloaded. 
        
        Parameters
        ----------
        data_dir : str
            A directory where the grid folders with files are located 
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
        grid_path = os.path.join(data_dir, "Stand_2010-2013", "DGM2_2010-2013_Erfass-lt-Meta_UTM32-UTM_2014-12-10.shp")
    elif dem_year == "2014-2019":
        grid_path = os.path.join(data_dir, "Stand_2014-2019", "DGM1_2014-2019_Erfass-lt-Meta_UTM_2020-04-20--17127.shp")
    elif dem_year == "2020-2025":
        grid_path = os.path.join(data_dir, "Stand_2020-2025", "DGM1_2020-2025_Erfass-lt-Meta_UTM_2021-03--17127.shp")
    else: raise ValueError("The provided timeframe is not yet supported by this function.")
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



def DEM_download(dem_dir:str, tiles:list, dem_format:str, dem_year:str):
    """DEM download function.
        
        Takes the identified AOI tiles and creates a proper download requests 
        for sending to the server.
        After the download the files are unzipped.
        
        Parameters
        ----------
        dem_dir : str
            A directory where the DEM files will be saved
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
        if not os.path.exists(dem_dir):
            os.mkdir(dem_dir)
        
        # creating request url
        if dem_format == "dgm" or dem_format == "dom":
            request = "https://geoportal.geoportal-th.de/hoehendaten/" + dem_format.upper() + "/" + dem_format + "_" + dem_year + "/" + dem_format + "1_" + tile + "_1_th_" + dem_year + ".zip"
        #elif dem_format == "las": 
        #    request = "https://geoportal.geoportal-th.de/hoehendaten/" + dem_format.upper() + "/" + dem_format + "_" + dem_year + "/" + dem_format + "_" + tile + "_1_th_" + dem_year + ".zip+"
        
        # download the file
        current_dem = os.path.join(dem_dir, tile+".zip")
        # if already there, unzip and then continue with the next tile
        if os.path.exists(current_dem):
            with zipfile.ZipFile(current_dem, 'r') as zip_ref:
                zip_ref.extractall(dem_dir)
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
            zip_ref.extractall(dem_dir)



def OP_download(op_dir:str, tiles:list, year:int, idfile:str):
    """Downloading OPs.
        
        This function takes the identified AOI tiles and searches the lookup
        table with IDs for the proper IDs to send download requests to the 
        server. The files are unzipped after download.
        
        Parameters
        ----------
        op_dir : str
            A directory where the OP files will be saved
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
        if not os.path.exists(op_dir):
            os.mkdir(op_dir)
        
        # download the file
        current_op = os.path.join(op_dir, str(tile_id)+".zip")
        # if already there, unzip and then continue with the next tile
        if os.path.exists(current_op):
            with zipfile.ZipFile(current_op, 'r') as zip_ref:
                zip_ref.extractall(op_dir)
            continue
        
        op_load = requests.get("https://geoportal.geoportal-th.de/gaialight-th/_apps/dladownload/download.php?type=op&id=" + str(tile_id))
        op_zip = []
        op_zip.append(current_op)
        
        # safe to file
        with open(current_op, "wb") as file:
            file.write(op_load.content)
    
        with zipfile.ZipFile(current_op, 'r') as zip_ref:
            zip_ref.extractall(op_dir)



def Merging_Tiles(dem_dir:str, op_dir:str):
    """Merging function.
    
        Function for merging together DEM and OP tiles into a mergedDEM.tif and
        mergedOP.tif using gdal.Translate.
        
        Parameters
        ----------
        dem_dir : str
            A directory where the DEM files will be searched
        op_dir : str
            A directory where the OP files will be searched
    """
    
    # find the xyz files 
    demlist = []
    for file in os.scandir(dem_dir):
        if file.name.endswith(".xyz"):
            demlist.append(file.path)
    
    # convert to tif 
    for xyz in demlist:
        outname = xyz.replace(".xyz","")
        gdal.Translate(outname + ".tif", xyz, outputSRS="EPSG:25832")
    
    # DEM merging
    # build virtual raster and merge
    demlist = [file.replace(".xyz", ".tif") for file in demlist]
    vrt = gdal.BuildVRT(os.path.join(dem_dir, "merged.vrt"), demlist)
    gdal.Translate(os.path.join(dem_dir, "mergedDEM.tif"), vrt, xRes = 1, yRes = -1)
    
    # OP Merging
    oplist = []
    for file in os.scandir(op_dir):
        if file.name.endswith(".tif"):
            oplist.append(file.path)
    vrt = gdal.BuildVRT(os.path.join(op_dir, "merged.vrt"), oplist)
    gdal.Translate(os.path.join(op_dir, "mergedOP.tif"), vrt, xRes = 0.2, yRes = -0.2)



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
def User_input():
    # specify a root folder
    root = os.path.join("F:", os.sep,"Marcel","Backup","Dokumente","Studium","Geoinformatik","SoSe 2021","GEO419","Abschlussaufgabe","Test")
    os.chdir(root)
    
    # specify AOI via shapefile
    shapefile = os.path.join(root,"shape.shp")
    
    # specify which DEM-type you want (dgm / dom)
    dem_format = "dgm"
    
    # specify the year from which data should be downloaded
    year = 2018
    
    # specify the file with Server-IDs for the grid tiles
    idfile = os.path.join(root,"idlist.txt")
    
    # execute the tool
    GeoportalTh_execute(root, shapefile, year, dem_format, idfile)

if __name__ == "__main__":
    User_input()