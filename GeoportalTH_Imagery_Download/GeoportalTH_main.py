# -*- coding: utf-8 -*-
"""
### GeoportalTH_Imagery_Download ###

# Main modular executable script.
@author: Marcel Felix & Friederike Metz
"""

import os
from osgeo import gdal
import geopandas as gpd
import zipfile
import requests
import pandas as pd
import warnings


def geoportalth_execute(root: str, shapefile: str, data_choice: str,
                        year: int, id_file: str, dem_format: str = "dgm",
                        data_dir: str = None, dem_dir: str = None,
                        op_dir: str = None, keep_files: list = None,
                        out_name: str = "merged", epsg: str = "EPSG: 25832"):
    """Main execution function.

        Executes a chain of modules which identify DEM and/or OP tiles, downloads
        them and uses a shapefile to clip resulting rasters to a custom extent.
        If not specified by the user, the directories where data is stored
        will be build within a pre-designed structure based on the root dir.
        Residual files like the zip archives, unzipped files and intermediate
        processing files (vrt, tif) will be deleted by default if not stated
        otherwise. The user can further specify a name and epsg for the
        generated output.

        Parameters
        ----------
        root : str
            Root directory, usually your main working directory
        shapefile : str
            The path to the shapefile representing the AOI
        data_choice : str
            What you want to download (can be one of: dem/op/both)
        year : int
            The year from which to download DEM and OP
            Mind that not all locations have aerial imagery (OP) for all years
        id_file : str
            Path to the file containing IDs, years and tile names
        dem_format : str, optional
            The format of the DEM data (dgm / dom) (eng.: dtm / dsm)
            Is only applied if data_choice is "dem" or "both"
            The default is "dgm"
        data_dir : str, optional
            A directory where the grid folder & files will be saved
            By default it is placed within root
        dem_dir : str, optional
            A directory where the DEM files will be saved
            By default it is placed within data_dir
        op_dir : str, optional
            A directory where the OP files will be saved
            By default it is placed within data_dir
        keep_files : list,  optional
            Specify files which to keep after the process is finished
            List can contain: "zip", "unzipped", "intermediate", "all"
            Formatting examples: ["zip", "unzipped"] or ["zip", "intermediate"]
            List content will be checked for keywords; "all" overrides others
            If not specified, all files will be deleted
        out_name: str, optional
            A name for the output rasters
            dgm/dom/op and _clip will always be added to the final product name
            The default is "merged"
        epsg: str, optional
            An epsg code to specify the epsg for the output
            The default is "EPSG: 25832" naturally as the data is Thuringian
    """

    # current dev status
    if epsg != "EPSG: 25832":
        raise FunctionalityNotImplemented("Changing output's EPSG not yet possible.")
    if dem_format == "las":
        raise FunctionalityNotImplemented("Las files not yet supported.")

    # provide new paths / build structure
    if data_dir is None:
        data_dir = os.path.join(root, "data")
    if dem_dir is None:
        dem_dir = os.path.join(data_dir, "dem")
    if op_dir is None:
        op_dir = os.path.join(data_dir, "op")

    # get the grid-file url and years
    request, dem_year = dem_year_func(year)

    # automated download of tile polygons
    tile_grid_download(data_dir, dem_year, request)

    # intersection of shapefile with dem tile polygons
    tiles = intersection(data_dir, dem_year, shapefile)

    # download, merge and clip of the tiles regarding the choice of the user
    # DEM case
    if data_choice == "dem" or data_choice == "both":
        # download of dem tiles
        dem_zip_list, dem_unzip_list = dem_download(dem_dir, tiles,
                                                    dem_format, dem_year)

        # merging DEMs into one tif
        dem_intermediates = merging_tiles(dem_unzip_list, dem_dir,
                                          out_name, epsg)

        # clipping DEM using the shapefile
        clip_rasters(os.path.join(dem_dir, out_name + ".tif"),
                     os.path.join(dem_dir,
                                  out_name + "_" + dem_format + "_clip.tif"),
                     options={"cutlineDSName": shapefile,
                              "cropToCutline": True, "dstNodata": 0,
                              "dstSRS": epsg})

        # delete all residual dem files which should not be kept
        delete_list = determine_files_to_delete(dem_zip_list,
                                                dem_unzip_list,
                                                dem_intermediates,
                                                keep_files)
        delete_residual_files(delete_list)

    # OP case
    elif data_choice == "op" or data_choice == "both":
        # download of op tiles
        op_zip_list, op_unzip_list = op_download(op_dir, tiles, year, id_file)

        # merging OPs into one tif
        op_intermediates = merging_tiles(op_unzip_list, op_dir, out_name, epsg)

        # clipping OP using the shapefile
        clip_rasters(os.path.join(op_dir, out_name + ".tif"),
                     os.path.join(op_dir, out_name + "OP_clip.tif"),
                     options={"cutlineDSName": shapefile,
                              "cropToCutline": True, "dstNodata": 0,
                              "dstSRS": epsg})

        # delete all residual dem files which should not be kept
        delete_list = determine_files_to_delete(op_zip_list,
                                                op_unzip_list,
                                                op_intermediates,
                                                keep_files)
        delete_residual_files(delete_list)


def dem_year_func(year: int):
    """Determine dem_year.

        This function decides which DEM grid matches the year of interest.
        Unique OP-years thus far are:
        1997, 2008, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019,
        2020, 2021

        Parameters
        ----------
        year : int
            The year of interest
            Mind that not all locations have aerial imagery (OP) for all years

        Returns
        ----------
        request : str
            The request url to send to the server
        dem_year : str
            A string defining which DEM grid basis to use
    """

    if year <= 2013 and year >= 2010:
        request = "https://geoportal.geoportal-th.de/hoehendaten/Uebersichten/Stand_2010-2013.zip"
        dem_year = "2010-2013"
    elif year <= 2019:
        request = "https://geoportal.geoportal-th.de/hoehendaten/Uebersichten/Stand_2014-2019.zip"
        dem_year = "2014-2019"
    elif year <= 2025:
        request = "https://geoportal.geoportal-th.de/hoehendaten/Uebersichten/Stand_2020-2025.zip"
        dem_year = "2020-2025"
    else:
        raise ValueError("The provided year is not yet supported with an URL. "
                         "Current support ranges from 2010 to 2025.")

    return request, dem_year


def tile_grid_download(data_dir: str, dem_year: str, request: str):
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

    # create dir and set target
    target_dir = os.path.join(data_dir, "Stand_" + dem_year)
    if not os.path.exists(data_dir):
        os.mkdir(data_dir)
    if not os.path.exists(target_dir):
        os.mkdir(target_dir)

    grid_file = os.path.join(target_dir, "Stand_" + dem_year + ".zip")

    # download the grid if needed
    if not os.path.exists(grid_file):
        grid_polygons = requests.get(request)
        with open(grid_file, "wb") as file:
            file.write(grid_polygons.content)

    # unzip the downloaded content
    # structure of 2020-2025 is different for whatever reason
    # therefore the files need to be extracted differently
    with zipfile.ZipFile(grid_file, "r") as zip_ref:
        if dem_year == "2020-2025":
            zipped_files = zip_ref.namelist()
            for file in zipped_files[1:len(zipped_files)]:
                zip_ref.extract(file, data_dir)

        else:
            zip_ref.extractall(target_dir)


def intersection(data_dir: str, dem_year: str, shapefile: str):
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
        tiles : list[str]
            A list of DEM tile names which intersect the AOI
    """

    polygon = gpd.read_file(shapefile)
    if dem_year == "2010-2013":
        grid_path = os.path.join(
            data_dir, "Stand_2010-2013", "DGM2_2010-2013_Erfass-lt-Meta_UTM32-UTM_2014-12-10.shp")
    elif dem_year == "2014-2019":
        grid_path = os.path.join(
            data_dir, "Stand_2014-2019", "DGM1_2014-2019_Erfass-lt-Meta_UTM_2020-04-20--17127.shp")
    elif dem_year == "2020-2025":
        grid_path = os.path.join(
            data_dir, "Stand_2020-2025", "Laser_2020-2025_Erfass-lt-Meta_UTM_2022-08--17127.shp")
    else:
        raise ValueError(
            "The provided timeframe is not yet supported by this function.")
    grid = gpd.read_file(grid_path)

    tiles = []
    intersecting = gpd.overlay(polygon, grid, how="intersection")

    # if no matches were found, terminate
    if len(intersecting) == 0:
        raise AreaOutOfBounds("Your area of interest seems to not lie within "
                              "Thuringian state borders or adequate coverage "
                              "is not available for chosen DEMs.")

    # differentiate between different dataset structures
    if dem_year == "2010-2013":
        end = len(intersecting.DGM_1X1[0])
        [tiles.append(tile[2:end]) for tile in intersecting.DGM_1X1]

    else:
        [tiles.append(tile) for tile in intersecting.NAME]
    # for poly in grid.iterrows():
    #     geom = poly[1].geometry
    #     for poly2 in polygon.iterrows():
    #         geom2 = poly2[1].geometry

    #         if geom.overlaps(geom2) is True:
    #             if dem_year == "2010-2013":
    #                 end = len(poly[1].DGM_1X1)
    #                 tiles.append(poly[1].DGM_1X1[2:end])
    #             else:
    #                 tiles.append(poly[1].NAME)
    return tiles


def dem_download(dem_dir: str, tiles: list, dem_format: str, dem_year: str):
    """DEM download function.

        Takes the identified AOI tiles and creates a proper download requests
        for sending to the server.
        After the download the files are unzipped and all file paths returned
        within lists.

        Parameters
        ----------
        dem_dir : str
            A directory where the DEM files will be saved
        tiles : list[str]
            A list of DEM tile names which intersect the AOI
        dem_format : str
            The format of the DEM data (dgm / dom)
        dem_year : str
            A string defining which DEM grid basis to use

        Returns
        ----------
        dem_zip_list : list[str]
            A list containing the paths to all downloaded zip-files
        dem_unzip_list : list[str]
            A list containing the paths to all unzipped files
    """
    # list of all tiles to be downloaded in this process
    dem_zip_list = []
    # list of all unzipped files
    dem_unzip_list = []

    for tile in tiles:
        tile = str(tile)

        # create dir if necessary
        if not os.path.exists(dem_dir):
            os.mkdir(dem_dir)

        # creating request url
        # set correct url details
        if dem_year == "2010-2013":
            indicator_number = "2_"
        elif dem_year == "2014-2019" or dem_year == "2020-2025":
            indicator_number = "1_"

        # distinguish dem and op from las
        if dem_format == "dgm" or dem_format == "dom":
            request = "https://geoportal.geoportal-th.de/hoehendaten/" \
                + dem_format.upper() + "/" + dem_format + "_" + dem_year \
                + "/" + dem_format + indicator_number + tile + "_1_th_" \
                + dem_year + ".zip"
        # elif dem_format == "las":
        #     request = "https://geoportal.geoportal-th.de/hoehendaten/" \
        #         + dem_format.upper() + "/" + dem_format + "_" + dem_year \
        #         + "/" + dem_format + "_" + tile + "_1_th_" + dem_year + ".zip+"

        # download the file
        # first create clear naming pattern, as mixing up dtm and dsm is
        # possible when working with the same directory multiple times
        current_dem = os.path.join(dem_dir,
                                   tile + "_" + dem_format + dem_year + ".zip")

        # add the current dem to the corresponding list
        dem_zip_list.append(current_dem)

        # if already there, unzip and then continue with the next tile
        if os.path.exists(current_dem):
            with zipfile.ZipFile(current_dem, "r") as zip_ref:
                zip_ref.extractall(dem_dir)

                # add the unzipped files to the corresponding list
                [dem_unzip_list.append(os.path.join(dem_dir, unzip_item))
                 for unzip_item in zip_ref.namelist()]
            continue

        dem_load = requests.get(request)

        # safe to file
        with open(current_dem, "wb") as file:
            file.write(dem_load.content)

        # unzipping
        with zipfile.ZipFile(current_dem, "r") as zip_ref:
            zip_ref.extractall(dem_dir)
            # add the unzipped files to the corresponding list
            [dem_unzip_list.append(os.path.join(dem_dir, unzip_item))
             for unzip_item in zip_ref.namelist()]

    return dem_zip_list, dem_unzip_list


def op_download(op_dir: str, tiles: list, year: int, id_file: str):
    """Downloading OPs.

        This function takes the identified AOI tiles and searches the lookup
        table with IDs for the proper IDs to send download requests to the
        server. If downladed, the zip-files are unzipped and all file paths
        saved within lists.

        Parameters
        ----------
        op_dir : str
            A directory where the OP files will be saved
        tiles : list[str]
            A list of DEM tile names which intersect the AOI
        year : int
            The year from which to download the orthophotos (OP)
            Mind that not all locations have aerial imagery (OP) for all years
        id_file : str
            Path to the file containing IDs, years and tile names

        Returns
        ----------
        op_zip_list : list[str]
            A list containing the paths to all downloaded zip-files
        op_unzip_list : list[str]
            A list containing the paths to all unzipped files
    """

    # create data from lookup file
    lookup = pd.read_csv(id_file, header=0, delimiter=",")
    # unique years are: 1997, 2008, 2010, 2011, 2012, 2013, 2014,
    #                   2015, 2016, 2017, 2018, 2019, 2020, 2021
    # keep in mind that not all locations have aerial imagery for all years

    idlist = []

    # filter lookup table by year
    lookup = lookup[lookup["year"] == year]

    # tiles from 2018 and earlier are grouped in four;
    # only even numbers for x and y are registered with an ID
    # therefore we need to make sure, we catch all the tiles inbetween
    # only the lower left tile out of the four (where x&y are even) has an ID
    if year <= 2018:
        # a copy to iterate over while the original is being altered
        tiles_check = tiles.copy()
        for tile in tiles_check:
            tile_x, tile_y = tile.split("_")
            tile_x, tile_y = int(tile_x), int(tile_y)

            # if either x or y is uneven,
            # then correct their coordinates to find the matching ID
            if ((tile_x & 1) + (tile_y & 1)) == 1:
                if tile_x & 1 == 1:
                    tile_x, tile_y = str(tile_x-1), str(tile_y)
                elif tile_y & 1 == 1:
                    tile_y, tile_x = str(tile_y-1), str(tile_x)

            elif (tile_x & 1) + (tile_y & 1) == 2:  # if both are uneven
                tile_x, tile_y = str(tile_x-1), str(tile_y-1)

            else:
                continue

            # first make sure, the "wrong" tile is dropped from list
            tiles.remove(tile)
            # at last, append the newly found tile back into the list
            new_tile = tile_x + "_" + tile_y
            if new_tile not in tiles:  # but only if it's not already there!
                tiles.append(new_tile)

    # now find the matching IDs
    for tile in tiles:
        tile_match = lookup[lookup["tilename"] == tile]
        if len(tile_match) < 1:
            raise MissingAerialCoverage(
                "There are no matching ortophoto tiles in this year.")
        id_match = tile_match.values[0][0]  # first row, first column
        idlist.append(id_match)

    # list for the op-zip-files
    op_zip_list = []
    # list for unzipped files
    op_unzip_list = []

    # download the identified files
    for tile_id in idlist:
        # create dir if necessary
        if not os.path.exists(op_dir):
            os.mkdir(op_dir)

        # download the file
        current_op = os.path.join(op_dir, str(tile_id) + ".zip")
        # add the current op to the corresponding list
        op_zip_list.append(current_op)

        # if already there, unzip and then continue with the next tile
        if os.path.exists(current_op):
            with zipfile.ZipFile(current_op, "r") as zip_ref:
                zip_ref.extractall(op_dir)

                # add the unzipped files to the corresponding list
                [op_unzip_list.append(os.path.join(op_dir, unzip_item))
                 for unzip_item in zip_ref.namelist()]
            continue

        op_load = requests.get(
            "https://geoportal.geoportal-th.de/gaialight-th/_apps/dladownload/download.php?type=op&id="
            + str(tile_id))

        # safe to file
        with open(current_op, "wb") as file:
            file.write(op_load.content)

        with zipfile.ZipFile(current_op, "r") as zip_ref:
            zip_ref.extractall(op_dir)
            # add the unzipped files to the corresponding list
            [op_unzip_list.append(os.path.join(op_dir, unzip_item))
             for unzip_item in zip_ref.namelist()]

    return op_zip_list, op_unzip_list


def merging_tiles(tiles_list: list, tiles_dir: str, out_name: str = "merged",
                  epsg: str = "EPSG: 25832", meta_files: tuple = None):
    """Merging function.

        Function for merging together DEM or OP tiles into a single tif-file
        using gdals virtual format (VRT).
        An EPSG can be provided as well as the output's name.

        Parameters
        ----------
        tiles_list : list[str]
            A list which contains the names of the raster tiles to be merged
            The files need to be gdal-readable
            Can also contain adjacent metadata like meta- and tfw-files
            in such a case use the meta_files parameter to ignore them
        tiles_dir : str
            A path to where the raster tiles are located
        out_name : str, optional
            A string with the name of the output as specified by the user
            The default is "merged"
        epsg : str, optional
            An EPSG code to transform the output to
            The default is "EPSG: 25832"
        meta_files, tuple, optional
            A tuple of file extensions like "meta" which indictaes what files
            should be ignored in the tiles_list parameter
    """

    if epsg != "EPSG: 25832":
        raise FunctionalityNotImplemented(
            "Changing output's EPSG not yet possible!")

    # clean the list of adjacent metadata files
    # first establish well known file extensions used by GeoportalTh
    if meta_files is None:
        meta_files = ("tfw", "meta")
    tiles_list = [file for file in tiles_list if not file.endswith(meta_files)]

    # look into the first tile to identfy the x and y resolution to be used
    tile = gdal.Open(os.path.join(tiles_dir, tiles_list[0]))
    x = tile.GetGeoTransform()[1]
    y = tile.GetGeoTransform()[5]

    # build virtual raster with custom name and epsg
    vrt = gdal.BuildVRT(os.path.join(tiles_dir, out_name + ".vrt"),
                        tiles_list, outputSRS=epsg)
    # merge into one tif
    gdal.Translate(os.path.join(tiles_dir, out_name + ".tif"),
                   vrt, xRes=x, yRes=y)

    # add the intermediate vrt and tif file to a list
    intermediates = [os.path.join(tiles_dir, out_name + end)
                     for end in (".vrt", ".tif")]
    return intermediates


def clip_rasters(inp: str, outp: str, options: dict):
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
    del result


def determine_files_to_delete(zip_list: list, unzip_list: list,
                              intermediates: list, keep_files: list = None):
    """Sorting function

        This function takes the lists with collected paths of files which are
        intermediate to the tools process and determines, which ought to be
        deleted according to the users specification.

        Parameters
        ----------
        zip_list : list
            A list of paths to zip-files
        unzip_list : list
            A list of paths to unzipped files
        intermediates : list
            A list of intermediate vrt- and tif-files
        keep_files : list, optional
            Specify files which to keep after the process is finished
            List can contain: "zip", "unzipped", "intermediate", "all"
            Formatting examples: ["zip", "unzipped"] or ["zip", "intermediate"]
            List content will be checked for keywords; "all" overrides others
            If not specified, all files will be deleted
    """

    # final list for deleting files
    delete_list = []

    # delete none if "all" is selected
    if "all" in keep_files:
        return delete_list

    # delete all if not specified
    if keep_files is None:
        [delete_list.append(a_list)
         for a_list in (zip_list, unzip_list, intermediates)]
        return delete_list

    # add specified lists to be deleted
    if "zip" not in keep_files:
        delete_list.append(zip_list)
    if "unzipped" not in keep_files:
        delete_list.append(unzip_list)
    if "intermediate" not in keep_files:
        delete_list.append(intermediates)

    return delete_list


def delete_residual_files(*lists: list):
    """Recursive deleting function

        This function takes an unspecified number of lists or netsted lists
        that should contain paths to files, which will be unpacked recursively.

        Parameters
        ----------
        *lists : list
            An unspecified number of lists containing paths to files
            Can be nested or can be a single path to a file
    """

    # check input
    # catch recursive list/tuple item
    if len(lists) < 2:
        # differentiate between a final path of type str and another list
        if type(lists[0]) is str:
            if os.path.isfile(lists[0]):
                os.remove(lists[0])
                return
            else:  # if the path can not be found
                warnings.warn(
                    "\nA file: '%s' could not be deleted.\n"
                    "-> os.path.isfile() check failed." % lists[0])
                return

        # use recursive call, if another list is found within the current
        elif type(lists[0]) is list:
            for sub_list in lists[0]:
                delete_residual_files(sub_list)
            return

    # the tuple has length two or bigger, therefore multiple lists were given
    # use a recursive call to get into the sublists given
    if len(lists) >= 2:
        for sub_list in lists:
            delete_residual_files(sub_list)


class FunctionalityNotImplemented(Exception):
    """Missing implementation exception

        Raised when a functionality is not yet implemented in the current
        development state of this tool.
    """
    pass


class MissingAerialCoverage(Exception):
    """Exception for missing OP matches

        Usually raised when there is no aerial coverage of the Area of interest
        within the given year of interest. This can happen because older data
        from Thuringia does not offer complete coverage for all time frames.
    """
    pass


class AreaOutOfBounds(Exception):
    """Exception for missing DEM matches

        Raised when there is no DEM coverage of the Area of interest.
        This happens when the AOI is beyond the borders of Thuringia and
        therefore the GeoportalTh does not provide coverage.
    """
    pass


# when executing this as a script, specify all necessary parameters here
def user_input():
    # specify a root folder
    root = os.path.join("F:", os.sep, "Marcel", "Backup", "Dokumente",
                        "Studium", "Geoinformatik", "SoSe 2021", "GEO419",
                        "Abschlussaufgabe", "Test")
    os.chdir(root)

    # specify AOI via shapefile
    shapefile = os.path.join(root, "shape.shp")

    # specify if you want to download dem, op or both (dem/op/both)
    data_choice = "dem"

    # specify the year from which data should be downloaded
    # Unique OP-years thus far are:
    # 1997, 2008, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019,
    # 2020, 2021
    # keep in mind that not all locations have aerial imagery for all years
    year = 2010

    # specify the fie with Server-IDs for the grid tiles
    id_file = os.path.join(root, "idlist.txt")

    # specify which DEM-type you want (dgm / dom) (eng.: dtm / dsm) (optional)
    # the default is dgm
    dem_format = "dgm"

    # specify deviating directories to save your data (optional)
    # the default is None
    data_dir = None
    dem_dir = None
    op_dir = None

    # specify if zip- and unzipped-files should be kept afterwards (optional)
    # list object can contain: "zip", "unzipped", "intermediate", "all"
    # "all" overrides other entries of the list
    # the default is None
    keep_files = ["zip"]

    # specify name for the output (optional)
    # dgm/dom/op and _clip will always be added to the final product name
    out_name = "my_output_2010"

    # specify a custom epsg for the output (optional)
    # the default is "EPSG: 25832"
    epsg = "EPSG: 25832"

    # execute the tool
    geoportalth_execute(root=root, shapefile=shapefile,
                        data_choice=data_choice, year=year,
                        id_file=id_file, dem_format=dem_format,
                        data_dir=data_dir, dem_dir=dem_dir,
                        op_dir=op_dir, keep_files=keep_files,
                        out_name=out_name, epsg=epsg)


if __name__ == "__main__":
    user_input()
