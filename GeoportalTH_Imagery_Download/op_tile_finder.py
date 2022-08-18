# -*- coding: utf-8 -*-
"""
### Browsing the server ###

# Execute this only once and only if necessary.
# Matching all OP tile names to their IDs.
@author: Marcel Felix & Friederike Metz
"""

import os
import requests
from joblib import Parallel, delayed

#request function
def OP_Tilename_Finder(request:str, index:int):
    """Request function which returns ID and filename.
    
        Requires the request url and an ID to query.
        
        Parameters
        ----------
        request : str
            The request url sent to the server
        index : int
            The current iteration index
        
        Returns
        -------
        list
            a list consisting of the current query ID and the matching tile name
    """
    r = requests.head(request) 
    try:
        text = r.headers["Content-disposition"]
    except KeyError: return
    
    filename = text.split("=")[1].replace('"', '')
    if "dop" in filename:
        return [index, filename]
    else: return

def Start_after_Interruption(n_jobs:int = 200, end:int = 300000):
    """Function for starting from the last retrieved ID.
        
        If the execution has been interrupted you can launch the process from here
        This will read the last ID in the created list and continue from there, 
        to an end that you have to set.
        
        Parameters
        ----------
        n_jobs : int, optional
            The parallelization parameter indicating the number of threads
        end : int, optional
            The last ID to request from the server
        """

    idfile = "../Data/"+"idlist.txt"    #created list
    with open(idfile, "r") as file:
        last = int(file.readlines()[-1].split(",")[0])+1 #extracts last ID and sets it +1
    if last >= end:
        return("End already reached")
    OP_Tilelist_Creator(n_jobs, last, end)
        
# main function
def OP_Tilelist_Creator(n_jobs:int, start:int, end:int):
    """Query function.
    
        Function for complete query of all tile IDs on the server within 
        a user-specified range fo IDs. Can be used to create only partial lists
        or one complete list. Note though, a complete query probably results in
        the server blocking the request at some point.
        In such case the function Start_after_Interruption is there to continue 
        from the last saved ID.
    
        Parameters
        ----------
        n_jobs : int
            The parallelization parameter indicating the number of threads
        start : int
            The query ID from which to start requesting
        end : int
            The last ID to request from the server
    """
    
    #create file in advance
    if not os.path.exists("../Data/"):
        os.mkdir("../Data/")
    idfile = "../Data/"+"idlist.txt" #location of file to be created
    if not os.path.exists(idfile):
        with open(idfile, "w") as file:
            file.write("id,year,filename\n")
    
    
    #requesting loop
    # end = start + n_jobs*2 # only for testing 
    for current in range(start, end, n_jobs): 
        with Parallel(n_jobs, require="sharedmem", prefer="threads") as parallel:
            names_list = parallel(delayed(OP_Tilename_Finder)("https://geoportal.geoportal-th.de/gaialight-th/_apps/dladownload/download.php?type=op&id="+str(index), index) for index in range(current, current+n_jobs))
                # will extend beyond "end" and secure that none are left out
        with open(idfile, "a+") as file:
            for entry in names_list:
                if entry != None:
                    year = entry[1][-8:].replace(".zip","")
                    idx_a = entry[1].find("_") + 3
                    idx_b = idx_a + 8
                    tilename = entry[1][idx_a, idx_b]
                    file.write(str(entry[0]) + "," + year + "," + tilename +"\n")
    return # names_list #for checking 

 # define standard parameters for execution as script
if __name__ == "__main__":
    # specify a root folder
    os.chdir("F:/Marcel/Backup/Dokumente/Studium/Geoinformatik/SoSe 2021/GEO419/Abschlussaufgabe/GeoportalTH_Imagery_Download/")
    
    # specify the number of threads to use for parallelization
    n_jobs = 200
    
    # specify the starting ID for the query
    start = 170000
    
    # specify the final ID for the query
    end = 300000
    
    # execute the tool
    OP_Tilelist_Creator(n_jobs, start, end)