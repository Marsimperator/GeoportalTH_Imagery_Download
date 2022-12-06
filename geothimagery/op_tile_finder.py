# -*- coding: utf-8 -*-
"""
### Browsing the server ###

# Execute this only once when necessary.
# Matching all OP tile names to their IDs.
@author: Marcel Felix & Friederike Metz
"""

import os
import requests
# import pandas as pd
from joblib import Parallel, delayed


# request function
def op_tilename_finder(request: str, index: int):
    """Request function

        This function returns ID and filename as a list.
        Requires the request url and the corresponding ID.

        Parameters
        ----------
        request : str
            The request url which is sent to the server
        index : int
            The current ID in iteration

        Returns
        -------
        : list
            a list consisting of the current query ID and matching tile name
    """
    r = requests.head(request)
    try:
        text = r.headers["Content-disposition"]
    except KeyError:
        return

    filename = text.split("=")[1].replace('"', '')
    if "dop" in filename:
        return [index, filename]
    else:
        return


def start_after_interruption(id_file: str, n_jobs: int, end: int,
                             safe_step: int = 200):
    """Function for starting from the last retrieved ID.

        If the execution of op_tilelist_creator has been interrupted, launch
        the process from here. This will read the last ID in the created list
        and continue from there. Make sure the id_file is the correct one.

        Parameters
        ----------
        id_file : str
            Path to the file containing IDs, years and tile names
        n_jobs : int
            The parallelization parameter indicating the number of processes
            or threads to be used
            Empirical testing has shown that >8< is reasonably fast
            For further info read into Joblib's documentation
        end : int
            The last ID to request from the server
        safe_step : int, optional
            An ID-related interval at which the retrieved IDs will be saved
            into the specified id_file
        """

    with open(id_file, "r") as file:
        # extracts last ID and sets it +1
        last = int(file.readlines()[-1].split(",")[0])+1
    if last >= end:
        return("Final ID has already been reached.")

    # extract variables for function call
    root = os.path.dirname(id_file)
    id_file = os.path.basename(id_file)
    op_tilelist_creator(root, id_file, n_jobs, last, end, safe_step=safe_step)


# main function
def op_tilelist_creator(root: str, id_file: str, n_jobs: int, start: int,
                        end: int, safe_step: int = 200):
    """Query function.

        Function for complete query of all tile IDs on the server within
        a user-specified range fo IDs. Can be used to create only partial lists
        or one complete list. Note though, a complete query can cause the
        server to block requests.
        In case of any interruption the function start_after_interruption is
        there to continue the query from the last saved ID.

        Parameters
        ----------
        root : str
            dgsgsdgfdhfd
        id_file : str
            Name to the file where IDs, years and tile names will be saved
        n_jobs : int
            The parallelization parameter indicating the number of processes
            or threads to be used
            Empirical testing has shown that >8< is reasonably fast
            For further info read into Joblib's documentation
        start : int
            The query ID from which to start the requests
        end : int
            The last ID to request from the server
        safe_step : int, optional
            An ID-related interval at which the retrieved IDs will be saved
            into the specified id_file
    """

    # reassign the id_file variable a complete path
    id_file = os.path.join(root, id_file)

    # create directory in advance if not existant
    if not os.path.exists(root):
        os.mkdir(root)
    # create file in advance if not existant
    if not os.path.exists(id_file):
        with open(id_file, "w") as file:
            file.write("id,year,tilename\n")

    # parallelized requesting loop returning a nested list
    for current in range(start, end+1, safe_step):
        names_list = Parallel(n_jobs)(delayed(op_tilename_finder)(
            "https://geoportal.geoportal-th.de/gaialight-th/_apps/dladownload/download.php?type=op&id=" + str(index),
            index) for index in range(current, current + safe_step))

        # safe those retrieved IDs to file
        with open(id_file, "a+") as file:
            for entry in names_list:
                if entry is not None:
                    year = entry[1][-8:].replace(".zip", "")
                    subentry = entry[1].split("_32")[1]
                    if subentry.startswith("_"):
                        idx_a, idx_b = 1, 9
                    else:
                        idx_a, idx_b = 0, 8
                    tilename = subentry[idx_a:idx_b]
                    file.write(str(entry[0]) + "," +
                               year + "," + tilename + "\n")


# when executing this as a script, specify all necessary parameters here
def user_input():
    # specify a root folder
    root = os.path.join("F:", os.sep, "Marcel", "Backup", "Dokumente",
                        "Studium", "Geoinformatik", "SoSe 2021", "GEO419",
                        "Abschlussaufgabe", "Test", "idlisttest")

    # specify the file where IDs are to be saved
    id_file = "newlist.txt"

    # specify the number of threads to use for parallelization
    # 8 is shown to deliver reasonably fast resuts
    n_jobs = 8

    # specify the starting ID for the query
    start = 170000

    # specify the final ID for the query
    end = 300000

    # specify an interval at which the retrieved IDs will be saved to file
    safe_step = 200

    # execute the tool
    op_tilelist_creator(root=root, id_file=id_file, n_jobs=n_jobs, start=start,
                        end=end, safe_step=safe_step)


# define standard parameters for execution as script
if __name__ == "__main__":
    user_input()
