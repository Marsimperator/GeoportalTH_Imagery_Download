# ID Files

## Explaination

These files are the execution result of "op_tile_finder's" "op_tilelist_creator" function.
They contain the retrieved Orthophoto-server-IDs and their corresponding years and grid-tile-IDs.
Only with one of these files is it possible to download OP-tiles with the geothimagery tool successfully. Executing without corresponding Server-IDs will raise a MissingAerialCoverage-Exception.

## How to create a file?

Use the "op_tilelist_creator" function to create your own file.
Execution time is around 3.5 to 4 hours (empirically). This may differ depending on the specs of your machine or unpredictable behaviour on behalf of the server.

The IDs have been found to lie between 170000 and 300000 (2022).

If you want to just update an existing file after e.g. one year, you can use the "start_after_interruption" function.

## Why is there more than one file?

During the second query it was discovered that some server-IDs were either deprecated or possibly in maintenance as the newer file is missing IDs the first one included. Likewise the newer file includes files from 2021 which were not yet available during the first query.
