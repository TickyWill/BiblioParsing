"""Module of main functions for parsing rawdata.
"""

__all__ = ['biblio_parser',
           'merge_database',
           ]

# Standard libraries import
import os
from pathlib import Path

# 3rd party library imports
import pandas as pd

# Local library imports
import BiblioParsing.parsing_globals as bp_pg
from BiblioParsing.scopus_parsing import scopus_parser
from BiblioParsing.scopus_rawdata_utils import read_scopus_rawdata
from BiblioParsing.wos_parsing import wos_parser
from BiblioParsing.wos_rawdata_utils import read_wos_rawdata


def merge_database(database, filename, in_dir, out_dir):
    """Merges several corpus of same database type in one corpus.

    Args:
        database (str): database type (scopus or wos).
        filename (str): name of the merged database.
        in_dir (str): name of the folder where the corpuses are saved.
        out_dir (str): name of the folder where the merged corpuses will be saved.
    """
    rawdata_paths_list = []
    rawdata_list = []
    if database==bp_pg.WOS:
        for path, _, files in os.walk(in_dir):
            rawdata_paths_list.extend(Path(path) / Path(file) for file in files
                                      if file.endswith(".txt"))
        for file_path in rawdata_paths_list:
            rawdata_list.append(read_wos_rawdata(file_path)[0])

    elif database==bp_pg.SCOPUS:
        for path, _, files in os.walk(in_dir):
            rawdata_paths_list.extend(Path(path) / Path(file) for file in files
                                      if file.endswith(".csv"))
        for file_path in rawdata_paths_list:
            rawdata_list.append(read_scopus_rawdata(file_path)[0])
    else:
        print(f"WARNING: Sorry, unrecognized database {database}: "
              f"it should be {bp_pg.WOS} or {bp_pg.SCOPUS}")

    result = pd.concat(rawdata_list, ignore_index=True)
    result.to_csv(out_dir / Path(filename), sep='\t')


def biblio_parser(rawdata_path, database, affil_filter_list=None, affil_params_dic=None):
    """Parses corpus rawdata using the appropriate parser.

    Two parsers are available:
    - `wos_parser` function imported from `wos_parsing` module;
    - `scopus_parser` function imported from `scopus_parsing` module.

    Args:
        rawdata_path (path): The full path to the corpus rawdata.
        database (str): The type of the rawdata among Scopus or WoS.
        affil_filter_list (list): The affiliations-filter composed of a list of \
        normalized affiliations (str), optional (default=None).
        affil_params_dic (dict): Optional dict (default=None) keyed by \
        ['affil_types_file_path', 'country_affils_file_path', 'country_towns_folder_path', \
        'country_towns_file'] and valued by the user as the full path to the data per country \
        of raw affiliations per normalized one, the full path to the data of affiliations-types \
        used to normalize the affiliations, the name of the file of the data of towns per country \
        and the full path to the folder where these data are available.
    Returns:
        (tup): The tuple of parsing results returned by the used appropriate parser.
    """
    parsing_tup = ()
    if database==bp_pg.WOS:
        parsing_tup = wos_parser(rawdata_path, affil_filter_list=affil_filter_list,
                                 affil_params_dic=affil_params_dic)
    elif database==bp_pg.SCOPUS:
        parsing_tup = scopus_parser(rawdata_path, affil_filter_list=affil_filter_list,
                                    affil_params_dic=affil_params_dic)
    else:
        print(f"WARNING: Sorry, unrecognized database {database}: it should be {bp_pg.WOS} or {bp_pg.SCOPUS}")

    return parsing_tup
