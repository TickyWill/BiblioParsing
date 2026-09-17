"""Module of useful functions for parsings demonstration.

This covers:
    - Configuration setting such as working folder architecture;
    - Reading parsings results;
    - Saving parsings results;
    - Setting paths to user's files to use for authors' affiliations parsing.
"""

__all__ = ['read_demo_parsing_dict',
           'save_demo_db_ids_data',
           'save_demo_fails_dict',
           'save_demo_parsing_dict',
           'save_demo_parsing_dicts',
           'set_demo_step_affil_parsing_paths',
           'set_demo_user_config',
          ]


# Standard library imports
import json
import os
from pathlib import Path

# 3rd party imports
import pandas as pd

# Local library imports
import bpfuncts.parsing_globals as bp_pg


def _get_demo_config():
    """Builds the demonstration configuration given in the 'BiblioParsing_config.json' 
    JSON file located in the 'bpfuncts/DemoConfig' folder.

    The JSON file gives:
        - The default architecture of a working folder for using the package;
        - The default parsings results file names.

    Returns:
        (dict): The dict built from the JSON file.
    Note:
        Open the JSON file in a text editor to read the description.
    """
    config_json_file_name = 'BiblioParsing_config.json'

    # Reads the default JSON config file
    pck_config_file_path = Path(__file__).parent / Path('DemoConfig') / Path(config_json_file_name)
    with open(pck_config_file_path, encoding='utf-8') as file:
        config_dict = json.load(file)
    return config_dict


def _build_demo_effective_config(parsing_folder_dict, db_list):
    """Builds the full working-folder architecture taking into account 
    the specified list of databases to parse.

    Args:
        parsing_folder_dict (dict): The working-folder architecture.
        db_list (list): The list of databases to parse.
    Returns:
        (dict): The full working-folder architecture.
    """
    rawdata_folder_name = parsing_folder_dict['corpus']['database']['rawdata']
    parsing_folder_name = parsing_folder_dict['corpus']['database']['parsing']
    parsing_folder_dict_init = parsing_folder_dict

    parsing_folder_dict = {'folder_root': parsing_folder_dict_init['folder_root'],
                           'corpus': {'corpus_root': parsing_folder_dict_init['corpus']['corpus_root'],
                                     'concat'     : parsing_folder_dict_init['corpus']['concat'],
                                     'dedup'      : parsing_folder_dict_init['corpus']['dedup'],
                                     'databases'  : {},
                                     },
                          }

    for db_num, db_label in enumerate(db_list):
        parsing_folder_dict['corpus']['databases'][str(db_num)]= {'root': db_label,
                                                                  'rawdata': rawdata_folder_name,
                                                                  'parsing': parsing_folder_name
                                                                 }
    return parsing_folder_dict


def _create_folder(parsing_folder_dict, keys_list, sub_root_path):
    """Creates a folder in the specified path using a list of keys for walking down 
    the working-folder architecture to the folder-name to be used.

    Args:
        parsing_folder_dict (dict): The full working-folder architecture.
        keys_list (list): Sequence of keys for working down the architecture \
        to the folder to create.
        sub_root_path (path): The full path to the top-folder where the folder \
        to create will be located.
    Returns:
        (tup): Composed of the full path to the created folder \
        and of the created folder-name.
    """
    # Initializing the architecture
    key_dict = parsing_folder_dict

    # Walking down recursively the architecture for setting the folder name
    for key in keys_list:
        key_dict = key_dict[key]
    folder_name = key_dict

    # Setting the folder path
    folder_path = sub_root_path / Path(folder_name)

    # Creating the folder if it doesn't exist
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    return folder_path, folder_name


def _create_year_demo_folders(year, parsing_folder_dict, root_path, db_list):
    """Creates the demonstration folders in the root path for a corpus year 
    and for the specified list of databases to parse.

    It returns the data of the full paths to the created folders according 
    to the specified architecture of the working folder.

    Args:
        year (str): The '4-digits' corpus year.
        parsing_folder_dict (dict): The architecture of the working folder.
        root_path (path): The full path to folder where the created folders \
        will be located according to the specified architecture of the working folder.
        db_list (list): The list of databases to parse.
    Returns:
        (tup): Composed of the full paths (dict) to the rawdata folders for each database \
        and of the full paths (dict) to the parsing-results folders.
    """

    # Updating 'parsing_folder_dict' using the list of databases 'db_list'
    parsing_folder_dict = _build_demo_effective_config(parsing_folder_dict, db_list)

    # Creating the working folder if not available
    keys_list = ['folder_root']
    wf_path, _ = _create_folder(parsing_folder_dict, keys_list, root_path)

    # Creating the year folder if not available
    year_files_path = wf_path / Path(str(year))
    if not os.path.exists(year_files_path):
        os.makedirs(year_files_path)

    # Creating the corpuses folder if not available
    keys_list = ['corpus', 'corpus_root']
    corpus_folder_path,_ = _create_folder(parsing_folder_dict, keys_list, year_files_path)

    rawdata_path_dict = {}
    parsing_path_dict = {}
    # Creating the databases folders if not available
    for db_num in parsing_folder_dict['corpus']['databases'].keys():

        keys_list = ['corpus', 'databases', db_num, 'root']
        db_root_path, db_root_name = _create_folder(parsing_folder_dict, keys_list, corpus_folder_path)

        keys_list = ['corpus', 'databases', db_num, 'rawdata']
        db_rawdata_path, _ = _create_folder(parsing_folder_dict, keys_list, db_root_path)
        rawdata_path_dict[db_root_name] = db_rawdata_path

        keys_list = ['corpus', 'databases', db_num, 'parsing']
        db_parsing_path, _ = _create_folder(parsing_folder_dict, keys_list, db_root_path)
        parsing_path_dict[db_root_name] = db_parsing_path

    # Creating the concatenation folder if not available
    keys_list = ['corpus', 'concat', 'root']
    concat_root_path, _ = _create_folder(parsing_folder_dict, keys_list, corpus_folder_path)

    keys_list = ['corpus', 'concat', 'parsing']
    concat_parsing_path, _ = _create_folder(parsing_folder_dict, keys_list, concat_root_path)
    parsing_path_dict['concat'] = concat_parsing_path

    # Creating the deduplication folder if not available
    keys_list = ['corpus', 'dedup', 'root']
    dedup_root_path, _ = _create_folder(parsing_folder_dict, keys_list, corpus_folder_path)

    keys_list = ['corpus', 'dedup', 'parsing']
    dedup_parsing_path, _ = _create_folder(parsing_folder_dict, keys_list, dedup_root_path)
    parsing_path_dict['dedup'] = dedup_parsing_path

    return rawdata_path_dict, parsing_path_dict


def set_demo_user_config(year=None, db_list=None):
    """Sets the user's configuration for a demonstration of the parsing 
    process .

    If the corpus year and the list of databases to parse are specified, 
    the function creates the working-folder architecture in the user's home path.

    Args:
        year (str) : Optional '4-digits' corpus year (default: None).
        db_list (list): Optional list of databases to parse (default: None).
    """
    # default values :
    rawdata_path_dict, parsing_path_dict, item_filename_dict = None, None, None

    # Getting the configuration dict
    config_dict = _get_demo_config()

    # Getting the working folder architecture base
    parsing_folder_dict = config_dict['PARSING_FOLDER_ARCHI']

    # Setting the working folder name
    wf_name = parsing_folder_dict['folder_root']

    # Getting the user's root path
    root_path = Path.home()

    # Setting the working folder path
    wf_path = root_path / Path(wf_name)

    if year and db_list:
        # Building the working folder architecture for a corpus single year "year"
        # and getting useful paths
        rawdata_path_dict, parsing_path_dict = _create_year_demo_folders(year, parsing_folder_dict,
                                                                         root_path, db_list)

    # Getting the filenames for each parsing item
    item_filename_dict = config_dict['PARSING_FILE_NAMES']

    return wf_path, rawdata_path_dict, parsing_path_dict, item_filename_dict


def save_demo_parsing_dict(parsing_dict, parsing_path,
                           item_filename_dict, save_extent):
    """Saves the parsing results of a single step of the parsing step.

    Args:
        parsing_dict (dict): Parsing results keyed by parsing items given \
        by the 'PARSING_ITEMS_LIST' global imported from the `bpfuncts.parsing_globals` \
        module and valued by the dataframes of parsing results.
        parsing_path (path): Full path to the folder where the parsing \
        results are to be saved.
        item_filename_dict (dict): Dict keyed by the parsing items and valued \
        by the file names of the parsing results.
        save_extent (str): File type given by file extension without the dot separator \
        (ex: "xlsx" for EXCEL file type).
    Returns:
        (str): End message.
    """
    # Cycling on parsing items
    for item in bp_pg.PARSING_ITEMS_LIST:
        if item in parsing_dict.keys():
            item_df = parsing_dict[item]
            if save_extent=="xlsx":
                item_xlsx_file = item_filename_dict[item] + ".xlsx"
                item_xlsx_path = parsing_path / Path(item_xlsx_file)
                item_df.to_excel(item_xlsx_path, index = False)
            elif save_extent=="dat":
                item_tsv_file = item_filename_dict[item] + ".dat"
                item_tsv_path = parsing_path / Path(item_tsv_file)
                item_df.to_csv(item_tsv_path, index=False, sep='\t')
            else:
                item_tsv_file = item_filename_dict[item] + ".csv"
                item_tsv_path = parsing_path / Path(item_tsv_file)
                item_df.to_csv(item_tsv_path, index=False, sep=',')
    message = f"All parsing results saved as {save_extent} files"
    return message


def save_demo_fails_dict(fails_dict, parsing_path):
    """Saves parsing performance indicators in a JSON file.

    Args:
        fails_dict (dict): The dict of parsing fails.
        parsing_path (path): The full path of the parsing results folder 
        where the JSON file is being saved.
    Returns:
        (str): End message.
    """
    perf_json_file_name = "Parsing_perf.json"
    perf_json_path = parsing_path / Path(perf_json_file_name)
    with open(perf_json_path, 'w', encoding='utf-8') as write_json:
        json.dump(fails_dict, write_json, indent=4)
    message = "Parsing-performance indicators saved as json file"
    return message


def save_demo_db_ids_data(db_ids_df, parsing_path, database):
    """The function `save_demo_db_ids_data` saves database-IDs data in as XLSX file.

    Args:
        db_ids_df (dataframe): The database IDs data.
        parsing_path (path): The full path of the parsing results folder \
        for saving the XLSX file.
        database (str): The database name.
    Returns:
        (str): End message.
    """
    file_name = database.capitalize() + "_IDs.xlsx"
    file_path = parsing_path / Path(file_name)

    db_ids_df.to_excel(file_path, index=False)

    message = "Database-IDs data saved as xlsx file"
    return message


def save_demo_parsing_dicts(parsing_dicts_dict, parsing_path_dict, item_filename_dict,
                       save_extent, fails_dicts, ids_dfs_dict):
    """Saves the parsing results of all the steps of the parsing process 
    from rawdata parsing to parsings deduplication.

    Args:
        parsing_dicts_dict (dict): Parsing results keyed by parsing step name \
        and valued by steps parsing results (dict) keyed by parsing items given \
        by the 'PARSING_ITEMS_LIST' global imported from the `bpfuncts.parsing_globals` \
        module and valued by the dataframes of parsing results.
        parsing_path_dict (dict): Dict keyed by parsing step name and valued by full path \
        to the folder where the parsing results are to be saved.
        item_filename_dict (dict): Dict keyed by the parsing items and valued \
        by the file names of the parsing results.
        save_extent (str): File type given by file extension without the dot separator \
        (ex: "xlsx" for EXCEL file type).
        fails_dicts (dict): Dict keyed by parsing step name and valued by the dict of parsing fails.
        ids_dfs_dict (dict): Dict keyed by parsing step name and valued by the database IDs data.
    Returns:
        (str): End message.
    """
    fails_save_status = False
    db_ids_save_status = False

    for parsing_name, parsing_dict in parsing_dicts_dict.items():
        parsing_path = parsing_path_dict[parsing_name]
        _ = save_demo_parsing_dict(parsing_dict, parsing_path, item_filename_dict, save_extent)

        if parsing_name in fails_dicts.keys():
            parsing_fails_dict = fails_dicts[parsing_name]
            _ = save_demo_fails_dict(parsing_fails_dict, parsing_path)
            fails_save_status = True

        if parsing_name in ids_dfs_dict.keys():
            db_ids_df = ids_dfs_dict[parsing_name]
            _ = save_demo_db_ids_data(db_ids_df, parsing_path, parsing_name)
            db_ids_save_status = True

    message = f"All parsing-to-deduplication results saved as files with .{save_extent} extension."
    if fails_save_status:
        message += "\n All parsing-fails results saved as JSON files."
    if db_ids_save_status:
        message += "\n All database-IDs data saved as XLSX files."

    return message


def set_demo_step_affil_parsing_paths(user_rep_utils, user_affil_files_dic,
                                      rawdata_parsing_step=False, verbose=False):
    """Builds a dict setting the full paths and useful name of user's files 
    to use for authors' affiliations parsing.

    The keys of the built dict are:
        - 'country_towns_file';
        - 'affil_types_file_path';
        - 'country_affils_file_path';
        - 'country_towns_folder_path'.

    The values of the built dict are:
        - The file name of the data file of towns per country.
        - The full path to the affiliations-types data file;
        - The full path to the data file of raw affiliations per normalized affiliation and per country;
        - The full path to the folder where all the data files are located including the data file of towns per country.

    Args:
        user_rep_utils (path): The full path to the folder where the user's files are located
        user_affil_files_dic (dict): The dict keyed by ['country_towns_file', 'affil_types_file', \
        'country_affils_file', 'institute_affils_file'] and valued by the file names for the following data: \
        towns per country, affiliations types, raw affiliations per normalized affiliation and per country and \
        also this last kind of data for the normalization of the Institute's affiliations.
        rawdata_parsing_step (bool): True use the key 'institute_affils_file' in the 'user_affil_files_dic' dict\
        rather than the key 'country_affils_file' for setting the value at key 'country_affils_file_path' \
        for the built dict (default: False).
        verbose (bool): True allows control prints (default: False).
    Returns:
        (dict): The built dict.
    """
    # Setting the filename for the affiliations-per-country data for parsings deduplication step
    parsing_step_norm_affil_file = user_affil_files_dic['country_affils_file']
    if rawdata_parsing_step:
        # Setting the filename for the affiliations-per-country data for parsing rawdata step
        parsing_step_norm_affil_file = user_affil_files_dic['institute_affils_file']

    # Setting user's affiliations-parsing paths
    affil_types_file_path = user_rep_utils / Path(user_affil_files_dic['affil_types_file'])
    country_affils_file_path = user_rep_utils / Path(parsing_step_norm_affil_file)
    user_affil_params_dic = {'country_towns_file'       : user_affil_files_dic['country_towns_file'],
                             'affil_types_file_path'    : affil_types_file_path,
                             'country_affils_file_path' : country_affils_file_path,
                             'country_towns_folder_path': user_rep_utils,
                            }

    if verbose:
        print(f"User's affiliations-parsing files set for rawdata-parsing-step '{rawdata_parsing_step}' as:\n"
              f"\n  - affil_types_file    : {user_affil_files_dic['affil_types_file']}"
              f"\n  - country_affils_file : {parsing_step_norm_affil_file}"
              f"\n  - country_towns_file  : {user_affil_files_dic['country_towns_file']}\n"
              f"\navailable at: {user_rep_utils}\n")
    return user_affil_params_dic


def read_demo_parsing_dict(parsing_path, item_filename_dict, read_extent):
    """Reads the dataframes of the parsing results from files of a specified type.

    Args:
        parsing_path (path): Full path to the folder where the parsing \
        results are located.
        item_filename_dict (dict): Dict keyed by the parsing items and valued \
        by the file names of the parsing results.
        read_extent (str): File type given by file extension without the dot separator \
        (ex: "xlsx" for Excel file type).
    Returns:
        (dict): Parsing results keyed by parsing items \
        given by 'PARSING_ITEMS_LIST' global imported from \
        the `bpfuncts.parsing_globals` module and valued by the dataframes \
        of parsing results.
    """
    parsing_dict = {}
    # Cycling on parsing items
    for item in bp_pg.PARSING_ITEMS_LIST:
        item_df = None
        if read_extent=="xlsx":
            item_xlsx_file = item_filename_dict[item] + ".xlsx"
            item_xlsx_path = parsing_path / Path(item_xlsx_file)
            if item_xlsx_path.is_file():
                try:
                    item_df = pd.read_excel(item_xlsx_path)
                except pd.errors.EmptyDataError:
                    item_df = pd.DataFrame()
        elif read_extent=="dat":
            item_tsv_file = item_filename_dict[item] + ".dat"
            item_tsv_path = parsing_path / Path(item_tsv_file)
            if item_tsv_path.is_file():
                try:
                    item_df = pd.read_csv(item_tsv_path, sep = "\t")
                except pd.errors.EmptyDataError:
                    item_df = pd.DataFrame()

        if item_df is not None:
            parsing_dict[item] = item_df
    return parsing_dict
