'''
'''
__all__ = ['set_user_config',
           'parse_to_dedup',
           'save_db_ids_data',
           'save_fails_dict',
           'save_parsing_dict',
           'save_parsing_dicts',
          ]


def _get_demo_config():
    # Standard library imports
    import json
    from pathlib import Path

    config_json_file_name = 'BiblioParsing_config.json'

    # Reads the default json_file_name config file
    pck_config_file_path = Path(__file__).parent / Path('DemoConfig') / Path(config_json_file_name)
    with open(pck_config_file_path) as file:
        config_dict = json.load(file)

    return config_dict


def _build_effective_config(parsing_folder_dict, db_list):
    parsing_folder_dict_init = parsing_folder_dict
    parsing_folder_dict = {}
    parsing_folder_dict['folder_root'] = parsing_folder_dict_init['folder_root']
    parsing_folder_dict['corpus'] = {}
    parsing_folder_dict['corpus']['corpus_root'] = parsing_folder_dict_init['corpus']['corpus_root']
    parsing_folder_dict['corpus']['concat'] = parsing_folder_dict_init['corpus']['concat']
    parsing_folder_dict['corpus']['dedup'] = parsing_folder_dict_init['corpus']['dedup']
    parsing_folder_dict['corpus']['databases'] = {}
    for db_num, db_label in enumerate(db_list):
        parsing_folder_dict['corpus']['databases'][str(db_num)]= {}
        parsing_folder_dict['corpus']['databases'][str(db_num)]['root'] = db_label
        rawdata_folder_name = parsing_folder_dict_init['corpus']['database']['rawdata']
        parsing_folder_dict['corpus']['databases'][str(db_num)]['rawdata'] = rawdata_folder_name
        parsing_folder_name = parsing_folder_dict_init['corpus']['database']['parsing']
        parsing_folder_dict['corpus']['databases'][str(db_num)]['parsing'] = parsing_folder_name    

    return parsing_folder_dict


def _build_files_paths(year, parsing_folder_dict, root_path, db_list):
    
    # Standard library imports
    import os
    from pathlib import Path
    
    # Internal functions
    def _create_folder(parsing_folder_dict, keys_list, folder_root):
        key_dict = parsing_folder_dict
        for key in keys_list: key_dict = key_dict[key]
        folder_name = key_dict
        folder_path = folder_root / Path(folder_name)
        if not os.path.exists(folder_path): os.makedirs(folder_path)
        return (folder_path, folder_name)
    
    # Updating 'parsing_folder_dict' using the list of databases 'db_list'
    parsing_folder_dict = _build_effective_config(parsing_folder_dict, db_list)
    
    # Creating the 'Biblioparsing_files' if not available
    keys_list = ['folder_root']
    BiblioParsing_files_path,_ = _create_folder(parsing_folder_dict, keys_list, root_path)
    
    # Creating the year folder if not available
    year_files_path = BiblioParsing_files_path / Path(str(year))
    if not os.path.exists(year_files_path): os.makedirs(year_files_path)
    
    # Creating the corpuses folder if not available
    keys_list = ['corpus', 'corpus_root']
    corpus_folder_path,_ = _create_folder(parsing_folder_dict, keys_list, year_files_path)
    
    rawdata_path_dict = {}
    parsing_path_dict = {}
    # Creating the databases folders if not available
    for db_num in list(parsing_folder_dict['corpus']['databases'].keys()):          
        
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
    concat_root_path, concat_root_name = _create_folder(parsing_folder_dict, keys_list, corpus_folder_path)
    
    keys_list = ['corpus', 'concat', 'parsing']
    concat_parsing_path, _ = _create_folder(parsing_folder_dict, keys_list, concat_root_path)
    parsing_path_dict['concat'] = concat_parsing_path   
    
    # Creating the deduplication folder if not available
    keys_list = ['corpus', 'dedup', 'root']
    dedup_root_path, dedup_root_name = _create_folder(parsing_folder_dict, keys_list, corpus_folder_path)
    
    keys_list = ['corpus', 'dedup', 'parsing']
    dedup_parsing_path, _ = _create_folder(parsing_folder_dict, keys_list, dedup_root_path)
    parsing_path_dict['dedup'] = dedup_parsing_path       
    
    return (rawdata_path_dict, parsing_path_dict)


def set_user_config(year = None, db_list = None):
    '''
    '''
    
    # Standard library imports
    from pathlib import Path
    
    # default values :
    rawdata_path_dict, parsing_path_dict, item_filename_dict = None, None, None
    
    # Getting the configuration dict
    config_dict = _get_demo_config()
    
    # Getting the working folder architecture base
    parsing_folder_dict = config_dict['PARSING_FOLDER_ARCHI']
    
    # Setting the working folder name 
    working_folder_name = parsing_folder_dict['folder_root']
    
    # Getting the user's root path
    root_path = Path.home()
    
    # Setting the working folder path
    working_folder_path = root_path / Path(working_folder_name)
    
    if year and db_list:

        # Building the working folder architecture for a corpus single year "year" and getting useful paths
        rawdata_path_dict, parsing_path_dict = _build_files_paths(year, parsing_folder_dict, root_path, db_list)
        
    # Getting the filenames for each parsing item
    item_filename_dict = config_dict['PARSING_FILE_NAMES']    

    return (working_folder_path, rawdata_path_dict, parsing_path_dict, item_filename_dict)


def parse_to_dedup(year, db_raw_dict, 
                   user_inst_filter_list, 
                   user_norm_inst_status,
                   user_istitute_affiliations_file_path,
                   user_inst_types_file_path,
                   user_country_affiliations_file_path,
                   user_country_towns_file,
                   user_country_towns_folder_path,
                   verbose = False):
    """
    """
    
    # Local library imports
    from BiblioParsing.BiblioParsingUtils import biblio_parser
    from BiblioParsing.BiblioParsingUtils import set_rawdata_error
    from BiblioParsing.BiblioParsingConcat import concatenate_parsing
    from BiblioParsing.BiblioParsingConcat import deduplicate_parsing
    
    # Globals imports
    #from BiblioParsing.BiblioSpecificGlobals import INST_FILTER_LIST
    from BiblioParsing.BiblioSpecificGlobals import SCOPUS
    from BiblioParsing.BiblioSpecificGlobals import SCOPUS_RAWDATA_EXTENT
    from BiblioParsing.BiblioSpecificGlobals import WOS
    from BiblioParsing.BiblioSpecificGlobals import WOS_RAWDATA_EXTENT
    
    # Parsing Scopus rawdata
    scopus_raw_path = db_raw_dict[SCOPUS]
    return_tup = biblio_parser(scopus_raw_path, SCOPUS, inst_filter_list = user_inst_filter_list,
                               country_affiliations_file_path = user_istitute_affiliations_file_path,
                               inst_types_file_path = user_inst_types_file_path,
                               country_towns_file = user_country_towns_file,
                               country_towns_folder_path = user_country_towns_folder_path)
    scopus_parsing_dict, scopus_fails_dict, scopus_ids_df = return_tup[0:3]
        
    # Parsing WoS rawdata 
    wos_raw_path = db_raw_dict[WOS]
    return_tup = biblio_parser(wos_raw_path, WOS, inst_filter_list = user_inst_filter_list,
                               country_affiliations_file_path = user_istitute_affiliations_file_path,
                               inst_types_file_path = user_inst_types_file_path,
                               country_towns_file = user_country_towns_file,
                               country_towns_folder_path = user_country_towns_folder_path)
    wos_parsing_dict, wos_fails_dict, wos_ids_df = return_tup[0:3]
    
    # Initializing results dicts
    parsing_dicts_dict = {} 
    fails_dicts = {}
    ids_dfs_dict = {}
    
    
    if scopus_parsing_dict and wos_parsing_dict:
        
        # Concatenating the two parsings
        concat_parsing_dict = concatenate_parsing(scopus_parsing_dict, wos_parsing_dict,  
                                                  inst_filter_list = user_inst_filter_list)

        # Deduplicating the concatenation of the two parsings
        dedup_parsing_dict = deduplicate_parsing(concat_parsing_dict, 
                                                 norm_inst_status = user_norm_inst_status,
                                                 inst_types_file_path = user_inst_types_file_path,
                                                 country_affiliations_file_path = user_country_affiliations_file_path,
                                                 country_towns_file = user_country_towns_file,
                                                 country_towns_folder_path = user_country_towns_folder_path)

        # Building parsing performances dict
        fails_dicts[SCOPUS] = scopus_fails_dict
        fails_dicts[WOS] = wos_fails_dict

        # Building databases-IDs dict
        ids_dfs_dict[SCOPUS] = scopus_ids_df
        ids_dfs_dict[WOS] = wos_ids_df

        # Building results dict        
        parsing_dicts_dict[SCOPUS] = scopus_parsing_dict
        parsing_dicts_dict[WOS] = wos_parsing_dict
        parsing_dicts_dict["concat"] = concat_parsing_dict
        parsing_dicts_dict["dedup"] = dedup_parsing_dict
        if verbose:
            print(f'\nScopus corpus successfully parsed in parsing_dicts_dict[{SCOPUS}]')
            print(f'\nWos corpus successfully parsed in parsing_dicts_dict[{WOS}]')
            print('\nParsings successfully concatenated in parsing_dicts_dict["concat"]')
            print('\nParsings successfully deduplicated in parsing_dicts_dict["dedup"]')
        
    else:
        if not scopus_parsing_dict:
            print(set_rawdata_error(SCOPUS, scopus_raw_path, SCOPUS_RAWDATA_EXTENT))
        if not wos_parsing_dict:
            print(set_rawdata_error(WOS, wos_raw_path, WOS_RAWDATA_EXTENT))
    
    return parsing_dicts_dict, fails_dicts, ids_dfs_dict


def save_parsing_dict(parsing_dict, parsing_path, 
                      item_filename_dict, save_extent):
    """
    """
    # Standard library imports
    from pathlib import Path
    
    # Globals imports
    from BiblioParsing.BiblioSpecificGlobals import PARSING_ITEMS_LIST
    
    # Cycling on parsing items 
    for item in PARSING_ITEMS_LIST:
        if item in parsing_dict.keys():
            item_df = parsing_dict[item]
            if save_extent == "xlsx":
                item_xlsx_file = item_filename_dict[item] + ".xlsx"
                item_xlsx_path = parsing_path / Path(item_xlsx_file)
                item_df.to_excel(item_xlsx_path, index = False)
            elif save_extent == "dat":
                item_tsv_file = item_filename_dict[item] + ".dat"
                item_tsv_path = parsing_path / Path(item_tsv_file)
                item_df.to_csv(item_tsv_path, index = False, sep = '\t')
            else:
                item_tsv_file = item_filename_dict[item] + ".csv"
                item_tsv_path = parsing_path / Path(item_tsv_file)
                item_df.to_csv(item_tsv_path, index = False, sep = '\,')
        else:
            pass

    message = f"All parsing results saved as {save_extent} files"
    return message  


def save_fails_dict(fails_dict, parsing_path):
    '''The function `save_fails_dict` saves parsing fails in a json file
    named "failed.json".
    
    Args:
        fails_dict (dict): The dict of parsing fails.
        parsing_path (path): The full path of the parsing results folder 
        where the json file is being saved.
        
    Returns:
        None
        
    '''
    # Standard library imports
    import json
    from pathlib import Path
    
    with open(parsing_path / Path('failed.json'), 'w') as write_json:
        json.dump(fails_dict, write_json, indent=4)
        
    message = f"Parsing-fails results saved as json file"
    return message 


def save_db_ids_data(db_ids_df, parsing_path, database):
    """The function `save_db_ids_data` saves database-IDs data in an xlsx file.
    
    Args:
        db_ids_df (dataframe): The database IDs data.
        parsing_path (path): The full path of the parsing results folder \
        for saving the xlsx file.
    """
    # Standard library imports
    from pathlib import Path
    
    file_name = database.capitalize() + "_IDs.xlsx"
    file_path = parsing_path / Path(file_name)
    
    db_ids_df.to_excel(file_path, index=False)
        
    message = f"Database-IDs data saved as xlsx file"
    return message


def save_parsing_dicts(parsing_dicts_dict, parsing_path_dict, item_filename_dict,
                       save_extent, fails_dicts, ids_dfs_dict): 
    """
    
    Note:
        Uses `save_parsing_dict` function.
    """
    fails_save_status = False
    db_ids_save_status = False

    for parsing_name, parsing_dict in parsing_dicts_dict.items():
        parsing_path = parsing_path_dict[parsing_name]
        _ = save_parsing_dict(parsing_dict, parsing_path, item_filename_dict, save_extent)
        
        if parsing_name in fails_dicts.keys():
            parsing_fails_dict = fails_dicts[parsing_name]
            _ = save_fails_dict(parsing_fails_dict, parsing_path)
            fails_save_status = True   

        if parsing_name in ids_dfs_dict.keys():
            db_ids_df = ids_dfs_dict[parsing_name]
            _ = save_db_ids_data(db_ids_df, parsing_path, parsing_name)
            db_ids_save_status = True

    message = f"All parsing-to-deduplication results saved as files with .{save_extent} extension."
    if fails_save_status:
        message += f"\n All parsing-fails results saved as json files."
    if db_ids_save_status:
        message += f"\n All database-IDs data saved as xlsx files."
    
    return message
