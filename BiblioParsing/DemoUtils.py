'''
'''
__all__ = ['build_files_paths',
           'build_item_filename_dict',
           'parse_to_dedup',
           'save_parsing_dicts',    
          ]

    
def build_files_paths(year, parsing_folder_dict, root_path):
    
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
    db_dict           = {}
    # Creating the databases folders if not available
    for db_num in list(parsing_folder_dict['corpus']['databases'].keys()):          
        
        keys_list = ['corpus', 'databases', db_num, 'root']
        db_root_path, db_root_name = _create_folder(parsing_folder_dict, keys_list, corpus_folder_path)
        db_dict[db_num] = db_root_name
        
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
    
    return (rawdata_path_dict, parsing_path_dict, db_dict)

def parse_to_dedup(year, db_raw_dict, verbose = False):
    """
    """
    
    # Local library imports
    from BiblioParsing.BiblioParsingUtils import biblio_parser
    from BiblioParsing.BiblioParsingConcat import parsing_concatenate_deduplicate
    
    # Globals imports
    from BiblioParsing.BiblioSpecificGlobals import INST_FILTER_LIST
    from BiblioParsing.BiblioSpecificGlobals import SCOPUS
    from BiblioParsing.BiblioSpecificGlobals import WOS
    
    # Parsing Scopus rawdata
    scopus_raw_path = db_raw_dict[SCOPUS]
    scopus_parsing_dict, scopus_fails_dict = biblio_parser(scopus_raw_path, SCOPUS, inst_filter_list = None)        
        
    # Parsing WoS rawdata 
    wos_raw_path = db_raw_dict[WOS]
    wos_parsing_dict, wos_fails_dict =  biblio_parser(wos_raw_path, WOS, inst_filter_list = None)
    
    # Concatenating and deduplicating the Scopus and WoS parsings
    concat_parsing_dict, dedup_parsing_dict = parsing_concatenate_deduplicate(scopus_parsing_dict, 
                                                                                 wos_parsing_dict, 
                                                                                 inst_filter_list = INST_FILTER_LIST)
    
    # Building parsing performances dict
    fails_dicts = {}
    fails_dicts[SCOPUS] = scopus_fails_dict
    fails_dicts[WOS]    = wos_fails_dict  
    
    # Building results dict
    parsing_dicts_dict = {}
    parsing_dicts_dict[SCOPUS] = scopus_parsing_dict
    if verbose: print(f'\nScopus corpus successfully parsed in parsing_dicts_dict[{SCOPUS}]')
    parsing_dicts_dict[WOS]    = wos_parsing_dict
    if verbose: print(f'\nWos corpus successfully parsed in parsing_dicts_dict[{WOS}]')
    parsing_dicts_dict["concat"] = concat_parsing_dict
    if verbose: print(f'\nParsings successfully concatenated in parsing_dicts_dict["concat"]')
    parsing_dicts_dict["dedup"]  = dedup_parsing_dict
    if verbose: print(f'\nParsings successfully deduplicated in parsing_dicts_dict["dedup"]')
    
    return parsing_dicts_dict, fails_dicts


def build_item_filename_dict(parsing_filenames_dict):
    # Globals imports
    from BiblioParsing.BiblioSpecificGlobals import PARSING_ITEMS
    
    item_filename_dict = {}
    for key,item in PARSING_ITEMS.items():
        item_filename_dict[item] = parsing_filenames_dict[key]

    return item_filename_dict


def _save_parsing(parsing_name, parsing_dict, parsing_path, 
                 item_filename_dict, tsv_extent):
    """
    """
    # Standard library imports
    import os
    from pathlib import Path
    
    # Globals imports
    from BiblioParsing.BiblioSpecificGlobals import PARSING_ITEMS
    
    # Setting useful variables
    xlsx_extent = "xlsx"
    
    # Setting default values
    item_xlsx_path = False
    
    # Cycling on parsing items 
    for item_key,item in PARSING_ITEMS.items():
        if item in parsing_dict.keys():
            item_df = parsing_dict[item]
            item_tsv_file = item_filename_dict[item] + "."  + tsv_extent
            item_tsv_path = parsing_path / Path(item_tsv_file)
            item_df.to_csv(item_tsv_path, index = False, sep = '\t')

            if item_key == "articles": 
                item_xlsx_file = item_filename_dict[item] + "_" + parsing_name + "." + xlsx_extent
                item_xlsx_path = parsing_path / Path(item_xlsx_file)
                item_df.to_excel(item_xlsx_path, index = False)
        else:
            pass

    message = f"All {parsing_name} parsing results saved as tsv files"
    return message  


def save_parsing_dicts(parsing_dicts_dict, parsing_path_dict, 
                       item_filename_dict, tsv_extent): 
    """
    
    Note:
        Uses `save_corpus_parsing` function.
    """

    # Standard library imports
    import json
    import os
    from pathlib import Path

    for parsing_name, parsing_dict in parsing_dicts_dict.items():
        parsing_path = parsing_path_dict[parsing_name]
        _save_parsing(parsing_name, parsing_dict, parsing_path, item_filename_dict, tsv_extent)
        
        if parsing_name in fails_dicts.keys():
            parsing_failed_dict = fails_dicts[parsing_name]
            with open(parsing_path / Path('failed.json'), 'w') as write_json:
                json.dump(parsing_failed_dict, write_json, indent=4)
    message = f"All results saved"
    return message


