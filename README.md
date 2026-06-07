# BiblioParsing
## Description
Python modules for parsing the rawdata extracted from Scopus and WoS databases.

## Installation
Run the following to install:
```python
pip install git+https://github.com/TickyWill/BiblioParsing.git@main
```

## Usage example
```python
from pathlib import Path
import bpfuncts as bp

# Getting the filenames for each parsing item
config_tup = bp.set_user_config()
item_filename_dict = config_tup[3]

# Setting the files type for saving results
save_extent = "xlsx"

# Setting the user's authors affiliations filter as a list of tuples (institution normalized name, institution column name)
user_affil_filter_list = [(<normalized name 1>, <column name 1>),
                         (<normalized name 2>, <column name 2>),
                         ...]

# Setting the user's xlsx files for mormalizing institutions
# if set to None, use of default files of RefFiles folder
user_affil_files_dic = {'country_towns_file'   : <your_country_towns_file_name>,
                        'country_affils_file'  : <your_country_affiliations_file_name>,
                        'institute_affils_file': <your_institute_affiliations_file_name>,
                        'affil_types_file'     : <your_affilation_types_file_name>,
                       }

user_rep_utils = <full_path_to_your_affil_root_folder_name>
rawdata_affil_params_dic = bp.set_step_affil_parsing_paths(user_rep_utils, user_affil_files_dic,
                                                           rawdata_parsing_step=True)
dedup_affil_params_dic = bp.set_step_affil_parsing_paths(user_rep_utils, user_affil_files_dic,
                                                         rawdata_parsing_step=False)

# Setting the user's status of building normalized institutions file and raw institutions file after deduplicating parsing
user_norm_affil_status = True

# Parsing Scopus rawdata and saving parsing results
scopus_raw_path = Path(<your_fullpath_to_scopus_rawdata>)
scopus_parsing_path = Path(<your_fullpath_for_scopus_parsing_results>)
return_tup = bp.scopus_parser(scopus_raw_path, affil_filter_list=None,
                               affil_params_dic=rawdata_affil_params_dic)
scopus_parsing_dict, scopus_fails_dict, scopus_ids_df = return_tup[0:3]
bp.save_parsing_dict(scopus_parsing_dict, scopus_parsing_path, item_filename_dict, save_extent)
bp.save_fails_dict(scopus_fails_dict, scopus_parsing_path)
bp.save_db_ids_data(scopus_ids_df, scopus_parsing_path, bp.SCOPUS)

# Parsing WoS rawdata and saving results
wos_raw_path = Path(<your_fullpath_to_wos_rawdata>)
wos_parsing_path = Path(<your_fullpath_for_wos_parsing_results>)
return_tup = bp.wos_parser(wos_raw_path, affil_filter_list=None,
                                  affil_params_dic=rawdata_affil_params_dic)
wos_parsing_dict, wos_fails_dict, wos_ids_df = return_tup
bp.save_parsing_dict(wos_parsing_dict, wos_parsing_path, item_filename_dict, save_extent)
bp.save_fails_dict(wos_fails_dict, wos_parsing_path)
bp.save_db_ids_data(wos_ids_df, wos_parsing_path, bp.WOS)

# Parsings concatenation and saving results
concat_parsing_path = Path(<your_fullpath_for_parsings_concat_results>)
concat_parsing_dict = bp.concatenate_parsing(scopus_parsing_dict, wos_parsing_dict,
                                             affil_filter_list=user_affil_filter_list)
bp.save_parsing_dict(concat_parsing_dict, concat_parsing_path, item_filename_dict, save_extent)

# Parsings deduplication and saving results
dedup_parsing_path = Path(<your_fullpath_for_parsings_dedup_results>)
dedup_parsing_dict = bp.deduplicate_parsing(concat_parsing_dict, norm_affil_status=user_norm_affil_status,
                                            affil_params_dic=rawdata_affil_params_dic)
bp.save_parsing_dict(dedup_parsing_dict, dedup_parsing_path, item_filename_dict, save_extent)
```
**for more exemples refer to** [BiblioParsing-exemples](https://github.com/TickyWill/BiblioParsing/Demo_BiblioParsing.ipynb).


# Release History
1.0.0 first release
1.1.0 Enhancement of author with affiliations parsing
1.2.0 Added parsing of database-IDs
2.0.0 Deep refactoring based on rationalization of imports
3.0.0 Deep refactoring based on rationalization of functions, modules and regex

# Meta
	- authors : BiblioAbnalysis team

Distributed under the [MIT license](https://mit-license.org/)
