# BiblioParsing
## Description
Python modules for parsing the rawdata extracted from Scopus and WoS databases.

## Installation
Run the following to install:
```python
pip install BiblioParsing
```

## Usage example
```python
from pathlib import Path
import BiblioParsing as bp

# Getting the filenames for each parsing item
config_tup = bp.set_user_config()
item_filename_dict = config_tup[3]

# Setting the files type for saving results
save_extent = "xlsx"

# Setting the user's authors affiliations filter as a list of tuples (institution normalized name, institution column name)
user_inst_filter_list = [(<normalized name 1>, <column name 1>),
                         (<normalized name 2>, <column name 2>),
                         ...]

# Setting the user's xlsx files for mormalizing institutions 
# if set to None, use of default files of BiblioParsing_RefFiles folder
user_institute_affiliations_file_path = Path(<your_fullpath_to_institute_affiliations_file>)
user_country_affiliations_file_path = Path(<your_fullpath_to_country_affiliations_file>)
user_inst_types_file_path = Path(<your_fullpath_to_inst_types_file>)
user_country_towns_folder_path = Path(<your_fullpath_to_country_towns_folder>)
user_country_towns_file = Path(<your_country_towns_file_name>)

# Setting the user's status of building normalized institutions file and raw institutions file after deduplicating parsing
user_norm_inst_status = True
    
# Parsing Scopus rawdata and saving parsing results
scopus_raw_path = Path(<your_fullpath_to_scopus_rawdata>)
scopus_parsing_path = Path(<your_fullpath_for_scopus_parsing_results>)
scopus_parsing_dict, scopus_fails_dict = bp.biblio_parser_scopus(scopus_raw_path,
                                                                 inst_filter_list = None,
                                                                 country_affiliations_file_path = user_institute_affiliations_file_path,
                                                                 inst_types_file_path = user_inst_types_file_path,
                                                                 country_towns_file = user_country_towns_file,
                                                                 country_towns_folder_path = user_country_towns_folder_path)
bp.save_parsing_dict(scopus_parsing_dict, scopus_parsing_path, item_filename_dict, save_extent)
bp.save_fails_dict(scopus_fails_dict, scopus_parsing_path)
    
# Parsing WoS rawdata and saving results
wos_raw_path = Path(<your_fullpath_to_wos_rawdata>)
wos_parsing_path = Path(<your_fullpath_for_wos_parsing_results>)
wos_parsing_dict, wos_fails_dict = bp.biblio_parser_wos(wos_raw_path,
                                                        inst_filter_list = None,
                                                        country_affiliations_file_path = user_institute_affiliations_file_path,
                                                        inst_types_file_path = user_inst_types_file_path,
                                                        country_towns_file = user_country_towns_file,
                                                        country_towns_folder_path = user_country_towns_folder_path) 
bp.save_parsing_dict(wos_parsing_dict, wos_parsing_path, item_filename_dict, save_extent)
bp.save_fails_dict(wos_fails_dict, wos_parsing_path)
    
# Parsings concatenation and saving results
concat_parsing_path = Path(<your_fullpath_for_parsings_concat_results>)
concat_parsing_dict = bp.concatenate_parsing(scopus_parsing_dict, wos_parsing_dict,  
                                             inst_filter_list = user_inst_filter_list)
bp.save_parsing_dict(concat_parsing_dict, concat_parsing_path, item_filename_dict, save_extent)

# Parsings deduplication and saving results
dedup_parsing_path = Path(<your_fullpath_for_parsings_dedup_results>)
dedup_parsing_dict = bp.deduplicate_parsing(concat_parsing_dict, 
                                            norm_inst_status = user_norm_inst_status,
                                            inst_types_file_path = user_inst_types_file_path,
                                            country_affiliations_file_path = user_country_affiliations_file_path,
                                            country_towns_file = user_country_towns_file,
                                            country_towns_folder_path = user_country_towns_folder_path)
bp.save_parsing_dict(dedup_parsing_dict, dedup_parsing_path, item_filename_dict, save_extent)
```
**for more exemples refer to** [BiblioParsing-exemples](https://github.com/TickyWill/BiblioParsing/Demo_BiblioParsing.ipynb).


# Release History
1.0.0 first release
1.1.0 Enhancement of author with affiliations parsing


# Meta
	- authors : BiblioAbnalysis team

Distributed under the [MIT license](https://mit-license.org/)
