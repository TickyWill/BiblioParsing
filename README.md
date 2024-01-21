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
item_filename_dict = bp.DEMO_GLOBAL['PARSING_FILE_NAMES']

# Setting the files type for saving results
file_extent = "xlsx"

# Setting the user's authors affiliations filter as a list of tuples (institution,country)
user_inst_filter_list = [(<institution1>,<country1),(<institution2>,<country2),...]
    
# Parsing Scopus rawdata and saving parsing results
scopus_raw_path = Path(<your_fullpath_file_to_scopus_rawdata>)
scopus_parsing_path = Path(<your_fullpath_for_scopus_parsing_results>)
scopus_parsing_dict, scopus_fails_dict = bp.biblio_parser_scopus(scopus_raw_path)
bp.save_parsing_dict(scopus_parsing_dict, scopus_parsing_path, item_filename_dict, file_extent)
bp.save_fails_dict(scopus_fails_dict, scopus_parsing_path)
    
# Parsing WoS rawdata and saving results
wos_raw_path = Path(<your_fullpath_file_to_wos_rawdata>)
wos_parsing_path = Path(<your_fullpath_for_wos_parsing_results>)
wos_parsing_dict, wos_fails_dict = bp.biblio_parser_wos(wos_raw_path) 
bp.save_parsing_dict(wos_parsing_dict, wos_parsing_path, item_filename_dict, file_extent)
bp.save_fails_dict(wos_fails_dict, wos_parsing_path)
    
# Parsings concatenation and saving results
concat_parsing_path = Path(<your_fullpath_for_parsings_concat_results>)
concat_parsing_dict = bp.concatenate_parsing(scopus_parsing_dict, wos_parsing_dict,  
                                             inst_filter_list = user_inst_filter_list)
bp.save_parsing_dict(concat_parsing_dict, concat_parsing_path, item_filename_dict, file_extent)

# Parsings dediplication
dedup_parsing_path = Path(<your_fullpath_for_parsings_dedup_results>)
dedup_parsing_dict = bp.deduplicate_parsing(concat_parsing_dict)
bp.save_parsing_dict(dedup_parsing_dict, dedup_parsing_path, item_filename_dict, file_extent)
```
**for more exemples refer to** [BiblioParsing-exemples](https://github.com/TickyWill/BiblioParsing/Demo_BiblioParsing.ipynb).


# Release History
1.0.0 first release


# Meta
	- authors : BiblioAbnalysis team

Distributed under the [MIT license](https://mit-license.org/)
