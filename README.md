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
import json
from pathlib import Path
import BiblioParsing as bp

# Setting the user's filenames (values) for the parsing items (keys)
item_filename_dict = {'AD' : 'addresses', 
                      "ADI": "addresses_institutions",
                      'A'  : 'articles',
                      'AU' : 'authors',
                      'I2' : 'authorsinst',
                      'AK' : 'authorskeywords',
                      'CU' : 'countries',
                      'I'  : 'institutions',
                      'IK' : 'journalkeywords',
                      'TK' : 'titlekeywords',
                      'S'  : 'subjects',
                      'S2' : 'subjects2',
                      'I3' : 'rawinstitutions',
                      'R'  : 'references'}

# Setting the user's authors affiliations filter as a list of tuples (institution,country)
user_inst_filter_list = [(<institution1>,<country1),(<institution2>,<country2),...]
    
# Parsing Scopus rawdata
scopus_raw_path = Path(<your_fullpath_file_to_scopus_rawdata>)
scopus_parsing_path = Path(<your_fullpath_for_scopus_parsing_results>)
scopus_parsing_dict, scopus_fails_dict = bp.biblio_parser(scopus_raw_path, bp.SCOPUS) 
bp.save_parsing_xlsx(bp.SCOPUS, scopus_parsing_dict, scopus_parsing_path, item_filename_dict)
with open(scopus_parsing_path / Path('failed.json'), 'w') as write_json:
    json.dump(scopus_fails_dict, write_json, indent = 4)
    
# Parsing WoS rawdata
wos_raw_path = Path(<your_fullpath_file_to_wos_rawdata>)
wos_parsing_path = Path(<your_fullpath_for_wos_parsing_results>)
wos_parsing_dict, wos_fails_dict = bp.biblio_parser(wos_raw_path, bp.WOS) 
bp.save_parsing_xlsx(bp.WOS, wos_parsing_dict, wos_parsing_path, item_filename_dict)
with open(wos_parsing_path / Path('failed.json'), 'w') as write_json:
    json.dump(wos_fails_dict, write_json, indent = 4)
    
# Parsings concatenation
concat_parsing_path = Path(<your_fullpath_for_parsings_concat_results>)
concat_parsing_dict = bp.concatenate_parsing(scopus_parsing_dict, wos_parsing_dict,  
                                             inst_filter_list = user_inst_filter_list)
bp.save_parsing_xlsx('concatenation', concat_parsing_dict, concat_parsing_path, item_filename_dict)

# Parsings dediplication
dedup_parsing_path = Path(<your_fullpath_for_parsings_dedup_results>)
dedup_parsing_dict = bp.deduplicate_parsing(concat_parsing_dict)
bp.save_parsing_xlsx('deduplication', dedup_parsing_dict, dedup_parsing_path, item_filename_dict)
```
**for more exemples refer to** [BiblioParsing-exemples](https://github.com/TickyWill/BiblioParsing/Demo_BiblioParsing.ipynb).


# Release History
1.0.0 first release


# Meta
	- authors : BiblioAbnalysis team

Distributed under the [MIT license](https://mit-license.org/)
