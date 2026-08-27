"""Module of global parameters built from generic information 
such as countries with their specific structure of zip codes.

The parameters values are set from the 'general_globals.yaml' file 
available by default in the 'DemoConfig' folder of the package.
"""

__all__ = ['ACCENT_CHANGE',
           'APOSTROPHE_CHANGE',
           'COUNTRIES',
           'COUNTRY_ALIASES',
           'COUNTRIES_CODES',
           'COUNTRIES_CONTINENT',
           'COUNTRIES_GPS',
           'DASHES_CHANGE',
           'IN_TO_MM',
           'LANG_CHAR_CHANGE',
           'PONCT_CHANGE',
           'SYMB_CHANGE',
           'SYMB_DROP',
           'REP_UTILS',
           'TITLE_SYMB_CHANGE_DIC',
           'USA_STATES',
           'ZIP_CODES',]

# Local imports
from bpfuncts.globals_utils import build_countries_globals
from bpfuncts.globals_utils import read_yaml_general_globals

# Getting the globals values from the YAML file of general globals
general_globals_dic = read_yaml_general_globals()

# Conversion factor for inch to millimeter
IN_TO_MM = general_globals_dic['in_to_mm']

# Folder name where useful files are located
REP_UTILS = general_globals_dic['rep_utils']

# USA-states codes
USA_STATES = general_globals_dic['usa_states']

# Countries aliases
_USA_ALIASES_LIST = general_globals_dic['usa_aliases_list']
COUNTRY_ALIASES = general_globals_dic['country_aliases']
COUNTRY_ALIASES["United States"] = _USA_ALIASES_LIST + [x.strip() for x in USA_STATES.split(',')]

# Normalized name, GPS coordinates, code, zip-code format and continent for each country
_COUNTRIES_INFO = general_globals_dic['countries_info']

_COUNTRIES_COL_NAMES = general_globals_dic['countries_col_names']
(COUNTRIES, COUNTRIES_GPS, COUNTRIES_CODES, ZIP_CODES,
 COUNTRIES_CONTINENT) = build_countries_globals(REP_UTILS, _COUNTRIES_INFO, _COUNTRIES_COL_NAMES)

#    - Escaping dots for use in regex
for country in ZIP_CODES.keys():
    ZIP_CODES[country]['letters'] = [x.replace(".", r"\.").lower()
                                     for x in ZIP_CODES[country]['letters']]


# For changing particularly encoded symbols (particular cote to standard cote)
APOSTROPHE_CHANGE_DIC = general_globals_dic['apostrophe_change_dic']
APOSTROPHE_CHANGE = str.maketrans(APOSTROPHE_CHANGE_DIC)

# For replacing dashes by hyphen-minus
DASHES_CHANGE_DIC = general_globals_dic['dashes_change_dic']
DASHES_CHANGE = str.maketrans(DASHES_CHANGE_DIC)

# For changing langages specific characters to standard characters in personal names
LANG_CHAR_CHANGE_DIC = general_globals_dic['lang_char_change_dic']
LANG_CHAR_CHANGE = str.maketrans(LANG_CHAR_CHANGE_DIC)

# For droping ponctuation symbols
PONCT_CHANGE_DIC = general_globals_dic['ponct_change_dic']
PONCT_CHANGE = str.maketrans(PONCT_CHANGE_DIC)

# For changing particularly encoded symbols
SYMB_CHANGE_DIC = general_globals_dic['symb_change_dic']
SYMB_CHANGE = str.maketrans(SYMB_CHANGE_DIC)

# For droping particular symbols
DROP_SYMB_DIC = general_globals_dic['drop_symb_dic']
SYMB_DROP = str.maketrans(DROP_SYMB_DIC)

# For changing particular symbols in document-title
TITLE_SYMB_CHANGE_DIC = general_globals_dic['title_symb_change_dic']

# Character replacements
#To Do : Check if this global is still used
ACCENT_CHANGE_DIC = {'À': 'A', 'Á': 'A', 'Â': 'A', 'Ã': 'A', 'Ä': 'A',
                     'à': 'a', 'á': 'a', 'â': 'a', 'ã': 'a', 'ä': 'a', 'ª': 'A',
                     'È': 'E', 'É': 'E', 'Ê': 'E', 'Ë': 'E',
                     'è': 'e', 'é': 'e', 'ê': 'e', 'ë': 'e',
                     'Í': 'I', 'Ì': 'I', 'Î': 'I', 'Ï': 'I',
                     'í': 'i', 'ì': 'i', 'î': 'i', 'ï': 'i',
                     'Ò': 'O', 'Ó': 'O', 'Ô': 'O', 'Õ': 'O', 'Ö': 'O',
                     'ò': 'o', 'ó': 'o', 'ô': 'o', 'õ': 'o', 'ö': 'o', 'º': 'O',
                     'Ù': 'U', 'Ú': 'U', 'Û': 'U', 'Ü': 'U',
                     'ù': 'u', 'ú': 'u', 'û': 'u', 'ü': 'u',
                     'Ñ': 'N', 'ñ': 'n',
                     'Ç': 'C', 'ç': 'c',
                     'Ž': 'Z','ž': 'z'}
ACCENT_CHANGE = str.maketrans(ACCENT_CHANGE_DIC)
