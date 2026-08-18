"""Module of global parameters built from generic information 
such as countries with their specific structure of zip codes.
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


# Standard library imports
import ast
from pathlib import Path

# 3rd party imports
import pandas as pd


# Conversion factor for inch to millimeter
IN_TO_MM = 25.4

# Countries normalized names and GPS coordinates
COUNTRIES_INFO = 'Countries.xlsx'

COUNTRIES_COL_NAMES = {"country"    : "Country",
                       "short_name" : "Short name",
                       "gps"        : "GPS Coordinates",
                       "zip_letters": "Zip code letters",
                       "zip_digits" : "Zip code digits",
                       "continent"  : "Continent",
                      }

REP_UTILS = 'RefFiles'


def build_countries_globals():
    """Builds countries list and their attributes as given in the dedicated EXCEL file.

    The name of the file is given by the 'COUNTRIES_INFO' global defined in the same module. 
    The file is located in the folder of the package which name is given by the 'REP_UTILS' global 
    defined in the same module. 
    The function builds the 'countries' countries list and the 'countries_gps' dict keyed by countries 
    and valued by a tuple '(lat,long)' where 'lat' is the country capital latitude and 'long', the country capital 
    longitude expressed in decimal degrees. 
    It builds also the 'countries_codes' dict keyed by countries and valued by the ISO code (3 letters) of the country 
    and the 'zip_codes' hierarchical dict where the outer dict is keyed by countries and valued by an inner dict keyed 
    by 'letters' and 'digits' of the zip-code.

        ex: zip_codes['France'] = {'letters': ['f', 'fr'], 'digits': [5, 6]} where the given digits are the possible \
        number of digits in the zip-code.

    Finally, it builds the 'countries_continent' dict keyed by countryes and valued by the country's continent.

    Returns:
        (list, dict, dict, dict): tuple of the built data.
    """
    # Setting columns name aliases
    col_keys = ['country', 'gps', 'short_name', 'zip_letters', 'zip_digits', 'continent']
    (countries_col, gps_col, short_col, zip_letters_col,
     zip_digits_col, continent_col) = [COUNTRIES_COL_NAMES[key] for key in col_keys]

    # Setting the specific file paths for countries information
    path_countries_info = Path(__file__).parent / Path(REP_UTILS) / Path(COUNTRIES_INFO)
    df = pd.read_excel(path_countries_info)

    countries = df[countries_col].to_list()
    countries_gps = {x[0]:ast.literal_eval(x[1])
                     for x in zip(df[countries_col], df[gps_col])}
    countries_codes = {x[0]:x[1] for x in zip(df[countries_col], df[short_col])}
    zip_codes = {x[0]:{'letters':ast.literal_eval(x[1]), 'digits':ast.literal_eval(x[2])}
                 for x in zip(df[countries_col], df[zip_letters_col], df[zip_digits_col])}
    countries_continent = {x[0]:x[1] for x in zip(df[countries_col], df[continent_col])}

    return countries, countries_gps, countries_codes, zip_codes, countries_continent

COUNTRIES, COUNTRIES_GPS, COUNTRIES_CODES, ZIP_CODES, COUNTRIES_CONTINENT =  build_countries_globals()

# Escape dot for the regex
for country in ZIP_CODES.keys():
    ZIP_CODES[country]['letters'] = [x.replace(".", r"\.").lower()
                                     for x in ZIP_CODES[country]['letters']]

USA_STATES = ("AL,AK,AZ,AR,CA,CO,CT,DE,FL,GA,HI,ID,IL,IN,IA,KS,KY,"
              "LA,ME,MD,MA,MI,MN,MS,MO,MT,NE,NV,NH,NJ,NM,NY,NC,ND,"
              "OH,OK,OR,PA,RI,SC,SD,TN,TX,UT,VT,VA,WA,WV,WI,WY")
USA_ALIASES = "UNITED STATES, United States of America, USA," + USA_STATES

COUNTRY_ALIASES = {"Belarus"              : ["BELARUS", "BLR"],
                   "China"                : ["China", "china"],
                   "France"               : ["FRANCE", "france", "FR", "Fr"],
                   "Netherlands"          : ["Netherlands"],
                   "Palestinian Territory": ["Palestine"],
                   "Russian Federation"   : ["Russia"],
                   "Turkey"               : ["Turkiye"],
                   "United Arab Emirates" : ["U Arab Emirates", "Arab Emirates"],
                   "United Kingdom"       : ["England", "Wales", "North Ireland", "Scotland"],
                   "United States"        : [x.strip() for x in USA_ALIASES.split(',')],
                   "Viet Nam"             : ["Vietnam"],
                  }


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

# For changing particularly encoded symbols (particular cote to standard cote)
APOSTROPHE_CHANGE_DIC = {"”": "'",
                         "’": "'",
                         '"': "'",
                         "“": "'",
                         "'": "'",
                        }
APOSTROPHE_CHANGE = str.maketrans(APOSTROPHE_CHANGE_DIC)


# For replacing dashes by hyphen-minus
DASHES_CHANGE_DIC = {"‐": "-",   # Non-Breaking Hyphen to hyphen-minus
                     "—": "-",   # En-dash to hyphen-minus
                     "–": "-",   # Em-dash to hyphen-minus
                     }
DASHES_CHANGE = str.maketrans(DASHES_CHANGE_DIC)


# For changing langages specific characters to standard characters in personal names
LANG_CHAR_CHANGE_DIC = {"Ł": "L",   # polish capital to L
                        "ł": "l",   # polish l
                        "ı": "i",
                        "Đ": "D",   # D with stroke (Vietamese,South Slavic) to D
                        "&": "",
                        }
LANG_CHAR_CHANGE = str.maketrans(LANG_CHAR_CHANGE_DIC)


# For droping ponctuation symbols
PONCT_CHANGE_DIC = {".": "",
                    ",": "",
                    ";": "",
                   }
PONCT_CHANGE = str.maketrans(PONCT_CHANGE_DIC)


# For changing particularly encoded symbols
SYMB_CHANGE_DIC = {"&": "and",
                   "’": "'",   # Particular cote to standard cote
                   ".": "",
                   "-": " ",   # To Do: to be tested from the point of view of the effect on raw institutions
                   "§": " ",
                   "(": " ",
                   ")": " ",
                   "/": " ",
                   "'": " ",   # To Do: to be tested from the point of view of the effect on raw institutions
                  }
SYMB_CHANGE = str.maketrans(SYMB_CHANGE_DIC)


# For droping particular symbols
DROP_SYMB_DIC = {"'": " ",
                 "*": " ",
                 "#": " ",
                 "|": " ",
                }
SYMB_DROP = str.maketrans(DROP_SYMB_DIC)


# For changing particular symbols in document-title
TITLE_SYMB_CHANGE_DIC = {" - ": "-",
                         "("  : "",
                         ")"  : "",
                         " :" : ": ",
                         "-"  : " ",
                         "  " : " ",
                        }
