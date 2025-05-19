__all__ = ['ACCENT_CHANGE',
           'ALIAS_UK',
           'APOSTROPHE_CHANGE',
           'CHANGE',
           'COUNTRIES',
           'COUNTRIES_CODES',
           'COUNTRIES_CONTINENT',
           'COUNTRIES_GPS',
           'DASHES_CHANGE',
           'IN_TO_MM',
           'LANG_CHAR_CHANGE',
           'PONCT_CHANGE',
           'SYMB_CHANGE',
           'SYMB_DROP',
           'USA_STATES',
           'ZIP_CODES',]


# Countries normalized names and GPS coordinates
COUNTRIES_INFO = 'Countries.xlsx'

COUNTRIES_COL_NAMES = {"country"    : "Country",
                       "short_name" : "Short name",
                       "gps"        : "GPS Coordinates",
                       "zip_letters": "Zip code letters",
                       "zip_digits" : "Zip code digits",
                       "continent"  : "Continent",                       
                      }
        
REP_UTILS = 'BiblioParsing_RefFiles' 

def build_countries_globals():
        
    '''The `build_countries_global` function reads the EXCEL file which name is given by the global 'COUNTRIES_INFO'. 
    It builds the countries list ' countries' and a dict 'countries_gps' keyed by countries and valued by a tuple '(lat,long)' 
    where 'lat' is the country capital latitude and 'long', the country capital longitude expressed in decimal degrees.
    It builds also a dict 'countries_codes' keyed by countries and valued by the ISO code (3 letters) of the country.
    Finally, it builds a dict of dict 'zip_codes' where the outer dict is keyed by countries and valued by an inner dict keyed 
    by 'letters' and 'digits' of the zip-code.
    
        ex: zip_codes['France'] = {'letters': ['f', 'fr'], 'digits': [5, 6]} where the given digits are the possible number of digits in 
    the zip-code.
    
    Args:
        None

    Returns:
        (list,dict,dict,dict): tuple of countries, countries_gps, countries_codes and zip_codes.
        
    Notes:
        The global 'COUNTRIES_INFO' is imported from 'BiblioGeneralGlobals' module of the 'BiblioParsing' package.
        The global 'REP_UTILS' is imported from 'BiblioSpecificGlobals' module of the 'BiblioParsing' package.
        
    '''

    # Standard library imports
    import ast
    from pathlib import Path
    
    # 3rd party imports
    import pandas as pd
    
    # Setting columns name aliases
    countries_alias   = COUNTRIES_COL_NAMES['country']
    gps_alias         = COUNTRIES_COL_NAMES['gps']
    short_alias       = COUNTRIES_COL_NAMES['short_name']
    zip_letters_alias = COUNTRIES_COL_NAMES['zip_letters']
    zip_digits_alias  = COUNTRIES_COL_NAMES['zip_digits']
    continent_alias   = COUNTRIES_COL_NAMES['continent']

    # Setting the specific file paths for countries information    
    path_countries_info = Path(__file__).parent / Path(REP_UTILS) / Path(COUNTRIES_INFO)
    df = pd.read_excel(path_countries_info)
    
    countries = df[countries_alias].to_list() 
    countries_gps = {x[0]:ast.literal_eval(x[1])
                     for x in zip(df[countries_alias], df[gps_alias])}
    countries_codes = {x[0]:x[1] for x in zip(df[countries_alias], df[short_alias])}
    zip_codes = {x[0]:{'letters':ast.literal_eval(x[1]), 'digits':ast.literal_eval(x[2])}
                 for x in zip(df[countries_alias], df[zip_letters_alias], df[zip_digits_alias])}
    countries_continent = {x[0]:x[1] for x in zip(df[countries_alias], df[continent_alias])}
    
    return (countries, countries_gps, countries_codes, zip_codes, countries_continent)

COUNTRIES, COUNTRIES_GPS, COUNTRIES_CODES, ZIP_CODES, COUNTRIES_CONTINENT =  build_countries_globals()
# Escape dot for the regex
for country in ZIP_CODES.keys(): ZIP_CODES[country]['letters'] = [x.replace(".", r"\.").lower() 
                                                                  for x in ZIP_CODES[country]['letters']]


USA_STATES = '''AL,AK,AZ,AR,CA,CO,CT,DE,FL,GA,HI,ID,IL,IN,IA,KS,KY,LA,ME,MD,MA,MI,MN,MS,MO,MT,
NE,NV,NH,NJ,NM,NY,NC,ND,OH,OK,OR,PA,RI,SC,SD,TN,TX,UT,VT,VA,WA,WV,WI,WY'''
USA_STATES = [x.strip() for x in USA_STATES.split(',')]

ALIAS_UK = '''England,Wales,North Ireland,Scotland'''
ALIAS_UK = [x.strip() for x in ALIAS_UK.split(',')] 


#To Do : Check if this global is still used
# Character replacements
DIC_CHANGE_ACCENT = {'À': 'A', 'Á': 'A', 'Â': 'A', 'Ã': 'A', 'Ä': 'A',
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

ACCENT_CHANGE = str.maketrans(DIC_CHANGE_ACCENT)

#To Do : Check if this global is still used
DIC_CHANGE_CHAR = {"Ł":"L",   # polish capital to L 
                   "ł":"l",   # polish l
                   "ı":"i",    
                   "‐":"-",   # Non-Breaking Hyphen to hyphen-minus
                   "—":"-",   # En-dash to hyphen-minus
                   "–":"-",   # Em-dash to hyphen-minus
                   "Đ":"D",   # D with stroke (Vietamese,South Slavic) to D
                   ".":"",
                   ",":"",
                   } 

CHANGE = str.maketrans(DIC_CHANGE_CHAR)

# For changing particularly encoded symbols (particular cote to standard cote)
DIC_CHANGE_APOST = {"”": "'",
                    "’": "'",   
                    '"': "'",
                    "“": "'",   
                    "'": "'",   
                  } 

APOSTROPHE_CHANGE = str.maketrans(DIC_CHANGE_APOST)


# For replacing dashes by hyphen-minus
DIC_CHANGE_DASHES = {"‐": "-",   # Non-Breaking Hyphen to hyphen-minus
                     "—": "-",   # En-dash to hyphen-minus
                     "–": "-",   # Em-dash to hyphen-minus
                     "–": "-",
                     }

DASHES_CHANGE = str.maketrans(DIC_CHANGE_DASHES)


# For changing langages specific characters to standard characters
DIC_CHANGE_LANG_CHAR = {"Ł": "L",   # polish capital to L 
                        "ł": "l",   # polish l
                        "ı": "i",    
                        "Đ": "D",   # D with stroke (Vietamese,South Slavic) to D
                        } 
LANG_CHAR_CHANGE = str.maketrans(DIC_CHANGE_LANG_CHAR)


# For droping ponctuation symbols
DIC_CHANGE_PONCT = {".": "",
                    ",": "",
                   }

PONCT_CHANGE = str.maketrans(DIC_CHANGE_PONCT)


# For changing particularly encoded symbols
DIC_CHANGE_SYMB = {"&": "and",
                   "’": "'",   # Particular cote to standard cote
                   ".": "",
                   "-": " ",   # To Do: to be tested from the point of view of the effect on raw institutions
                   "§": " ",
                   "(": " ",
                   ")": " ",
                   "/": " ",
                   "'": " ",   # To Do: to be tested from the point of view of the effect on raw institutions
                  } 

SYMB_CHANGE = str.maketrans(DIC_CHANGE_SYMB)


# For droping particular symbols 
DIC_DROP_SYMB = {"'": " ",
                 "*": " ",
                 "#": " ",
                 "|": " ",
                } 

SYMB_DROP = str.maketrans(DIC_DROP_SYMB)


# Conversion factor for inch to millimeter
IN_TO_MM = 25.4