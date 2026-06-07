"""Module of global parameters for parsing and normalization 
of authors' affiliations.
"""

__all__ = ['AFFIL_COL_NAMES',
           'AFFIL_DEFAULT_FILES_DIC',
           'AFFIL_TYPES_USECOLS',
           'BASIC_KEEPING_WORDS',
           'DIC_TOWN_SYMBOLS',
           'DIC_TOWN_WORDS',
           'DROPPING_WORDS',
           'DROPPING_SUFFIX',
           'EMPTY',
           'FR_DROPPING_WORDS',
           'KEEPING_WORDS',
           'KEEPING_PREFIX',
           'MISSING_SPACE_ACRONYMS',
           'SMALL_WORDS_DROP',
           'USER_KEEPING_WORDS',
          ]


# Local imports
import biblioparsing.regex_globals as bp_rg
from biblioparsing.general_utils import remove_special_symbol


# *********************************************
# * Specific globals for affiliations parsing *
# *********************************************

EMPTY = 'empty'

AFFIL_COL_NAMES = {'norm_affil_col'    : "Norm affiliations",
                   'raw_affil_col_base': "Raw affiliations"}

AFFIL_DEFAULT_FILES_DIC = {'country_towns_file'   : 'Country_towns.xlsx',
                           'country_affils_file'  : 'Country_affiliations.xlsx',
                           'institute_affils_file': 'Institute_affiliations.xlsx',
                           'affil_types_file'     : 'Affiliations_types.xlsx',
                          }

AFFIL_TYPES_USECOLS = ['Level', 'Abbreviation']

# For replacing symbols in town names
DIC_TOWN_SYMBOLS = {"-": " ",
                    "'": " ",
                   }

# For replacing names in town names
DIC_TOWN_WORDS = {" lez " : " les ",
                  "saint ": "st ",
                 }


# For keeping chunks of addresses (without accents and in lower case)
    # Setting a list of keeping words
        # Setting a list of general keeping words
_GEN_KEEPING_WORDS = list(bp_rg.AFFIL_WORD_SUBSTITUTE_PATTERN_DIC.keys())
GEN_KEEPING_WORDS = [remove_special_symbol(x, only_ascii=False, strip=False).lower() for x in _GEN_KEEPING_WORDS]

        # Setting a list of basic keeping words only for country = 'France'
_BASIC_KEEPING_WORDS = ['Beamline', 'CRG', 'EA', 'ED', 'Equipe', 'ULR', 'UMR', 'UMS', 'UPR']
        # Removing accents keeping non adcii characters and converting to lower case the words, by default
BASIC_KEEPING_WORDS = [remove_special_symbol(x, only_ascii=False, strip=False).lower() for x in _BASIC_KEEPING_WORDS]

        # Setting a user list of keeping words
_USER_KEEPING_WORDS = ['CEA', 'CEMHTI', 'CNRS', 'ESRF', 'FEMTO ST', 'IMEC', 'INES', 'INSA',
                       'INSERM', 'IRCELYON', 'KU Leuven', 'LaMCoS', 'LEPMI', 'LETI', 'LITEN',
                       'LOCIE', 'spLine', 'STMicroelectronics', 'TNO', 'UMI', 'VTT']
        # Removing accents keeping non adcii characters and converting to lower case the words, by default
USER_KEEPING_WORDS = [remove_special_symbol(x, only_ascii=False, strip=False).lower() for x in _USER_KEEPING_WORDS]

        # Setting a total list of keeping words
_KEEPING_WORDS = _GEN_KEEPING_WORDS + _BASIC_KEEPING_WORDS + _USER_KEEPING_WORDS
        # Removing accents keeping non adcii characters and converting to lower case the words, by default
KEEPING_WORDS = [remove_special_symbol(x, only_ascii=False, strip=False).lower() for x in _KEEPING_WORDS]


# For keeping chunks of addresses with these prefixes followed by 3 or 4 digits for country France
# only followed by 3 or 4 digits and only for country = 'France'
_KEEPING_PREFIX = ['EA', 'FR', 'U', 'ULR', 'UMR', 'UMS', 'UPR',]
KEEPING_PREFIX = [x.lower() for x in _KEEPING_PREFIX]


# For dropping chunks of addresses (without accents and in lower case)
    # Setting a list of dropping suffixes
_DROPPING_SUFFIX = ["campus", "laan", "park", "platz", "staal", "strae", "strasse", "straße",
                    "vej", "waldring", "weg", "schule", "-ku", "-cho", "-ken", "-shi", "-gun",
                    "alleen", "vagen", "vei", "-gu", "-do", "-si", "shire"]

    # added "ring" but drops chunks containing "Engineering"
    # Removing accents keeping non adcii characters and converting to lower case the dropping suffixes, by default
DROPPING_SUFFIX = [remove_special_symbol(x, only_ascii=False, strip=False).lower() for x in _DROPPING_SUFFIX]


    # Setting a list of dropping words for country different from France
_DROPPING_WORDS = ["alle", "alleen", "area", "avda", "avda.",
                   "bd", "bldg", "box", "bp", "building",
                   "c", "calla", "calle", "camino", "carrera", "carretera", "cesta", "cho",
                   "circuito", "city", "ciudad", "complejo", "corso", "country", "ctra", "cubillos",
                   "district", "edificio", "east", "esplanade", "estrada", "floor", "jardim", "jardins",
                   "km", "ku", "lane", "largo", "linder", "mall", "marg",
                   "p.", "p.le", "p.o.box", "parcella", "passeig", "pk", "playa", "plaza", "parc", "park",
                   "parque", "piazza", "piazzale", "po", "pob", "pola", "pza", "pzza",
                   "rambla", "rd", "rua", "road", "sec.", "sc", "s-n", "s/n", "sp", "st", "st.", "strada",
                   "street", "str", "str.", "tietotie", "vei", "veien", "vej", "via", "viale", "vialle",
                   "voc.", "w", "way", "west", "zona"]

        # Removing accents keeping non adcii characters and converting to lower case the dropping words, by default
_DROPPING_WORDS = [remove_special_symbol(x, only_ascii=False, strip=False).lower() for x in _DROPPING_WORDS]
        # Escaping the regex meta-character "." from the dropping words, by default
_DROPPING_WORDS = [x.replace(".", r"\.") for x in _DROPPING_WORDS]
DROPPING_WORDS = [x.replace("/", r"\/") for x in _DROPPING_WORDS]


        # Setting a list of dropping words for France
_FR_DROPPING_WORDS = ["allee", "antenne", "av", "av.", "ave", "avenue",
                     "ba", "bat", "bat.", "batiment", "blv.", "blvd", "boulevard",
                     "campus", "cedex", "ch.", "chemin", "complexe", "cours", "cs",
                     "domaine", "esplanade", "foret", "immeuble",
                     "montee", "no.", "p", "p°", "parcelle", "parvis", "pl", "pl.", "place", "parc",
                     "plan", "pole", "quai", "r", "r.", "rambla", "region", "route", "rue",
                     "site", "v.", "via", "villa", "voie", "zac", "zi", "z.i.", "zone"]

        # Removing accents keeping non adcii characters and converting to lower case the dropping words, by default
_FR_DROPPING_WORDS = [remove_special_symbol(x, only_ascii=False, strip=False).lower() for x in _FR_DROPPING_WORDS]
        # Escaping the regex meta-character "." from the dropping words, by default
_FR_DROPPING_WORDS = [x.replace(".", r"\.") for x in _FR_DROPPING_WORDS]
FR_DROPPING_WORDS = [x.replace("/", r"\/") for x in _FR_DROPPING_WORDS]


# List of small words to drop in raw affiliations for affiliations normalization
SMALL_WORDS_DROP = ['the', 'and','of', 'for', 'de', 'et', 'la', 'aux', 'a', 'sur', 'pour', 'en', 'l', 'd', 'le']


# List of acronyms for detecting missing space in raw affiliations for affiliations normalization
_MISSING_SPACE_ACRONYMS = ['FR', 'FRE', 'ULR', 'UMR', 'UMS', 'U', 'UPR', 'UR']
MISSING_SPACE_ACRONYMS = [x.lower() for x in _MISSING_SPACE_ACRONYMS]
