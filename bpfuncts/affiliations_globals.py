"""Module of global parameters for parsing and normalization 
of authors' affiliations.

The parameters values are set from the 'affiliations_globals.yaml' file 
available by default in the 'DemoConfig' folder of the package.
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
import bpfuncts.regex_globals as bp_rg
from bpfuncts.general_utils import remove_special_symbol
from bpfuncts.globals_utils import read_yaml_affiliations_globals


# Getting the globals values from the YAML file of parsing globals
affiliations_globals_dic = read_yaml_affiliations_globals()


# *********************************************
# * Specific globals for affiliations parsing *
# *********************************************

# Word to indicate that raw affiliations are all normalized for a given address
EMPTY = affiliations_globals_dic['empty']

# Column names of affiliations
AFFIL_COL_NAMES = affiliations_globals_dic['affil_col_names']

# File names of data for affiliations normalization
AFFIL_DEFAULT_FILES_DIC = affiliations_globals_dic['affil_default_files_dic']

# Column names of useful data setting affiliations types
AFFIL_TYPES_USECOLS = affiliations_globals_dic['affil_types_usecols']

# Dict for replacing symbols in town names
DIC_TOWN_SYMBOLS = affiliations_globals_dic['dic_town_symbols']

# Dict for replacing specific words in town names
DIC_TOWN_WORDS = affiliations_globals_dic['dic_town_words']

# For keeping chunks of addresses (without accents and in lower case)
    # Setting a list of keeping words
        # Setting a list of general keeping words
_GEN_KEEPING_WORDS = list(bp_rg.AFFIL_WORD_SUBSTITUTE_PATTERN_DIC.keys())
GEN_KEEPING_WORDS = [remove_special_symbol(x, only_ascii=False, strip=False).lower() for x in _GEN_KEEPING_WORDS]

        # Setting a list of basic keeping words only for country = 'France'
_BASIC_KEEPING_WORDS = affiliations_globals_dic['basic_keeping_words']
        # Removing accents keeping non adcii characters and converting to lower case the words, by default
BASIC_KEEPING_WORDS = [remove_special_symbol(x, only_ascii=False, strip=False).lower() for x in _BASIC_KEEPING_WORDS]

        # Setting a user list of keeping words
_USER_KEEPING_WORDS = affiliations_globals_dic['user_keeping_words']
        # Removing accents keeping non adcii characters and converting to lower case the words, by default
USER_KEEPING_WORDS = [remove_special_symbol(x, only_ascii=False, strip=False).lower() for x in _USER_KEEPING_WORDS]

        # Setting a total list of keeping words
_KEEPING_WORDS = _GEN_KEEPING_WORDS + _BASIC_KEEPING_WORDS + _USER_KEEPING_WORDS
        # Removing accents keeping non adcii characters and converting to lower case the words, by default
KEEPING_WORDS = [remove_special_symbol(x, only_ascii=False, strip=False).lower() for x in _KEEPING_WORDS]

# For keeping chunks of addresses with these prefixes followed by 3 or 4 digits for country France
# only followed by 3 or 4 digits and only for country = 'France'
_KEEPING_PREFIX = affiliations_globals_dic['keeping_prefix']
KEEPING_PREFIX = [x.lower() for x in _KEEPING_PREFIX]

# For dropping chunks of addresses (without accents and in lower case)
    # Setting a list of dropping suffixes
_DROPPING_SUFFIX = affiliations_globals_dic['dropping_suffix']

    # added "ring" but drops chunks containing "Engineering"
    # Removing accents keeping non adcii characters and converting to lower case the dropping suffixes, by default
DROPPING_SUFFIX = [remove_special_symbol(x, only_ascii=False, strip=False).lower() for x in _DROPPING_SUFFIX]

    # Setting a list of dropping words for country different from France
_DROPPING_WORDS = affiliations_globals_dic['dropping_words']

        # Removing accents keeping non adcii characters and converting to lower case the dropping words, by default
_DROPPING_WORDS = [remove_special_symbol(x, only_ascii=False, strip=False).lower() for x in _DROPPING_WORDS]
        # Escaping the regex meta-character "." from the dropping words, by default
_DROPPING_WORDS = [x.replace(".", r"\.") for x in _DROPPING_WORDS]
DROPPING_WORDS = [x.replace("/", r"\/") for x in _DROPPING_WORDS]

        # Setting a list of dropping words for France
_FR_DROPPING_WORDS = affiliations_globals_dic['fr_dropping_words']

        # Removing accents keeping non adcii characters and converting to lower case the dropping words, by default
_FR_DROPPING_WORDS = [remove_special_symbol(x, only_ascii=False, strip=False).lower() for x in _FR_DROPPING_WORDS]
        # Escaping the regex meta-character "." from the dropping words, by default
_FR_DROPPING_WORDS = [x.replace(".", r"\.") for x in _FR_DROPPING_WORDS]
FR_DROPPING_WORDS = [x.replace("/", r"\/") for x in _FR_DROPPING_WORDS]

# List of small words to drop in raw affiliations for affiliations normalization
SMALL_WORDS_DROP = affiliations_globals_dic['small_words_drop']

# List of acronyms for detecting missing space in raw affiliations for affiliations normalization
_MISSING_SPACE_ACRONYMS = affiliations_globals_dic['missing_space_acronyms']
MISSING_SPACE_ACRONYMS = [x.lower() for x in _MISSING_SPACE_ACRONYMS]
