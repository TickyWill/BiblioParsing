"""Module of global parameters for rawdata parsings 
and concatenation/deduplication of parsings.

The parameters values are set from the 'parsing_globals.yaml' file 
available by default in the 'DemoConfig' folder of the package.
"""


__all__ = ['AUTHORS_SMALL_WORDS',
           'BLACKLISTED_WORDS',
           'DIC_DOCTYPE',
           'DIC_LOW_WORDS',
           'ENCODING',
           'FIELD_SIZE_LIMIT',
           'IDS_TO_DROP_FILE_BASE',
           'LC_DOCTYPE_DIC',
           'LENGTH_THRESHOLD',
           'NLTK_VALID_TAG_LIST',
           'NOUN_MINIMUM_OCCURRENCES',
#           'PARSING_ITEMS_LIST',
           'PARTIAL',
           'SCOPUS',
           'SCOPUS_CAT_CODES',
           'SCOPUS_JOURNALS_ISSN_CAT',
           'SCOPUS_RAWDATA_EXTENT',
           'SIMILARITY_THRESHOLD',
           'UNKNOWN',
           'UNKNOWN_COUNTRY',
           'WOS',
           'WOS_RAWDATA_EXTENT',
          ]


# Local imports
from bpfuncts.globals_utils import read_yaml_parsing_globals


# Getting the globals values from the YAML file of parsing globals
parsing_globals_dic = read_yaml_parsing_globals()

# *******************************************
# * Globals specific to Scopus data parsing *
# *******************************************

SCOPUS = parsing_globals_dic['scopus']

# File names of Scopus data for identification of publications subjects
# These files are available in 'RefFiles' folder of the 'BiblioParsing' package
SCOPUS_CAT_CODES = parsing_globals_dic['scopus_cat_codes']
SCOPUS_JOURNALS_ISSN_CAT = parsing_globals_dic['scopus_journals_issn_cat']

# Scopus_rawdata file-name extension
SCOPUS_RAWDATA_EXTENT = parsing_globals_dic['scopus_rawdata_extent']

# Value of authors when references field of Scopus rawdata is partially parsed
PARTIAL = parsing_globals_dic['partial']


# ****************************************
# * Globals specific to WoS data parsing *
# ****************************************

WOS = parsing_globals_dic['wos']

# Encoding for reading WoS rawdata
ENCODING = parsing_globals_dic['encoding']

# Parameter for extending maximum field size for reading WoS rawdata
FIELD_SIZE_LIMIT = parsing_globals_dic['field_size_limit']

# WoS-rawdata file-name extension
WOS_RAWDATA_EXTENT = parsing_globals_dic['wos_rawdata_extent']

# Uniformization of author's names
# Used for uniformization of first author in references field of WoS rawdata
AUTHORS_SMALL_WORDS = parsing_globals_dic['authors_smal_words']


# ****************************************
# * Specific globals for parsing rawdata *
# ****************************************

# Value for undefined fields in rawdata
UNKNOWN = parsing_globals_dic['unknown']

# Value for undefined country in affiliations rawdata
UNKNOWN_COUNTRY = parsing_globals_dic['unknown_country']

# File name of identifiers of publications to remove from rawdata before parsing
IDS_TO_DROP_FILE_BASE = parsing_globals_dic['ids_to_drop_file_base']

# List of items separatly built from the rawdata parsing
#PARSING_ITEMS_LIST = parsing_globals_dic['parsing_items_list']

# Uniformization of document types between rawdata
DIC_DOCTYPE = parsing_globals_dic['dic_doctype']

# Lower case doc-type dict for normalization of doc-types
LC_DOCTYPE_DIC = {}
for k,v in DIC_DOCTYPE.items():
    LC_DOCTYPE_DIC[k.lower()] = [x.lower() for x in v]

# Uniformization of journal names through identification of low words
DIC_LOW_WORDS = parsing_globals_dic['dic_low_words']


# *************************************************
# * Globals specific to deduplication of parsings *
# *************************************************

# Threshold of strings length for checking similarity
LENGTH_THRESHOLD = parsing_globals_dic['length_threshold']

# Threshold of true similarity between strings
SIMILARITY_THRESHOLD = parsing_globals_dic['similarity_threshold']


# ***********************************************
# * Globals specific to building title keywords *
# ***********************************************

# Help on the nltk tags set using nltk.help.upenn_tagset()
NLTK_VALID_TAG_LIST = parsing_globals_dic['nltk_valid_tag_list']

# Minimum occurrences of a noun for retaining it
# when building the set of title keywords of a corpus
NOUN_MINIMUM_OCCURRENCES = parsing_globals_dic['noun_minimum_occurrences']

# ['null','nan'] for  parsing title keywords
BLACKLISTED_WORDS = parsing_globals_dic['blacklisted_words']
