"""Module of global parameters for rawdata parsings 
and concatenation/deduplication of parsings.
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
           'PARSING_ITEMS_LIST',
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


# *******************************************
# * Globals specific to Scopus data parsing *
# *******************************************

SCOPUS = 'scopus'

# File names of Scopus data for identification of publications subjects
# These files are available in 'RefFiles' folder of the 'BiblioParsing' package
SCOPUS_CAT_CODES = 'scopus_cat_codes.txt'
SCOPUS_JOURNALS_ISSN_CAT = 'scopus_journals_issn_cat.txt'

# Scopus_rawdata file-name extension
SCOPUS_RAWDATA_EXTENT = 'csv'


# ****************************************
# * Globals specific to WoS data parsing *
# ****************************************

WOS = 'wos'

# Encoding for reading WoS rawdata
ENCODING = 'utf-8'

# Parameter for extending maximum field size for reading WoS rawdata
FIELD_SIZE_LIMIT = 256<<10

# WoS-rawdata file-name extension
WOS_RAWDATA_EXTENT = 'txt'


# ****************************************
# * Specific globals for parsing rawdata *
# ****************************************

# Value for undefined fields in rawdata
UNKNOWN = 'unknown'

# Value for undefined country in affiliations rawdata
UNKNOWN_COUNTRY = 'Unknown'

# Filae name of identifiers of publications to remove from rawdata before parsing
IDS_TO_DROP_FILE_BASE = "_IDs à supprimer.xlsx"

# List of items separatly built from the rawdata parsing
PARSING_ITEMS_LIST = ["articles", "authors", "addresses", "countries",
                      "institutions", "authors_institutions",
                      "authors_keywords", "indexed_keywords", "title_keywords",
                      "subjects", "sub_subjects", "references",
                      "norm_institutions","raw_institutions",]

# Uniformization of document types between rawdata
DIC_DOCTYPE = {'Article'              : ['Article'],
               'Article; early access': ['Article; Early Access'],
               'Book'                 : ['Book'],
               'Book chapter'         : ['Book Chapter','Article; Book Chapter'],
               'Conference paper'     : ['Conference Paper','Proceedings Paper','Article; Proceedings Paper'],
               'Data paper'           : ['Data Paper','Article; Data Paper'],
               'Correction'           : ['Correction'],
               'Editorial material'   : ['Editorial Material','Editorial Material; Book Chapter'],
               'Erratum'              : ['Erratum'],
               'Letter'               : ['Letter'],
               'Meeting Abstract'     : ['Meeting Abstract'],
               'Note'                 : ['Note'],
               'Review'               : ['Review'],
               'Review; early access' : ['Review; Early Access'],
               'Short survey'         : ['Short survey']
              }

# Lower case doc-type dict for normalization of doc-types
LC_DOCTYPE_DIC = {}
for k,v in DIC_DOCTYPE.items():
    LC_DOCTYPE_DIC[k.lower()] = [x.lower() for x in v]

# Uniformization of journal names through identification of low words
DIC_LOW_WORDS = {'proceedings of'        : '',
                 'conference record of'  : '',
                 'proceedings'           : '',
                 'communications'        : '',
                 'conference proceedings': '',
                 'ieee'                  : '',
                 'international'         : 'int',
                 'conference'            : 'conf',
                 'journal of'            : 'j',
                 'transactions on'       : 'trans',
                 'science'               : 'sci',
                 'technology'            : 'tech',
                 'engineering'           : 'eng',
                 '&'                     : 'and',
                 ':'                     : ' ',
                 '-'                     : ' ',
                 ','                     : ' ',
                 '('                     : ' ',
                 ')'                     : ' ',
                 '/'                     : ' ',
                 ';'                     : ' ',
                }

# Uniformization of author's names
# Used for uniformization of first author in references field of WoS rawdata
AUTHORS_SMALL_WORDS = ['de', 'von']

# Value of authors when references field of Scopus rawdata is partially parsed
PARTIAL = 'Partial'


# *************************************************
# * Globals specific to deduplication of parsings *
# *************************************************

# Threshold of strings length for checking similarity
LENGTH_THRESHOLD = 30

# Threshold of true similarity between strings
SIMILARITY_THRESHOLD = 80


# ***********************************************
# * Globals specific to building title keywords *
# ***********************************************

# Help on the nltk tags set using nltk.help.upenn_tagset()
NLTK_VALID_TAG_LIST = ['NN','NNS','VBG','JJ']

# Minimum occurrences of a noun for retaining it
# when building the set of title keywords of a corpus
NOUN_MINIMUM_OCCURRENCES = 3

# ['null','nan'] for  parsing title keywords
BLACKLISTED_WORDS = []
