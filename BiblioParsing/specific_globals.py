"""The BiblioGlobals module defines global parameters used in other BiblioParsing modules.
"""

__all__ = ['AFFIL_COL_NAMES',
           'AFFIL_FILE_BASE_DIC',
           'AUTHORS_SMALL_WORDS',
           'BASIC_KEEPING_WORDS',
           'BLACKLISTED_WORDS',
           'COL_NAMES',
           'COLUMN_LABEL_SCOPUS',
           'COLUMN_LABEL_SCOPUS_PLUS',
           'COLUMN_LABEL_WOS',
           'COLUMN_LABEL_WOS_PLUS',
           'COLUMN_TYPE_SCOPUS',
           'COUNTRY_AFFILIATIONS_FILE',
           'COUNTRY_TOWNS',
           'COUNTRY_TOWNS_FILE',
           'DIC_DOCTYPE',
           'DIC_LOW_WORDS',
           'DIC_TOWN_SYMBOLS',
           'DIC_TOWN_WORDS',
           'DROPPING_WORDS',
           'DROPPING_SUFFIX',
           'EMPTY',
           'ENCODING',
           'FIELD_SIZE_LIMIT',
           'FR_DROPPING_WORDS',
           'IDS_TO_DROP_FILE_BASE',
           'INST_TYPES_FILE',
           'INST_TYPES_USECOLS',
           'INSTITUTE_AFFILIATIONS_FILE',
           'KEEPING_WORDS',
           'KEEPING_PREFIX',
           'LC_DOCTYPE_DIC',
           'LENGTH_THRESHOLD',
           'MISSING_SPACE_ACRONYMS',
           'NLTK_VALID_TAG_LIST',
           'NORM_JOURNAL_COLUMN_LABEL',
           'NOUN_MINIMUM_OCCURRENCES',
           'PARSING_ITEMS_LIST',
           'PARTIAL',
           'SCOPUS',
           'SCOPUS_CAT_CODES',
           'SCOPUS_JOURNALS_ISSN_CAT',
           'SCOPUS_RAWDATA_EXTENT',
           'SIMILARITY_THRESHOLD',
           'SMALL_WORDS_DROP',
           'SYMBOL',
           'UNKNOWN',
           'UNKNOWN_COUNTRY',
           'USECOLS_SCOPUS',
           'USECOLS_WOS',
           'USER_KEEPING_WORDS',
           'WOS',
           'WOS_RAWDATA_EXTENT',
           'XLSX_EXTENT',
          ]


# Local imports
import BiblioParsing.regex_globals as bp_rg
from BiblioParsing.affil_norm_utils import read_towns_per_country
from BiblioParsing.parsing_utils import remove_special_symbol


#####################
# Globals to be set #
#####################

BLACKLISTED_WORDS = [] #['null','nan'] for title keywords


##################
# Shared globals #
##################

XLSX_EXTENT = "xlsx"

################
# Column names #
################

# Particular column names
NORM_JOURNAL_COLUMN_LABEL = 'Norm_journal'
AFFIL_COL_NAMES = {'norm_affil_col'    : "Norm affiliations",
                   'raw_affil_col_base': "Raw affiliations"}

# Column names common to column names dicts
PUB_ID      = 'Pub_id'
AUTHOR_IDX  = 'Idx_author'
ADDRESS_IDX = 'Idx_address'
ADDRESS     = 'Address'
COUNTRY     = 'Country'
JOURNAL     = 'Journal'
YEAR        = 'Year'
DOI         = 'DOI'
TITLE       = 'Title'

# Column names dicts
COL_NAMES = {'pub_id'      : PUB_ID,
             'wos_id'      : ['WoS_id',
                              PUB_ID],
             'scopus_id'   : ['Scopus_id',
                              PUB_ID,],
             'address'     : [PUB_ID,
                              ADDRESS_IDX,
                              ADDRESS,],
             'address_inst': [PUB_ID,
                              ADDRESS_IDX,
                              ADDRESS,
                              COUNTRY,
                              'Norm_institutions',
                              'Unknown_institutions',],
             'articles'    : [PUB_ID,
                              'Authors',
                              YEAR,
                              JOURNAL,
                              'Volume',
                              'Page',
                              DOI,
                              'Document_type',
                              'Language',
                              TITLE,
                              'ISSN',],
             'authors'     : [PUB_ID,
                              AUTHOR_IDX,
                              'Co_author',],
             'auth_inst'   : [PUB_ID,
                              AUTHOR_IDX,
                              ADDRESS,
                              COUNTRY,
                              'Norm_institutions',
                              'Raw_institutions',
                              'Secondary_institutions',],
             'country'     : [PUB_ID,
                              ADDRESS_IDX,
                              COUNTRY,],
             'institution' : [PUB_ID,
                              ADDRESS_IDX,
                              'Institution',],
             'keywords'    : [PUB_ID,
                              'Keyword',],
             'references'  : [PUB_ID,
                              'Authors',
                              YEAR,
                              JOURNAL,
                              DOI,
                              TITLE,
                              'Full_reference',],
             'subject'     : [PUB_ID,
                              'Subject',],
             'sub_subject' : [PUB_ID,
                              'Sub_subject',],
             'temp_col'    : ['Title_LC',
                              'Dedup_Same_Journal',
                              TITLE,
                              'title_tokens',
                              'kept_tokens',
                              'doc_type_lc',
                              'doi_lc',],
            }


COLUMN_LABEL_SCOPUS = {'affiliations'             : 'Affiliations',
                       'author_keywords'          : 'Author Keywords',
                       'authors'                  : 'Authors',
                       'authors_with_affiliations': 'Authors with affiliations',
                       'document_type'            : 'Document Type',
                       'doi'                      : 'DOI',
                       'index_keywords'           : 'Index Keywords' ,
                       'issn'                     : 'ISSN',
                       'journal'                  : 'Source title',
                       'language'                 : 'Language of Original Document',
                       'page_start'               : 'Page start' ,
                       'references'               : 'References' ,
                       'sub_subjects'             : '',
                       'subjects'                 : '',
                       'title'                    : 'Title' ,
                       'volume'                   : 'Volume',
                       'year'                     : 'Year',
                       }


COLUMN_LABEL_SCOPUS_PLUS = {'scopus_id'     : 'EID',
                            'auth_fullnames': 'Author full names',
                           }


COLUMN_TYPE_SCOPUS = {COLUMN_LABEL_SCOPUS['affiliations']             : str,
                      COLUMN_LABEL_SCOPUS['author_keywords']          : str,
                      COLUMN_LABEL_SCOPUS['authors']                  : str,
                      COLUMN_LABEL_SCOPUS['authors_with_affiliations']: str,
                      COLUMN_LABEL_SCOPUS['document_type']            : str,
                      COLUMN_LABEL_SCOPUS['doi']                      : str,
                      COLUMN_LABEL_SCOPUS['index_keywords']           : str,
                      COLUMN_LABEL_SCOPUS['issn']                     : str,
                      COLUMN_LABEL_SCOPUS['journal']                  : str,
                      COLUMN_LABEL_SCOPUS['language']                 : str,
                      COLUMN_LABEL_SCOPUS['page_start']               : str,
                      COLUMN_LABEL_SCOPUS['references']               : str,
                      COLUMN_LABEL_SCOPUS['sub_subjects']             : str,
                      COLUMN_LABEL_SCOPUS['subjects']                 : str,
                      COLUMN_LABEL_SCOPUS['title']                    : str,
                      COLUMN_LABEL_SCOPUS['volume']                   : str,
                      COLUMN_LABEL_SCOPUS['year']                     : int,
                     }


COLUMN_LABEL_WOS = {'affiliations'             : '',
                    'author_keywords'          : 'DE',
                    'authors'                  : 'AU',
                    'authors_fullnames'        : 'AF',
                    'authors_with_affiliations': 'C1',
                    'document_type'            : 'DT',
                    'doi'                      : 'DI',
                    'index_keywords'           : 'ID',
                    'issn'                     : 'SN',
                    'journal'                  : 'SO',
                    'language'                 : 'LA',
                    'page_start'               : 'BP',
                    'references'               : 'CR',
                    'subjects'                 : 'WC',
                    'sub_subjects'             : 'SC',
                    'title'                    : 'TI',
                    'volume'                   : 'VL',
                    'year'                     : 'PY' ,
                    }


COLUMN_LABEL_WOS_PLUS = {'e_issn'              : 'EI',
                         'wos_id'              : 'UT',
                        }


###############################
# Specific globals to parsing #
###############################

PARSING_ITEMS_LIST = ["articles", "authors", "addresses", "countries",
                      "institutions", "authors_institutions",
                      "authors_keywords", "indexed_keywords", "title_keywords",
                      "subjects", "sub_subjects", "references",
                      "norm_institutions","raw_institutions",]

# For uniformization of document types
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

# Setting lower case doc-type dict for normalization of doc-types
LC_DIC_DOCTYPE_KEYS = [k.lower() for k in DIC_DOCTYPE.keys()]
LC_DIC_DOCTYPE_VALUES = [[x.lower() for x in v] for v in DIC_DOCTYPE.values()]
LC_DOCTYPE_DIC = dict(zip(LC_DIC_DOCTYPE_KEYS, LC_DIC_DOCTYPE_VALUES))

# For uniformization of journal names
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
                 '&'                     : 'and',                # & to and
                 ':'                     : ' ',                  # colon to space
                 '-'                     : ' ',                  # hyphen-minus to space
                 ','                     : ' ',                  # comma to space
                 '('                     : ' ',                  # parenthese to space
                 ')'                     : ' ',                  # parenthese to space
                 '/'                     : ' ',                  # slash to space
                 ';'                     : ' ',
                }


# Thresholds
LENGTH_THRESHOLD = 30
SIMILARITY_THRESHOLD = 80

# General parsing globals
NLTK_VALID_TAG_LIST = ['NN','NNS','VBG','JJ'] # you can find help on the nltk tags set
                                              # using nltk.help.upenn_tagset()

NOUN_MINIMUM_OCCURRENCES = 3 # Minimum occurrences of a noun to be retained when
                             # building the set of title keywords see "build_title_keywords" function


AUTHORS_SMALL_WORDS = ['de', 'von']

SYMBOL  = '\s,;:.\-\/'
PARTIAL = 'partial'    # For unparsed partial references
EMPTY   = 'empty'
UNKNOWN = 'unknown'
UNKNOWN_COUNTRY = 'Unknown'

IDS_TO_DROP_FILE_BASE = "_IDs à supprimer.xlsx"


#######################################
# Globals specific to Scopus database #
#######################################

SCOPUS = 'scopus'
SCOPUS_CAT_CODES = 'scopus_cat_codes.txt'
SCOPUS_JOURNALS_ISSN_CAT = 'scopus_journals_issn_cat.txt'
SCOPUS_RAWDATA_EXTENT = 'csv'

# This global is used in merge_database function
_USECOLS_SCOPUS = '''Abstract,Affiliations,Authors,Author Keywords,Authors with affiliations,
                     CODEN,Document Type,DOI,EID,Index Keywords,ISBN,ISSN,Issue,Language of Original Document,
                     Page start,References,Source title,Title,Volume,Year'''
USECOLS_SCOPUS  = [x.strip() for x in _USECOLS_SCOPUS.split(',')]


####################################
# Globals specific to WOS database #
####################################
WOS = 'wos'
ENCODING = 'utf-8' # 'iso-8859-1' # encoding used by the function read_database_wos
FIELD_SIZE_LIMIT = 256<<10 # extend maximum field size for wos file reading
WOS_RAWDATA_EXTENT = 'txt'

# To Do: Check if this global is still used
_USECOLS_WOS ='''AB,AU,BP,BS,C1,CR,DE,DI,DT,ID,IS,LA,PY,RP,
                SC,SN,SO,TI,UT,VL,WC'''
USECOLS_WOS  = [x.strip() for x in _USECOLS_WOS.split(',')]


#############################################
# Specific globals for institutions parsing #
#############################################

# For replacing symbols in town names
DIC_TOWN_SYMBOLS = {"-": " ",
                    "'": " ",
                   }

# For replacing names in town names
DIC_TOWN_WORDS = {" lez " : " les ",
                  "saint ": "st ",
                 }


# ToDo: Check the use of this globals by their own
# Setting the file name of the file for dropping towns in addresses
COUNTRY_TOWNS_FILE = 'Country_towns.xlsx'

# Setting the file name of the file gathering de normalized affiliations with their raw affiliations per country
COUNTRY_AFFILIATIONS_FILE = 'Country_affiliations.xlsx'

# Setting the file name of the file gathering de normalized affiliations with their raw affiliations per country
INSTITUTE_AFFILIATIONS_FILE = "Institute_affiliations.xlsx"

# Setting the file name for the file of institutions types description and order level with the useful columns
INST_TYPES_FILE = "Institutions_types.xlsx"

AFFIL_FILE_BASE_DIC = {'root'                 : 'Traitement Institutions',
                       'country_towns_file'   : 'Country_towns.xlsx',
                       'country_affils_file'  : 'Country_affiliations.xlsx',
                       'institute_affils_file': 'Institute_affiliations.xlsx',
                       'affil_types_file'     : 'Institutions_types.xlsx',
                      }


COUNTRY_TOWNS = read_towns_per_country(country_towns_file=None, country_towns_folder_path=None)

INST_TYPES_USECOLS = ['Level', 'Abbreviation']


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
_USER_KEEPING_WORDS = ['CEA', 'CEMHTI', 'CNRS', 'ESRF', 'FEMTO ST', 'IMEC', 'INES', 'INSA', 'INSERM', 'IRCELYON',
                       'KU Leuven', 'LaMCoS', 'LEPMI', 'LETI', 'LITEN', 'LOCIE', 'spLine', 'STMicroelectronics', 'TNO', 'UMI', 'VTT']
        # Removing accents keeping non adcii characters and converting to lower case the words, by default
USER_KEEPING_WORDS = [remove_special_symbol(x, only_ascii=False, strip=False).lower() for x in _USER_KEEPING_WORDS]

        # Setting a total list of keeping words
_KEEPING_WORDS = _GEN_KEEPING_WORDS + _BASIC_KEEPING_WORDS + _USER_KEEPING_WORDS
        # Removing accents keeping non adcii characters and converting to lower case the words, by default
KEEPING_WORDS = [remove_special_symbol(x, only_ascii=False, strip=False).lower() for x in _KEEPING_WORDS]


# For keeping chunks of addresses with these prefixes followed by 3 or 4 digits for country France
_KEEPING_PREFIX = ['EA', 'FR', 'U', 'ULR', 'UMR', 'UMS', 'UPR',] # only followed by 3 or 4 digits and only for country = 'France'
KEEPING_PREFIX = [x.lower() for x in _KEEPING_PREFIX]


# For dropping chunks of addresses (without accents and in lower case)
    # Setting a list of dropping suffixes
_DROPPING_SUFFIX = ["campus", "laan", "park", "platz", "staal", "strae", "strasse", "straße", "vej", "waldring", "weg",
                    "schule", "-ku", "-cho", "-ken", "-shi", "-gun", "alleen", "vagen", "vei", "-gu", "-do", "-si", "shire"]

        # added "ring" but drops chunks containing "Engineering"
        # Removing accents keeping non adcii characters and converting to lower case the dropping suffixes, by default
DROPPING_SUFFIX = [remove_special_symbol(x, only_ascii=False, strip=False).lower() for x in _DROPPING_SUFFIX]


    # Setting a list of dropping words for country different from France
_DROPPING_WORDS = ["alle", "alleen", "area", "avda", "avda.",
                   "bd", "bldg", "box", "bp", "building",
                   "c", "calla", "calle", "camino", "carrera", "carretera", "cesta", "cho",
                   "circuito", "city", "ciudad", "complejo", "corso", "country", "ctra", "cubillos",
                   "district", "edificio", "east", "esplanade", "estrada", "floor", "jardim", "jardins", "km", "ku",
                   "lane", "largo", "linder", "mall", "marg",
                   "p.", "p.le", "p.o.box", "parcella", "passeig", "pk", "playa", "plaza", "parc", "park",
                   "parque", "piazza", "piazzale", "po", "pob", "pola", "pza", "pzza",
                   "rambla", "rd", "rua", "road", "sec.", "sc", "s-n", "s/n", "sp", "st", "st.", "strada", "street", "str", "str.",
                   "tietotie", "vei", "veien", "vej", "via", "viale", "vialle", "voc.", "w", "way", "west", "zona"]

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
