'''The BiblioGlobals module defines global parameters 
   used in other BiblioParsing modules.
   modidied 04/12/2023

'''

__all__ = ['BASIC_KEEPING_WORDS',
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
           'DIC_AMB_WORDS',
           'DIC_LOW_WORDS',
           'DIC_TOWN_SYMBOLS',
           'DIC_TOWN_WORDS',
           'DIC_WORD_RE_PATTERN',
           'DROPING_WORDS',
           'DROPING_SUFFIX',
           'EMPTY',
           'ENCODING',
           'FIELD_SIZE_LIMIT',
           'FR_DROPING_WORDS',
           'INST_TYPES_FILE',
           'INST_TYPES_USECOLS',
           'INSTITUTE_AFFILIATIONS_FILE',
           'KEEPING_WORDS',
           'KEEPING_PREFIX',
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
          ]

# Local imports 
from BiblioParsing.BiblioGeneralGlobals import REP_UTILS
from BiblioParsing.BiblioParsingInstitutions import read_towns_per_country
from BiblioParsing.BiblioParsingUtils import remove_special_symbol


#####################
# Globals to be set #
#####################

BLACKLISTED_WORDS = [] #['null','nan'] for title keywords


################
# Column names #
################

# Particular column names
NORM_JOURNAL_COLUMN_LABEL = 'Norm_journal'

# Column names common to column names dicts 
pub_id      = 'Pub_id'
idx_author  = 'Idx_author'
idx_address = 'Idx_address'
address     = 'Address'
country     = 'Country'
journal     = 'Journal'
year        = 'Year'
volume      = 'Volume'
page        = 'Page'

# Column names dicts
COL_NAMES = {'pub_id'      : pub_id,
             'wos_id'      : ['WoS_id',
                              pub_id],
             'scopus_id'   : ['Scopus_id',
                              pub_id,],
             'address'     : [pub_id,
                              idx_address,
                              address,],
             'address_inst': [pub_id,
                              idx_address,
                              address,
                              country,
                              'Norm_institutions',
                              'Unknown_institutions',],
             'articles'    : [pub_id,
                              'Authors',
                              year,
                              journal,
                              volume,
                              page,
                              'DOI',
                              'Document_type',
                              'Language',
                              'Title',
                              'ISSN',],
             'authors'     : [pub_id,
                              idx_author,
                              'Co_author',],  
             'auth_inst'   : [pub_id,
                              idx_author,
                              address,
                              country,
                              'Norm_institutions',
                              'Raw_institutions',
                              'Secondary_institutions',], 
             'country'     : [pub_id,
                              idx_address,
                              country,],
             'institution' : [pub_id,
                              idx_address,
                              'Institution',],                             
             'keywords'    : [pub_id,
                              'Keyword',],                             
             'references'  : [pub_id,
                              'Author',
                              year,                             
                              journal,
                              volume,
                              page,],
             'subject'     : [pub_id,
                              'Subject',],
             'sub_subject' : [pub_id,
                              'Sub_subject',],
             'temp_col'    : ['Title_LC', 
                              'Dedup_Same_Journal',
                              'Title',
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
                            'auth_fullnames':'Author full names',
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
                    'sub_subjects'             : 'SC',
                    'subjects'                 : 'WC',
                    'title'                    : 'TI',
                    'volume'                   : 'VL',
                    'year'                     : 'PY' ,
                    }


COLUMN_LABEL_WOS_PLUS = {'e_issn'              : 'EI',
                         'wos_id'              : 'UT',
                        }


###############################
# Globals specific to parsing #
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

SYMBOL  = '\s,;:.\-\/'
PARTIAL = 'partial'    # For unparsed partial references
EMPTY   = 'empty'
UNKNOWN = 'unknown'
UNKNOWN_COUNTRY = 'Unknown'


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
ENCODING = 'iso-8859-1' # encoding used by the function read_database_wos
FIELD_SIZE_LIMIT = 256<<10 # extend maximum field size for wos file reading
WOS_RAWDATA_EXTENT = 'txt'

# To Do: Check if this global is still used
_USECOLS_WOS ='''AB,AU,BP,BS,C1,CR,DE,DI,DT,ID,IS,LA,PY,RP,
                SC,SN,SO,TI,UT,VL,WC'''
USECOLS_WOS  = [x.strip() for x in _USECOLS_WOS.split(',')]


#################
# Built globals #
#################

# For replacing symbols in town names
DIC_TOWN_SYMBOLS = {"-": " ",
                    "'": " ",
                   }

# For replacing names in town names
DIC_TOWN_WORDS = {" lez " : " les ",
                  "saint ": "st ",
                 }

# Setting the file name of the file for droping towns in addresses
COUNTRY_TOWNS_FILE = 'Country_towns.xlsx'

COUNTRY_TOWNS = read_towns_per_country(country_towns_file = None, country_towns_folder_path = None)


#############################################
# Specific globals for institutions parsing #
#############################################

# Standard library imports
import re

# Local library imports 
from BiblioParsing.BiblioParsingUtils import remove_special_symbol

# Setting the file name of the file gathering de normalized affiliations with their raw affiliations per country
COUNTRY_AFFILIATIONS_FILE = 'Country_affiliations.xlsx'

# Setting the file name of the file gathering de normalized affiliations with their raw affiliations per country
INSTITUTE_AFFILIATIONS_FILE = "Institute_affiliations.xlsx"

# Setting the file name for the file of institutions types description and order level with the useful columns
INST_TYPES_FILE    = "Institutions_types.xlsx"                                                                                         
INST_TYPES_USECOLS = ['Level', 'Abbreviation']

# Potentialy ambiguous words in institutions names
DIC_AMB_WORDS = {' des ': ' ', # Conflict with DES institution
                 ' @ ': ' ', # Management conflict with '@' between texts
                }


# For replacing aliases of a word by a word (case sensitive)
DIC_WORD_RE_PATTERN = {}
DIC_WORD_RE_PATTERN['University'] = re.compile(r'\b[a-z]?Univ[aàäcdeéirstyz]{0,8}\b\.?')
DIC_WORD_RE_PATTERN['Laboratory'] = re.compile(  r"'?\bLab\b\.?" \
                                               +  "|" \
                                               + r"'?\bLabor[aeimorstuy]{0,7}\b\.?")
DIC_WORD_RE_PATTERN['Center']     = re.compile(r"\b[CZ]ent[erum]{1,3}\b\.?")
DIC_WORD_RE_PATTERN['Department'] = re.compile(r"\bD[eé]{1}p[artemnot]{0,9}\b\.?")
DIC_WORD_RE_PATTERN['Institute']  = re.compile( r"\bInst[ituteosky]{0,7}\b\.?" \
                                               + "|" \
                                               + r"\bIstituto\b") 
DIC_WORD_RE_PATTERN['Faculty']    = re.compile(r"\bFac[lutey]{0,4}\b\.?")
DIC_WORD_RE_PATTERN['School']     = re.compile(r"\bSch[ol]{0,3}\b\.?")


# For keeping chunks of addresses (without accents and in lower case)
    # Setting a list of keeping words
        # Setting a list of general keeping words
_GEN_KEEPING_WORDS = list(DIC_WORD_RE_PATTERN.keys())
GEN_KEEPING_WORDS  = [remove_special_symbol(x, only_ascii = False, 
                                            strip = False).lower() for x in _GEN_KEEPING_WORDS]

        # Setting a list of basic keeping words only for country = 'France'
_BASIC_KEEPING_WORDS = ['Beamline', 'CRG', 'EA', 'ED', 'Equipe', 'ULR', 'UMR', 'UMS', 'UPR']
        # Removing accents keeping non adcii characters and converting to lower case the words, by default
BASIC_KEEPING_WORDS  = [remove_special_symbol(x, only_ascii = False, 
                                              strip = False).lower() for x in _BASIC_KEEPING_WORDS]

        # Setting a user list of keeping words
_USER_KEEPING_WORDS  = ['CEA', 'CEMHTI', 'CNRS', 'ESRF', 'FEMTO ST', 'IMEC', 'INES', 'INSA', 'INSERM', 'IRCELYON', 
                        'KU Leuven', 'LaMCoS', 'LEPMI', 'LITEN', 'LOCIE', 'spLine', 'STMicroelectronics', 'TNO', 'UMI', 'VTT']
        # Removing accents keeping non adcii characters and converting to lower case the words, by default
USER_KEEPING_WORDS   = [remove_special_symbol(x, only_ascii = False, 
                                              strip = False).lower() for x in _USER_KEEPING_WORDS]

        # Setting a total list of keeping words
_KEEPING_WORDS = _GEN_KEEPING_WORDS + _BASIC_KEEPING_WORDS + _USER_KEEPING_WORDS
        # Removing accents keeping non adcii characters and converting to lower case the words, by default
KEEPING_WORDS  =[remove_special_symbol(x, only_ascii = False, 
                                       strip = False).lower() for x in _KEEPING_WORDS]


# For keeping chunks of addresses with these prefixes followed by 3 or 4 digits for country France
_KEEPING_PREFIX = ['EA', 'FR', 'U', 'ULR', 'UMR', 'UMS', 'UPR',] # only followed by 3 or 4 digits and only for country = 'France'
KEEPING_PREFIX  = [x.lower() for x in _KEEPING_PREFIX]


# For droping chunks of addresses (without accents and in lower case)
    # Setting a list of droping suffixes
_DROPING_SUFFIX = ["campus", "laan", "park", "platz", "staal", "strae", "strasse", "straße", "vej", "waldring", "weg",
                   "schule", "-ku", "-cho", "-ken", "-shi", "-gun", "alleen", "vagen", "vei", "-gu", "-do", "-si", "shire"] 

        # added "ring" but drops chunks containing "Engineering"
        # Removing accents keeping non adcii characters and converting to lower case the droping suffixes, by default
DROPING_SUFFIX = [remove_special_symbol(x, only_ascii = False, 
                                        strip = False).lower() for x in _DROPING_SUFFIX]


    # Setting a list of droping words for country different from France
_DROPING_WORDS = ["alle", "alleen", "area", "avda", "avda.",  
                  "bd", "bldg", "box", "bp", "building",
                  "c", "calla", "calle", "camino", "carrera", "carretera", "cesta", "cho",
                  "circuito", "city", "ciudad", "complejo", "corso", "country", "ctra", "cubillos",  
                  "district", "edificio", "east", "esplanade", "estrada", "floor", "jardim", "jardins", "km", "ku",
                  "lane", "largo", "linder", "mall", "marg",
                  "p.", "p.le", "p.o.box", "parcella", "passeig", "pk", "playa", "plaza", "parc", "park", 
                  "parque", "piazza", "piazzale", "po", "pob", "pola", "pza", "pzza",
                  "rambla", "rd", "rua", "road", "sec.", "sc", "s-n", "s/n", "sp", "st", "st.", "strada", "street", "str", "str.",
                  "tietotie", "vei", "veien", "vej", "via", "viale", "vialle", "voc.", "w", "way", "west", "zona"]

        # Removing accents keeping non adcii characters and converting to lower case the droping words, by default
_DROPING_WORDS = [remove_special_symbol(x, only_ascii = False, 
                                        strip = False).lower() for x in _DROPING_WORDS]
        # Escaping the regex meta-character "." from the droping words, by default
_DROPING_WORDS = [x.replace(".", r"\.") for x in _DROPING_WORDS]
DROPING_WORDS  = [x.replace("/", r"\/") for x in _DROPING_WORDS]


        # Setting a list of droping words for France
_FR_DROPING_WORDS = ["allee", "antenne", "av", "av.", "ave", "avenue", 
                     "ba", "bat", "bat.", "batiment", "blv.", "blvd", "boulevard",
                     "campus", "cedex", "ch.", "chemin", "complexe", "cours", "cs",
                     "domaine", "esplanade", "foret", "immeuble", 
                     "montee", "no.", "p", "p°", "parcelle", "parvis", "pl", "pl.", "place", "parc",
                     "plan", "pole", "quai", "r", "r.", "rambla", "region", "route", "rue",
                     "site", "v.", "via", "villa", "voie", "zac", "zi", "z.i.", "zone"]

        # Removing accents keeping non adcii characters and converting to lower case the droping words, by default
_FR_DROPING_WORDS = [remove_special_symbol(x, only_ascii = False, 
                                           strip = False).lower() for x in _FR_DROPING_WORDS]
        # Escaping the regex meta-character "." from the droping words, by default
_FR_DROPING_WORDS = [x.replace(".", r"\.") for x in _FR_DROPING_WORDS]
FR_DROPING_WORDS  = [x.replace("/", r"\/") for x in _FR_DROPING_WORDS]


# List of small words to drop in raw affiliations for affiliations normalization 
SMALL_WORDS_DROP = ['the', 'and','of', 'for', 'de', 'et', 'la', 'aux', 'a', 'sur', 'pour', 'en', 'l', 'd', 'le']


# List of acronyms for detecting missing space in raw affiliations for affiliations normalization 
_MISSING_SPACE_ACRONYMS = ['FR', 'FRE', 'ULR', 'UMR', 'UMS', 'U', 'UPR', 'UR']
MISSING_SPACE_ACRONYMS  = [x.lower() for x in _MISSING_SPACE_ACRONYMS]



    


