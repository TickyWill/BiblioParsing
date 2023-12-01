'''The BiblioGlobals module defines global parameters 
   used in other BiblioAnalysis modules.

'''

__all__ = ['BASIC_KEEPING_WORDS',
           'BLACKLISTED_WORDS',
           'COL_NAMES',
           'COLUMN_LABEL_SCOPUS',
           'COLUMN_LABEL_WOS',
           'COLUMN_LABEL_WOS_PLUS',
           'COLUMN_TYPE_SCOPUS',
           'CONCATENATED_XLSX',
           'COOC_AUTHORIZED_ITEMS',
           'COOC_AUTHORIZED_ITEMS_DICT',
           'COOC_COLOR_NODES',
           'COOC_NETWORKS_FILE',
           'COOC_HTML_PARAM',
           'COUNTRY_TOWNS',
           'COUPL_AUTHORIZED_ITEMS',
           'COUPL_FILENAME_GEXF',
           'COUPL_FILENAME_XLSX',
           'COUPL_GLOBAL_VALUES',
           'COUPL_HTML_PARAM',
           'DEDUPLICATED_XLSX',
           'DIC_DOCTYPE',
           'DIC_AMB_WORDS',
           'DIC_LOW_WORDS',
           'DIC_OUTDIR_DESCRIPTION',
           'DIC_OUTDIR_PARSING',
           'DIC_INST_FILENAME',
           'DIC_TOWN_SYMBOLS',
           'DIC_TOWN_WORDS',
           'DIC_WORD_RE_PATTERN',
           'DISTRIBS_ITEM_FILE',
           'DROPING_WORDS',
           'DROPING_SUFFIX',
           'EMPTY',
           'ENCODING',
           'FIELD_SIZE_LIMIT',
           'FOLDER_NAMES',
           'FOLDER_SELECTION_HELP_TEXT',
           'FR_DROPING_WORDS',
           'GEN_KEEPING_WORDS',
           'GUI_BUTTON_RATIO',
           'GUI_TEXT_MAX_LINES_NB',
           'GUI_WIDGET_RATIO',
           'HEADER',
           'INST_BASE_LIST',
           'INST_FILTER_LIST',
           'KEEPING_WORDS',
           'KEEPING_PREFIX',
           'LABEL_MEANING',
           'LENGTH_DIFF_THRESHOLD',
           'LENGTH_THRESHOLD',
           'MISSING_SPACE_ACRONYMS',
           'NAME_MEANING',
           'NLTK_VALID_TAG_LIST',
           'NMAX_NODES',
           'NORM_JOURNAL_COLUMN_LABEL',
           'NOUN_MINIMUM_OCCURRENCES',
           'NODE_SIZE_REF',
           'PARSING_PERF',
           'RAW_INST_FILENAME',
           'RE_ADDRESS',
           'RE_ADDS_JOURNAL',
           'RE_AUTHOR',
           'RE_DETECT_SCOPUS_NEW',
           'RE_NUM_CONF',
           'RE_REF_AUTHOR_SCOPUS',
           'RE_REF_AUTHOR_SCOPUS_NEW',
           'RE_REF_AUTHOR_WOS',
           'RE_REF_JOURNAL_SCOPUS',
           'RE_REF_JOURNAL_SCOPUS_NEW',
           'RE_REF_JOURNAL_WOS',
           'RE_REF_PAGE_SCOPUS',
           'RE_REF_PAGE_SCOPUS_NEW',
           'RE_REF_PAGE_WOS',
           'RE_REF_VOL_SCOPUS',
           'RE_REF_VOL_WOS',
           'RE_REF_YEAR_SCOPUS',
           'RE_REF_YEAR_WOS',
           'RE_SUB',
           'RE_SUB_FIRST',
           'RE_YEAR',
           'RE_YEAR_JOURNAL',
           'REP_UTILS',
           'RE_ZIP_CODE',
           'SCOPUS',
           'SCOPUS_CAT_CODES',
           'SCOPUS_JOURNALS_ISSN_CAT',
           'SIMILARITY_THRESHOLD',
           'SIZE_MIN',
           'SMALL_WORDS_DROP',
           'SYMBOL',
           'UNKNOWN',
           'USECOLS_SCOPUS',
           'USECOLS_WOS',
           'USER_KEEPING_WORDS',
           'VALID_LABEL_GRAPH',
           'WOS',
          ]

# Standard library imports
import re

# Local imports 
from BiblioAnalysis_Utils.BiblioParsingUtils import remove_special_symbol
from BiblioAnalysis_Utils.BiblioParsingUtils import read_towns_per_country


#####################
# Globals to be set #
#####################

BLACKLISTED_WORDS = [] #['null','nan'] for title keywords


pub_id = 'Pub_id'
idx_address = 'Idx_address'
address = 'Address'
country = 'Country'

COL_NAMES = {   'pub_id'      :   pub_id,
                'address'     :  [pub_id,
                                  idx_address,
                                  address,
                                 ],
                'address_inst':  [pub_id,
                                  idx_address,
                                  address,
                                  country,
                                  'Norm_institutions',
                                  'Unknown_institutions',
                                 ],
                'articles'    :  [pub_id,
                                  'Authors',
                                  'Year',
                                  'Journal',
                                  'Volume',
                                  'Page',
                                  'DOI',
                                  'Document_type',
                                  'Language',
                                  'Title',
                                  'ISSN',
                                 ],
                'authors'     :  [pub_id,
                                  'Idx_author',
                                  'Co_author',
                                 ],  
                'auth_inst'   :  [pub_id,
                                  'Idx_author',
                                  address,
                                  country,
                                  'Norm_institutions',
                                  'Raw_institutions',
                                  'Secondary_institutions',
                                  ], 
                'country'     :  [pub_id,
                                  idx_address,
                                  country,
                                 ],
                'institution' :  [pub_id,
                                  idx_address,
                                  'Institution',
                                 ],                             
                'keywords'    :  [pub_id,
                                  'Keyword',
                                 ],                             
                'references'  :  [pub_id,
                                  'Author',
                                  'Year',                             
                                  'Journal',
                                  'Volume',
                                  'Page',
                                 ],
                'subject'     :  [pub_id,
                                  'Subject',
                                 ],
                'sub_subject' :  [pub_id,
                                  'Sub_subject',
                                 ],
                'temp_col'    :  ['Title_LC', 
                                  'Journal_norm',
                                  'Title',
                                  'title_tokens',
                                  'kept_tokens',
                                  'doc_type_lc',
                                 ],             
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

            
COLUMN_LABEL_WOS = {'affiliations'             : '',
                    'author_keywords'          : 'DE',
                    'authors'                  : 'AU',
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
                        }

NORM_JOURNAL_COLUMN_LABEL = 'Norm_journal'

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


CONCATENATED_XLSX = 'articles_concat.xlsx'


# Setting the file name of the file gathering de normalized affiliations with their raw affiliations per country
COUNTRY_AFFILIATIONS_FILE = 'Country_affiliations.xlsx'

# For droping towns in addresses
COUNTRY_TOWNS_FILE = 'Country_towns.xlsx'


# Setting the file name for the file of institutions types description and order level with the useful columns
INST_TYPES_FILE = "Inst_types.xlsx"
INST_TYPES_USECOLS = ['Level', 'Abbreviation']


COOC_AUTHORIZED_ITEMS = ['AU','CU','AK','IK','TK','S','S2']


COOC_COLOR_NODES = {"Y" : "255,255,0",  # default color for gephi display
                    "J" : "150,0,150",
                    "AU": "20,50,255",
                    "IK": "255,0,255",
                    "AK": "255,0,255",
                    "TK": "205,0,205",
                    "S" : "50,0,150",
                    "S2": "50,0,150",
                    "R" : "255,0,0",
                    "RJ": "255,97,0",
                    "I" : "0,255,0",
                    "CU": "0,255,255",
                    "LA": "0,180,0",
                    "DT": "0,180,0",
                   }


COOC_HTML_PARAM = {'algo'      : 'barnes',
                   'height'    : 1000,
                   'width'     : 1000,
                   'bgcolor'   : '#EAEDED',
                   'font_color': 'black',
                  }


COOC_NETWORKS_FILE = 'coocnetworks.json'


COUPL_AUTHORIZED_ITEMS = ['AU','CU','I','AK','IK','TK','S','S2']


COUPL_FILENAME_XLSX = 'biblio_network.xlsx'


COUPL_FILENAME_GEXF = 'biblio_network.gexf'


COUPL_GLOBAL_VALUES = {'BCTHR' :  1,  # minimum number of shared references 
                                      # for keeping an edge
                       'RTUTHR': 2,   # minimum times of use in the corpus
                                      # to count a reference in the 
                                      # shared references
                       'WTHR'  :   0, # minimum weight to keep a link
                       'NRTHR' :  1,  # minimum number of references to keep a node
                       }


COUPL_HTML_PARAM = {'background_color': '#EAEDED', #light grey,
                    'font_color'      : 'black',
                    'edges_color'     : '#808080',       # gray,
                    'nodes_colors'    : {0        : '#5677fc',  #blue
                                         1        : '#3f51b5',  #indigo
                                         2        : '#e51c23',  #red
                                         3        : '#00bcd4',  #cyan
                                         4        : '#259b24',  #green
                                         5        : '#ffeb3b',  #yellow
                                         6        : '#ff9800',  #orange
                                         7        : '#795548',  #brown 
                                         8        : '#cddc39',  #lime   # limited number of colored values 
                                         'uncolor': '#e0e0e0',  # grey 300 - for nodes out of colored values
                                     },
                    }


DEDUPLICATED_XLSX = 'articles_dedup.xlsx'


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
                }


DIC_OUTDIR_PARSING = {'A'  : 'articles.dat',
                      'AU' : 'authors.dat',
                      'AD' : 'addresses.dat',
                      'ADI': 'addressesinst.dat',
                      'CU' : 'countries.dat',
                      'I'  : 'institutions.dat',
                      'I2' : 'authorsinst.dat',
                      'AK' : 'authorskeywords.dat',
                      'IK' : 'journalkeywords.dat',
                      'TK' : 'titlekeywords.dat',
                      'S'  : 'subjects.dat',
                      'S2' : 'subjects2.dat',
                      'R'  : 'references.dat',
                     }


# For replacing symbols in town names
DIC_TOWN_SYMBOLS = {"-": " ",
                   }


# For replacing names in town names
DIC_TOWN_WORDS = {" lez " : " les ",
                  "saint ": "st ",
                 } 


DISTRIBS_ITEM_FILE = 'DISTRIBS_itemuse.json'


EMPTY = 'empty'


ENCODING = 'iso-8859-1' # encoding used by the function read_database_wos


FIELD_SIZE_LIMIT = 256<<10 # extend maximum field size for wos file reading


FOLDER_NAMES = {'corpus'      : 'Corpus',
                'concat'      : 'concatenation',
                'cooccurrence': 'cooc',
                'coupling'    : 'coupling',
                'description' : 'freq',
                'dedup'       : 'deduplication',
                'filtering'   : 'filter',
                'parsing'     : 'parsing',
                'rawdata'     : 'rawdata',
                'scopus'      : 'scopus',
                'wos'         : 'wos',
               }


FOLDER_SELECTION_HELP_TEXT ='''The selected folder is edited.
                               For changing the selection, just make a new selection.
                               If the selection is valid, please close the window'''


GUI_BUTTON_RATIO = 2.5


GUI_TEXT_MAX_LINES_NB = 3


GUI_WIDGET_RATIO = 1.2 

 
HEADER = True                                                                                      #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!


LABEL_MEANING = {'AU':'Authors',              # ex: Nom1 J, Nom2 E, Nom3 J-P
                 'CU':'Countries',            # ex: France, United States
                 'I' :'Institutions',         # ex: Acronyme1, Acronyme2
                 'DT':'Document types',       # ex: Review, Article, Proceeding
                 'J' :'Journals',          
                 'AK':'Authors keywords',     # ex: BIOMASS, SOLAR FUEL
                 'IK':'Journal keywords',
                 'TK':'Title keywords',
                 'S' :'Subjects',             # ex: Chemical Engineering,Engineering 
                 'S2':'Sub-subjects',         # ex: Applied Mathematics, Organic Chemistry     
                 'R' :'References',
                 'RJ':'References journals',
                 'LA':'Languages',            # ex: English, French
                 'Y' :'Years',                # ex: 2019
                } 


LENGTH_DIFF_THRESHOLD = 10                # unused


LENGTH_THRESHOLD = 30


NAME_MEANING = {'years'          : 'Y',
                'languages'      : 'LA',
                'doctypes'       : 'DT',
                'countries'      : 'CU',
                'institutions'   : 'I',
                'journals'       : 'J',
                'references'     : 'R',
                'refjournals'    : 'RJ',
                'subjects'       : 'S',
                'subjects2'      : 'S2',
                'journalkeywords': 'IK',
                'titlekeywords'  : 'TK',
                'authorskeywords': 'AK',
                'authors'        :'AU',
               }


NLTK_VALID_TAG_LIST = ['NN','NNS','VBG','JJ'] # you can find help on the nltk tags set
                                              # using nltk.help.upenn_tagset() 
    
    
NMAX_NODES = 100 # maximum number of nodes to keep in corpus description 


NOUN_MINIMUM_OCCURRENCES = 3 # Minimum occurrences of a noun to be retained when 
                             # building the set of title keywords see build_title_keywords
    
    
NODE_SIZE_REF = 30


PARSING_PERF = 'failed.json'


RE_ADDRESS = re.compile('''(?<=\]\s)               # Captures: "xxxxx" in string between "]" and "["  
                        [^;]*                      # or  between "]" and end of string or ";"
                        (?=; | $ )''',re.X)


RE_ADDS_JOURNAL = re.compile(r'\([^\)]+\)')        # Captures string between "()" in journal name   (unused)


RE_AUTHOR = re.compile('''(?<=\[)
                      [a-zA-Z,;\s\.\-']*(?=, | \s )
                      [a-zA-Z,;\s\.\-']*
                      (?=\])''',re.X)               # Captures: "xxxx, xxx" or "xxxx xxx" in string between "[" and "]"


RE_NUM_CONF = re.compile(r'\s\d+th\s|\s\d+nd\s')    # Captures: " d...dth " or " d...dnd " in string


RE_DETECT_SCOPUS_NEW = re.compile("\(\d{4}\)(\s)?$")                  # find (dddd); at the end of a string


RE_REF_AUTHOR_SCOPUS = re.compile(r'^[^,0123456789:]*,'               # Captures: "ccccc, ccccc,"
                                  '[^,0123456789:]*,') 

RE_REF_AUTHOR_SCOPUS_NEW = re.compile(r'^[^,0123456789:]*,')          # Captures: "ccccc," (since 07-2023)


RE_REF_AUTHOR_WOS = re.compile(r'^[^,0123456789:]*,')                 # Captures: "ccccc ccccc,"  ; To Do: to be converted to explicite list 


RE_REF_JOURNAL_SCOPUS = re.compile('''\(\d{4}\)\s+[^,]*,              # Capures "(dddd) cccccc," c not a comma
                                   |\(\d{4}\)\s+[^,]*$''',re.X)       # or "(dddd) cccccc" at the end


RE_REF_JOURNAL_SCOPUS_NEW = re.compile('''(?<=,\s)[^,]*,\s+\d+,''')   # (since 07-2023)


RE_REF_JOURNAL_WOS = re.compile('''(?<=,)\s[A-Z]{2}[0-9A-Z&\s\-\.\[\]]+(?=,)         # Captures ", Science & Dev.[3],"
                                |(?<=,)\s[A-Z]{2}[0-9A-Z&\s\-\.\[\]]+$''',re.X)


RE_REF_PAGE_SCOPUS = re.compile(r'\s+[p]{1,2}\.\s+[a-zA-Z0-9]{1,9}')  # Captures: "pp. ddd" or "p. ddd"


RE_REF_PAGE_SCOPUS_NEW = re.compile(r'\s+[p]{1,2}\.\s+[a-zA-Z0-9]{1,9}'
                                     '-[a-zA-Z0-9]{1,9}')              # Captures: "pp. ddd-ddd" (since 07-2023)


RE_REF_PAGE_WOS = re.compile(r',\s+P\d{1,6}')                         # Captures: ", Pdddd"


RE_REF_VOL_SCOPUS = re.compile(''',\s+\d{1,6},                         # Capture: ", dddd,"
                               |,\s+\d{1,6}\s\(                        # or: ", dddd ("
                               |,\s+\d{1,6}$''',re.X)                  # or: ", dddd" at the string end


RE_REF_VOL_WOS = re.compile(r',\s+V\d{1,6}')             # Captures: ", Vdddd"


RE_REF_YEAR_SCOPUS = re.compile(r'(?<=\()\d{4}(?=\))')  # Captures: "dddd" within parenthesis in scopus references


RE_REF_YEAR_WOS = re.compile(r',\s\d{4},')               # Captures: ", dddd," in wos references


RE_SUB = re.compile('''[a-z]?Univ[\.a-zé]{0,6}\s        # Captures alias of University surrounded by texts
                    |[a-z]?Univ[\.a-zé]{0,6}$''',re.X)


RE_SUB_FIRST = re.compile('''[a-z]?Univ[,]\s ''',re.X)       # Captures alias of University before a coma


RE_YEAR = re.compile(r'\d{4}')                            # Captures "dddd" as the string giving the year


RE_YEAR_JOURNAL = re.compile(r'\s\d{4}\s')               # Captures " dddd " as the year in journal name


REP_UTILS = 'BiblioAnalysis_RefFiles'


RE_ZIP_CODE = re.compile(',\s[a-zA-Z]?[\-]?\d+.*',)     # Captures text begining with ', '                                        #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                                                        # and that possibly contains letters and hyphen-minus
    

# Scopus database name
SCOPUS = 'scopus'


SCOPUS_CAT_CODES = 'scopus_cat_codes.txt'


SCOPUS_JOURNALS_ISSN_CAT = 'scopus_journals_issn_cat.txt'


SIMILARITY_THRESHOLD = 80


SIZE_MIN = 1 # Minimum size of co-occurrence nodes


SYMBOL = '\s,;:.\-\/'


UNKNOWN = 'unknown'


# Cooccurrence graph built only for the following labels
VALID_LABEL_GRAPH = ['AU', 'CU', 'S', 'S2', 'IK', 'R', 'RJ', 'I', 'AK', 'TK'] 


# WOS database name
WOS = 'wos'


#################
# Built globals #
#################

COUNTRY_TOWNS = read_towns_per_country(COUNTRY_TOWNS_FILE, REP_UTILS, DIC_TOWN_SYMBOLS, DIC_TOWN_WORDS)


COOC_AUTHORIZED_ITEMS_DICT = {label:name for name,label in LABEL_MEANING.items() 
                              if name in COOC_AUTHORIZED_ITEMS}


_DIC_OUTDIR_DESCRIPTION = {acronym:'freq_'+file for acronym,file in DIC_OUTDIR_PARSING.items()}

_DIC_OUTDIR_DESCRIPTION_ADD = {'DT' : 'freq_doctypes.dat',
                               'J'  : 'freq_journals.dat',
                               'LA' : 'freq_languages.dat',
                               'RJ' : 'freq_refjournals.dat',
                               'Y'  : 'freq_years.dat',
                              }
DIC_OUTDIR_DESCRIPTION = res = {**_DIC_OUTDIR_DESCRIPTION, **_DIC_OUTDIR_DESCRIPTION_ADD}


# This global is used in merge_database function
_USECOLS_SCOPUS = '''Abstract,Affiliations,Authors,Author Keywords,Authors with affiliations,
       CODEN,Document Type,DOI,EID,Index Keywords,ISBN,ISSN,Issue,Language of Original Document,
       Page start,References,Source title,Title,Volume,Year'''
USECOLS_SCOPUS = [x.strip() for x in _USECOLS_SCOPUS.split(',')]


# To Do: Check if this global is still used
_USECOLS_WOS ='''AB,AU,BP,BS,C1,CR,DE,DI,DT,ID,IS,LA,PY,RP,
                SC,SN,SO,TI,UT,VL,WC'''
USECOLS_WOS = [x.strip() for x in _USECOLS_WOS.split(',')]



#############################################
# Specific globals for institutions parsing #
#############################################


# For replacing aliases of a word by a word (case sensitive)
DIC_WORD_RE_PATTERN = {}
DIC_WORD_RE_PATTERN['University'] = re.compile(r'\bUniv[aàädeéirstyz]{0,8}\b\.?')
DIC_WORD_RE_PATTERN['Laboratory'] = re.compile(  r"'?\bLab\b\.?" \
                                               +  "|" \
                                               + r"'?\bLabor[aeimorstuy]{0,7}\b\.?")
DIC_WORD_RE_PATTERN['Center'] = re.compile(r"\b[CZ]ent[erum]{1,3}\b\.?")
DIC_WORD_RE_PATTERN['Department'] = re.compile(r"\bD[eé]{1}p[artemnot]{0,9}\b\.?")
DIC_WORD_RE_PATTERN['Institute'] = re.compile( r"\bInst[ituteosky]{0,7}\b\.?" \
                                              + "|" \
                                              + r"\bIstituto\b") 
DIC_WORD_RE_PATTERN['Faculty'] = re.compile(r"\bFac[lutey]{0,4}\b\.?")
DIC_WORD_RE_PATTERN['School'] = re.compile(r"\bSch[ol]{0,3}\b\.?")


# For keeping chunks of addresses (without accents and in lower case)
    # Setting a list of keeping words
        # Setting a list of general keeping words
_GEN_KEEPING_WORDS = list(DIC_WORD_RE_PATTERN.keys())
GEN_KEEPING_WORDS = [remove_special_symbol(x, only_ascii = False, strip = False).lower() for x in _GEN_KEEPING_WORDS]

        # Setting a list of basic keeping words only for country = 'France'
_BASIC_KEEPING_WORDS = ['Beamline', 'CRG', 'EA', 'ED', 'Equipe', 'ULR', 'UMR', 'UMS', 'UPR']
        # Removing accents keeping non adcii characters and converting to lower case the words, by default
BASIC_KEEPING_WORDS = [remove_special_symbol(x, only_ascii = False, strip = False).lower() for x in _BASIC_KEEPING_WORDS]

        # Setting a user list of keeping words
_USER_KEEPING_WORDS = ['CEA', 'CEMHTI', 'CNRS', 'ESRF', 'FEMTO ST', 'IMEC', 'INES', 'INSA', 'INSERM', 'IRCELYON', 
                       'KU Leuven', 'LaMCoS', 'LEPMI', 'LITEN', 'LOCIE', 'spLine', 'STMicroelectronics', 'TNO', 'UMI', 'VTT']
        # Removing accents keeping non adcii characters and converting to lower case the words, by default
USER_KEEPING_WORDS = [remove_special_symbol(x, only_ascii = False, strip = False).lower() for x in _USER_KEEPING_WORDS]

        # Setting a total list of keeping words
_KEEPING_WORDS = _GEN_KEEPING_WORDS + _BASIC_KEEPING_WORDS + _USER_KEEPING_WORDS
        # Removing accents keeping non adcii characters and converting to lower case the words, by default
KEEPING_WORDS =[remove_special_symbol(x, only_ascii = False, strip = False).lower() for x in _KEEPING_WORDS]


# For keeping chunks of addresses with these prefixes followed by 3 or 4 digits for country France
_KEEPING_PREFIX = ['EA', 'FR', 'U', 'ULR', 'UMR', 'UMS', 'UPR',] # only followed by 3 or 4 digits and only for country = 'France'
KEEPING_PREFIX = [x.lower() for x in _KEEPING_PREFIX]


# For droping chunks of addresses (without accents and in lower case)
    # Setting a list of droping suffixes
_DROPING_SUFFIX = ["campus", "laan", "park", "platz", "staal", "strae", "strasse", "straße", "vej", "waldring", "weg",
                  "schule", "-ku", "-cho", "-ken", "-shi", "-gun", "alleen", "vagen", "vei", "-gu", "-do", "-si", "shire"] 

        # added "ring" but drops chunks containing "Engineering"
        # Removing accents keeping non adcii characters and converting to lower case the droping suffixes, by default
DROPING_SUFFIX = [remove_special_symbol(x, only_ascii = False, strip = False).lower() for x in _DROPING_SUFFIX]


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
_DROPING_WORDS = [remove_special_symbol(x, only_ascii = False, strip = False).lower() for x in _DROPING_WORDS]
        # Escaping the regex meta-character "." from the droping words, by default
_DROPING_WORDS = [x.replace(".", r"\.") for x in _DROPING_WORDS]
DROPING_WORDS = [x.replace("/", r"\/") for x in _DROPING_WORDS]


        # Setting a list of droping words for France
_FR_DROPING_WORDS = ["allee", "antenne", "av", "av.", "ave", "avenue", 
                     "ba", "bat", "bat.", "batiment", "blv.", "blvd", "boulevard",
                     "campus", "cedex", "ch.", "chemin", "complexe", "cours", "cs",
                     "domaine", "esplanade", "foret", "immeuble", 
                     "montee", "no.", "p", "p°", "parcelle", "parvis", "pl", "pl.", "place", "parc",
                     "plan", "pole", "quai", "r", "r.", "region", "route", "rue",
                     "site", "v.", "via", "villa", "voie", "zac", "zi", "z.i.", "zone"]

        # Removing accents keeping non adcii characters and converting to lower case the droping words, by default
_FR_DROPING_WORDS = [remove_special_symbol(x, only_ascii = False, strip = False).lower() for x in _FR_DROPING_WORDS]
        # Escaping the regex meta-character "." from the droping words, by default
_FR_DROPING_WORDS = [x.replace(".", r"\.") for x in _FR_DROPING_WORDS]
FR_DROPING_WORDS = [x.replace("/", r"\/") for x in _FR_DROPING_WORDS]


# List of small words to drop in raw affiliations for affiliations normalization 
SMALL_WORDS_DROP = ['the', 'and','of', 'for', 'de', 'et', 'la', 'aux', 'a', 'sur', 'pour', 'en', 'l', 'd', 'le']


# List of acronyms for detecting missing space in raw affiliations for affiliations normalization 
_MISSING_SPACE_ACRONYMS = ['FR', 'FRE', 'ULR', 'UMR', 'UMS', 'U', 'UPR', 'UR']
MISSING_SPACE_ACRONYMS = [x.lower() for x in _MISSING_SPACE_ACRONYMS]


#################################################### Use to be checked ###############################

# Potentialy ambiguous words in institutions names
DIC_AMB_WORDS = {' des ': ' ', # Conflict with DES institution
                 ' @ ': ' ', # Management conflict with '@' between texts
                }

DIC_INST_FILENAME = 'Inst_dic.csv'

INST_BASE_LIST = ['UMR', 'CNRS', 'University']                                                     #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

# Authors affiliations filter (default: None) as a list of tuples (instituion,country)
INST_FILTER_LIST = [('LITEN','France'),('INES','France')]

RAW_INST_FILENAME = 'Raw_inst.csv'
    


