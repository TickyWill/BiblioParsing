"""Module to set regex as globals used in other modules.
"""

__all__ = ['AFFIL_DROPPING_PATTERNS_DIC',
           'AFFIL_KEEPING_PATTERNS_DIC',
           'AFFIL_WORDS_SET_TEMPLATE',
           'AFFIL_WORD_SUBSTITUTE_PATTERN_DIC',
           'AFFIL_WORD_TO_DROP_TEMPLATE',
           'COUNTRY_ALIAS_TEMPLATE',
           'RE_ADDRESS',
           'RE_ADDS_JOURNAL',
           'RE_AFFIL_AMB_WORDS_LIST',
           'RE_AUTHOR',
           'RE_AUTHORS_SMALL_WORDS',
           'RE_AWA',
           'RE_ISSN',
           'RE_JOURNAL_ACRONYMS',
           'RE_NUM_CONF',
           'RE_REF_AUTHOR_DROP',
           'RE_SCOPUS_AUTHOR_INITIALS',
           'RE_SCOPUS_AUTHOR_NAME',
           'RE_SCOPUS_JOURNAL_DIGITS',
           'RE_SCOPUS_REF_AND',
           'RE_SCOPUS_REF_AUTHOR',
           'RE_SCOPUS_REF_CONF',
           'RE_SCOPUS_REF_DIGITS',
           'RE_SCOPUS_REF_DIGITS_DROP',
           'RE_SCOPUS_REF_DOI',
           'RE_SCOPUS_REF_DOI_YEAR',
           'RE_SCOPUS_REF_DOT',
           'RE_SCOPUS_REF_EL_AL',
           'RE_SCOPUS_REF_JOURNAL',
           'RE_SCOPUS_REF_MONTHS_DROP',
           'RE_SCOPUS_REF_ONLY_DIGITS',
           'RE_SCOPUS_REF_PAGES',
           'RE_SCOPUS_REF_SYMB',
           'RE_SCOPUS_REF_WORDS_DROP',
           'RE_SCOPUS_REF_YEAR',
           'RE_SCOPUS_REF_YEARS',
           'RE_SUB',
           'RE_SUB_FIRST',
           'RE_WOS_REF_DOI',
           'RE_WOS_REF_YEAR',
           'RE_WOS_REF_JOURNAL',
           'RE_YEAR',
           'RE_YEAR_JOURNAL',
           'RE_ZIP_CODE',
          ]

##################
# Parsing regexp #
##################

# Standard library imports
import re
from string import Template

# For uniformization of country names
COUNTRY_ALIAS_TEMPLATE = Template(r'$word\s' + "|" + r'$word$$')

RE_ADDRESS = re.compile(r'''(?<=\]\s)           # Captures: "xxxxx" in string between "]" and "["
                        [^;]*                          # or  between "]" and end of string or ";"
                        (?=; | $ )''', re.X)

RE_ADDS_JOURNAL = re.compile(r'\([^\)]+\)')        # Captures string between "()" in journal name   (unused)

# Potentialy ambiguous words in affiliations names
AFFIL_AMB_WORDS_LIST = ['des', '@']
SET_AMB_WORDS_TEMPLATE = Template(r'\s$word\s')
RE_AFFIL_AMB_WORDS_LIST = [SET_AMB_WORDS_TEMPLATE.substitute({"word":word}) for word in AFFIL_AMB_WORDS_LIST]


RE_AUTHOR = re.compile(r'''(?<=\[)
                       [a-zA-Z,;\s\.\-']*(?=, | \s )
                       [a-zA-Z,;\s\.\-']*
                       (?=\])''', re.X)              # Captures: "xxxx, xxx" or "xxxx xxx" in string between "[" and "]"

RE_NUM_CONF = re.compile(r'\s\d+th\s|\s\d+nd\s')     # Captures: " d...dth " or " d...dnd " in string


RE_SUB = re.compile(r'''[a-z]?Univ[\.a-zé]{0,6}\s # Captures alias of University surrounded by texts
                    |[a-z]?Univ[\.a-zé]{0,6}$''', re.X)

RE_SUB_FIRST = re.compile(r'''[a-z]?Univ[,]\s ''', re.X) # Captures alias of University before a coma

RE_YEAR = re.compile(r'\d{4}')                           # Captures "dddd" as the string giving the year

RE_YEAR_JOURNAL = re.compile(r'\s\d{4}\s')               # Captures " dddd " as the year in journal name

RE_ZIP_CODE = re.compile(r',\s[a-zA-Z]?[\-]?\d+.*',)     # Captures text begining with ', '
                                                         # and that possibly contains letters and hyphen-minus
RE_AWA = re.compile(r'\w+;,\s\w+|\w+;\w+')               # Captures ';, ' or ';' surrounded by letters


AUTHORS_SMALL_WORDS_LIST = ['de', 'von']

_JOURNAL_NAMES_LIST = ['Arxiv']
_JOURNAL_SHORTS_LIST = ['Adv', 'Bull', 'Chem', 'Commun', 'Conf', 'J', 'Lett', 'Nat', 'Proc', 'Rep', 'Rev',
                        'Ser', 'Symp', 'Trans', 'Transact']
_JOURNAL_WORDS_LIST = ['Acta', 'Conference', 'Journal', 'Letters', 'Magazine', 'Procedia', 'Proceedings',
                       'Series', 'Symposium', 'Transactions', 'Workshop']

JOURNAL_ACRONYMS_LIST = ['ACS', 'ECS', 'IEEE', 'RRL']
JOURNAL_NAMES_LIST = (_JOURNAL_NAMES_LIST
                      + [x.lower() for x in _JOURNAL_NAMES_LIST]
                      + [x.upper() for x in _JOURNAL_NAMES_LIST])
JOURNAL_SHORTS_LIST = (_JOURNAL_SHORTS_LIST
                       + [x.lower() for x in _JOURNAL_SHORTS_LIST]
                       + [x.upper() for x in _JOURNAL_SHORTS_LIST])
JOURNAL_WORDS_LIST = (_JOURNAL_WORDS_LIST
                      + [x.lower() for x in _JOURNAL_WORDS_LIST]
                      + [x.upper() for x in _JOURNAL_WORDS_LIST])
JOURNAL_FULLS_LIST =  JOURNAL_WORDS_LIST + JOURNAL_ACRONYMS_LIST

SET_ACRONYMS_TEMPLATE = Template(r'^$word\s')
SET_AUTH_SMALL_WORDS_TEMPLATE = Template(r'^$word\s')
SET_FULLS_TEMPLATE = Template(r'^$word\s' + '|' + r'\s$word[,\s]?')
SET_SHORTS_TEMPLATE = Template(r'^$word\.\s' + '|' + r'\s$word\.[,\s]?')
SET_UNDOTTED_SHORTS_TEMPLATE = Template(r'^$word\s' + '|' + r'\s$word\s'+ '|' + r'\s$word$$')
SET_NAMES_TEMPLATE = Template(r'^$word$$')

RE_JOURNAL_ACRONYMS = re.compile('|'.join([SET_ACRONYMS_TEMPLATE.substitute({"word":word})
                                           for word in JOURNAL_ACRONYMS_LIST]))


RE_AUTHORS_SMALL_WORDS = re.compile('|'.join([SET_AUTH_SMALL_WORDS_TEMPLATE.substitute({"word":word})
                                              for word in AUTHORS_SMALL_WORDS_LIST]))


# ************************************************************************
# * Regex specific to parsing of publications' references in Scopus data *
# ************************************************************************

RE_SCOPUS_REF_JOURNAL = re.compile('|'.join([SET_FULLS_TEMPLATE.substitute({"word":word})
                                             for word in JOURNAL_FULLS_LIST])
                                   + '|' +
                                   '|'.join([SET_SHORTS_TEMPLATE.substitute({"word":word})
                                             for word in JOURNAL_SHORTS_LIST])
                                   + '|' +
                                   '|'.join([SET_UNDOTTED_SHORTS_TEMPLATE.substitute({"word":word})
                                             for word in JOURNAL_SHORTS_LIST]))

# Captures: "dddd" after, before or within parenthesis in string or at end of string
RE_SCOPUS_REF_YEARS = re.compile(r'(?<=\()\d{4}' + '|' + r'\d{4}(?=\))' + '|' + r'\s\d{4}$'
                                 + '|' + r'(?<=/)\d{4}' + '|' + r'\d{4}(?=/)'
                                 + '|' + r'\s\d{4}(?=,)' + '|' + r'\s\d{4}\s')

# Captures: "dddd" within parenthesis in string or at end of string
RE_SCOPUS_REF_YEAR = re.compile(r'(?<=\()\d{4}(?=\))')


RE_SCOPUS_REF_DOI_YEAR = re.compile(r'\.\d{4}\.')


RE_SCOPUS_REF_DIGITS = re.compile(r'\s\d{4}\s')


RE_SCOPUS_REF_CONF = re.compile(r'[a-zA-Z0-9]?\sConference\s[a-zA-Z0-9]?')


RE_SCOPUS_REF_DOI = re.compile(r'[\s]?10\.[\d]{4,}\/.*$')


RE_SCOPUS_REF_DOT = re.compile(r'^[a-zA-Z]\.[\s]?[\-]?[a-zA-Z]\.\s'
                               + '|' +
                               r'\s[a-zA-Z]\.[\s]?[\-]?[a-zA-Z]\.\s')


RE_REF_AUTHOR_DROP = re.compile(r'^[A-Z]{3,}\s' + '|' + r'\s[A-Z]{3,}\s' + '|' + r'\s[A-Z]{4,}$')


RE_SCOPUS_REF_AUTHOR = re.compile(r'^[a-zA-Z]*\s[a-zA-Z][\.]?$'
                                  + '|' +
                                  r'^[a-zA-Z]*\s[a-zA-Z][\.]?-?[a-zA-Z][\.]?$'
                                  + '|' +
                                  r'\set\sal\.$'
                                  + '|' +
                                  r'^[a-zA-Z]{1,2}[\.]?$'
                                  + '|' +
                                  r'[a-zA-Z]{1}-[a-zA-Z]{1}[\.]?$'
                                  + '|' +
                                  r'[a-zA-Z]{1}[\.]?-[a-zA-Z]{1}[\.]?$')


RE_SCOPUS_AUTHOR_INITIALS = re.compile(r'^[a-zA-Z]{1,2},\s$'
                                       + '|' +
                                       r'^[a-zA-Z]-?[a-zA-Z],\s$')


RE_SCOPUS_AUTHOR_NAME = re.compile(r'^[a-zA-Z]*\s[a-zA-Z]{1,2},\s$'
                                   + '|' +
                                   r'^[a-zA-Z]*\s[a-zA-Z]-[a-zA-Z],\s$')


RE_SCOPUS_REF_PAGES = re.compile(r'^pp\.\s\d')


RE_SCOPUS_REF_EL_AL = re.compile(r'[eE]t\-al\.$' + '|' + r'[eE]t\sal\.$')


RE_SCOPUS_JOURNAL_DIGITS = re.compile(r'^[\d]+[^a-zA-Z]*')


RE_SCOPUS_REF_AND = re.compile(r'\sand\s' + '|' + r'^and\s')


RE_SCOPUS_REF_SYMB = re.compile(r'^[^a-zA-Z0-9]+$')


RE_SCOPUS_REF_ONLY_DIGITS = re.compile(r'^[\d]+$')


REF_DROPING_SHORT_MONTHS_LIST = ['Jan', 'Feb', 'Apr', 'Jun', 'Jul', 'Aug', 'Sept', 'Oct', 'Nov',
                                 'Dec', 'janv', 'févr', 'avr', 'juill', 'sept', 'oct', 'nov', 'déc']
REF_DROPING_FULL_MONTHS_LIST = ['March', 'May', 'June', 'October', 'mars', 'mai', 'juin', 'août']
REF_DROPING_ALL_MONTHS_LIST = REF_DROPING_SHORT_MONTHS_LIST + REF_DROPING_FULL_MONTHS_LIST

SET_DROP_SHORT_MONTH_TEMPLATE = Template(r'^$word\.\s' + '|' + r'\s$word\.\s'
                                         + '|' +
                                         r'\s$word\.$$' + '|' + r'^$word\.$$')

SET_DROP_ALL_MONTH_TEMPLATE = Template(r'^$word\s' + '|' + r'\s$word\s' + '|' +
                                       r'\s$word$$' + '|' + r'^$word$$')

RE_SCOPUS_REF_MONTHS_DROP = re.compile('|'.join([SET_DROP_SHORT_MONTH_TEMPLATE.substitute({"word":word})
                                                 for word in REF_DROPING_SHORT_MONTHS_LIST])
                                       + '|' +
                                       '|'.join([SET_DROP_ALL_MONTH_TEMPLATE.substitute({"word":word})
                                                 for word in REF_DROPING_ALL_MONTHS_LIST]))

REF_DROPING_WORDS_LIST = ['Appendix', 'available', 'Available', 'https', r'\[?Online\]?', 'presented', 'Presented']
SET_DROP_WORD_TEMPLATE = Template(r'^$word[,\s]?')

RE_SCOPUS_REF_WORDS_DROP = re.compile('|'.join([SET_DROP_WORD_TEMPLATE.substitute({"word":word})
                                                for word in REF_DROPING_WORDS_LIST]))

RE_SCOPUS_REF_DIGITS_DROP = re.compile(r'^\d{4}[a-zA-Z]{2,}\.[\.]?' + '|' + r'^\d{1}\:[A-Z]{3}\:')


# Setting regex for normalization of ISSN to the form dddd-dddd or dddd-dddX
RE_ISSN = re.compile(r'^[0-9]{8}' + '|' + r'[0-9]{4}' + '|' + r'[0-9]{3}X')


# *********************************************************************
# * Regex specific to parsing of publications' references in WoS data *
# *********************************************************************
RE_WOS_REF_DOI = re.compile(r'[\s]?10\.[\d]{4,}\/.*$' + '|' + r'^DOI\s')


RE_WOS_REF_YEAR = re.compile(r'^\d{4}$')


RE_WOS_REF_JOURNAL = re.compile('|'.join([SET_FULLS_TEMPLATE.substitute({"word":word})
                                          for word in JOURNAL_FULLS_LIST])
                                + '|' +
                                '|'.join([SET_SHORTS_TEMPLATE.substitute({"word":word})
                                          for word in JOURNAL_SHORTS_LIST])
                                + '|' +
                                '|'.join([SET_NAMES_TEMPLATE.substitute({"word":word})
                                          for word in JOURNAL_NAMES_LIST])
                                + '|' +
                                '|'.join([SET_UNDOTTED_SHORTS_TEMPLATE.substitute({"word":word})
                                          for word in JOURNAL_SHORTS_LIST]))


# ******************************************************
# * Regex specific to parsing of authors' affiliations *
# ******************************************************

# For replacing aliases of a word by a word (case sensitive)
AFFIL_WORD_SUBSTITUTE_PATTERN_DIC = {'University': r'\b[a-z]?Univ[aàäcdeéirstyz]{0,8}\b\.?',
                                     'Laboratory': r"'?\bLab\b\.?" + "|" + r"'?\bLabor[aeimorstuy]{0,7}\b\.?",
                                     'Center'    : r'\b[CZ]ent[erum]{1,3}\b\.?',
                                     'Department': r'\bD[eéi]{1}p[arteimnot]{0,9}\b\.?',
                                     'Institute' : r'\bInst[ituteosky]{0,7}\b\.?' + '|' + r'\bIstituto\b',
                                     'Faculty'   : r'\bFac[lutey]{0,4}\b\.?',
                                     'School'    : r'\bSch[ol]{0,3}\b\.?',
                                    }


# Setting useful regex template capturing for instance "word" in "word of set"
# or " word" in "set with word", or "word" in "Azert Word Azerty"
AFFIL_WORDS_SET_TEMPLATE = Template(r'[\s]$word[\s)]' + '|' + r'[\s]$word$$' + '|'
                                    + r'^$word\b')


# Setting the regex for capturing, for instance "bp12" in "azert BP12 yui_OP"
# capturing " bp 156X" in " bp 156X azert" or capturing "08bp" in "azert 08BP yui_OP".
AFFIL_DROPPING_BP_PATTERN = r'\bbp\s?\d+[a-z]?\b' + '|' + r'\b\d+bp\b'


# Pattern for capturing state code in addresses for UK
# Capturing: for instance, " BT7 1NN" or " WC1E 6BT" or " G128QQ"
# " a# #a", " a# #az", " a# ##a", " a# ##az", " a##a", " a##az", " a###a", " a###az",
# " a#a #a", " a#a #az", " a#a ##a", " a#a ##az", " a#a#a", " a#a#az", " a#a##a", " a#a##az",
# " a## #a", " a## #az", " a## ##a", " a## ##az", " a###a", " a###az", " a####a", " a####az",
# " a##a #a", " a##a #az", " a##a ##a", " a##a ##az", " a##a#a", " a##a#az", " a##a##a", " a##a##az",
# " az# #a", " az# #az", " az# ##a", " az# ##az", " az##a", " az##az", " az###a", " az###az",
# " az#a #a", " az#a #az", " az#a ##a", " az#a ##az", " az#a#a", " az#a#az", " az#a##a", " az#a##az",
# " az## #a", " az## #az", " az## ##a", " az## ##az", " az###a", " az###az", " az###a", " az####az",
# " az##a #a", " az##a #az", " az##a ##a", " az##a ##az", " az##a#a", " az##a#az", " az##a#a", " az##a##az",
AFFIL_UK_ZIP_PATTERN = r'^\s?[a-z]{1,2}\d{1,2}[a-z]{0,1}\s?\d{1,2}[a-z]{1,2}$'


# Pattern for capturing state code in addresses for North America
# Capturing: for instance, " NY" or ' NI BT48 0SG' or " ON K1N 6N5"
# " az" or " az " + 6 or 7 characters in 2 parts separated by spaces
AFFIL_NAM_ZIP_PATTERN = r'^\s?[a-z]{2}$' + '|' + r'^\s?[a-z]{2}\s[a-z0-9]{3,4}\s[a-z0-9]{2,3}$'

# Template for capturing zip code in addresses for other countries
# Capturing letters and zip-digits as given for each country by the global ZIP_CODES
# defined in BiblioParsing.general_globals module
AFFIL_ZIP_TEMPLATE = Template(r'\b($zip_letters)[\s-]?(\d{$zip_digits})\b')


AFFIL_DROPING_ZIP_PATTERN_DIC = {'united_kingdom'  : AFFIL_UK_ZIP_PATTERN,
                                 'north_america'   : AFFIL_NAM_ZIP_PATTERN,
                                 'zip_code_country': AFFIL_ZIP_TEMPLATE,
                                }


# Pattern for capturing embedding digits in addresses
# In first part, for capturing, for instance, " 1234" in "azert 1234-yui_OP"
# or " 1" in "azert 1-yui_OP" or " 1-23" in "azert 1-23-yui"
# Or, in second part, capturing, for instance, "azert12" in "azert12 UI_OPq"
# or "azerty1234567" in "azerty1234567 ui_OPq"
AFFIL_DROPPING_DIGITS_PATTERN = r'\s?\d+(-\d+)?\b' + '|' + r'\b[a-z]+(-)?\d{2,}\b'

# Template for capturing prefix followed by 4 digits and potentially separated by "-",
# the prefix is given by the KEEPING_PREFIX global in BiblioParsing.specific_globals module
# for instance, capturing "umr1234" in "azert UMR1234 YUI_OP"
# or "fr1234" in "azert-fr1234 Yui_OP".
AFFIL_DIGITS_KEEPING_PREFIX_TEMPLATE = Template(r'\b$prefix[-]?\d{4}\b')


# Template for capturing suffix given by the DROPING_SUFFIX global
# in BiblioParsing.specific_globals module
# For instance, capturing "platz" in "Azertyplatz uiops12"
# Or, for instance, capturing "-gu" in "Yeongtong-gu"
AFFIL_DROPPING_SUFFIX_TEMPLATE = Template(r'\B$word\b' + '|' + r'\b$word\b')


# Template for capturing word given by the FR_DROPPING_WORDS and DROPPING_WORDS globals
# in BiblioParsing.specific_globals module
# For instance, capturing "avenue" in "12 Avenue Azerty" or " cedex" in "azert cedex"
# in "12 Avenue Azerty" or " cedex" in "azert cedex"
AFFIL_DROPPING_WORD_TEMPLATE = Template(r'[\s(]$word[\s)]' + '|' + r'[\s]$word$$' + '|' + r'^$word\b')


# Template for capturing prefix attached to 3 or 4 digits, the prefix by given
# by the KEEPING_PREFIX global in BiblioParsing.specific_globals module
AFFIL_KEEPING_PREFIX_TEMPLATE = Template(r'\b$prefix\d{3,4}\b')


AFFIL_KEEPING_WORD_TEMPLATE = Template(r'\b$word\b')


AFFIL_DROPPING_PATTERNS_DIC = {'digits'            : AFFIL_DROPPING_DIGITS_PATTERN,
                               'suffix'            : AFFIL_DROPPING_SUFFIX_TEMPLATE,
                               'word'              : AFFIL_DROPPING_WORD_TEMPLATE,
                               'united_kingdom_zip': AFFIL_UK_ZIP_PATTERN,
                               'north_america_zip' : AFFIL_NAM_ZIP_PATTERN,
                               'other_zip'         : AFFIL_ZIP_TEMPLATE,
                               'bp'                : AFFIL_DROPPING_BP_PATTERN,
                              }


AFFIL_KEEPING_PATTERNS_DIC = {'digits_prefix'     : AFFIL_DIGITS_KEEPING_PREFIX_TEMPLATE,
                              'prefix'            : AFFIL_KEEPING_PREFIX_TEMPLATE,
                              'word'              : AFFIL_KEEPING_WORD_TEMPLATE,
                             }


# Template for capturing small words or accronyms given by the SMALL_WORDS_DROP
# and the MISSING_SPACE_ACRONYMS globals in BiblioParsing.specific_globals module
# For instance capturing 'of' in 'technical university of denmark'
# capturing 'd' in 'institut d ingenierie'
# capturing 'the' in 'the denmark university'
# or capturing 'umr' in 'umr dddd' or 'umr dd'
AFFIL_WORD_TO_DROP_TEMPLATE = Template(r'[\s(]$word[\s)]|[\s]$word$$|^$word\b')
