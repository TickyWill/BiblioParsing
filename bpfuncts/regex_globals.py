"""Module to set regex as globals used in other modules.

The parameters values are set from the 'regex_globals.yaml' file 
available by default in the 'DemoConfig' folder of the package.
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
           'RE_SCOPUS_REF_ET_AL',
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


# Standard library imports
import re
from string import Template

# Local imports
from bpfuncts.globals_utils import read_yaml_regex_globals


# Getting the globals values from the YAML file of parsing globals
regex_globals_dic = read_yaml_regex_globals()

# ****************************************************
# * Regex for standardization and correction of data *
# ****************************************************

# Regex for capturing potentialy ambiguous words to standardize addresses
AFFIL_AMB_WORDS_LIST = regex_globals_dic['affil_amb_words_list']
SET_AMB_WORDS_TEMPLATE = Template(regex_globals_dic['amb_words_template'])
RE_AFFIL_AMB_WORDS_LIST = [SET_AMB_WORDS_TEMPLATE.substitute({"word":word}) for word in AFFIL_AMB_WORDS_LIST]

# Regex for capturing small words in authors' names
AUTHORS_SMALL_WORDS_LIST = regex_globals_dic['authors_small_words_list']
SET_AUTH_SMALL_WORDS_TEMPLATE = Template(regex_globals_dic['auth_small_words_template'])
RE_AUTHORS_SMALL_WORDS = re.compile('|'.join([SET_AUTH_SMALL_WORDS_TEMPLATE.substitute({"word":word})
                                              for word in AUTHORS_SMALL_WORDS_LIST]))

# Templates for uniformization of country names in addresses
COUNTRY_ALIAS_TEMPLATE_LIST = regex_globals_dic['country_alias_template_list']
COUNTRY_ALIAS_TEMPLATE = Template('|'.join(COUNTRY_ALIAS_TEMPLATE_LIST))

# Regex for capturing "xxxxx" in string between "]" and "[" or  between "]" and end of string or ";"
RE_ADDRESS = re.compile(regex_globals_dic['re_address'], re.X)

# Regex for capturing "xxxx, xxx" or "xxxx xxx" in string between "[" and "]"
RE_AUTHOR = re.compile(regex_globals_dic['re_author'], re.X)

# Regex for capturing " d...dth " or " d...dnd " in string
RE_NUM_CONF_LIST = regex_globals_dic['re_num_conf_list']
RE_NUM_CONF = re.compile('|'.join(RE_NUM_CONF_LIST))

# Regex for capturing alias of University surrounded by texts
RE_SUB_LIST = regex_globals_dic['re_sub_list']
RE_SUB = re.compile('|'.join(RE_SUB_LIST), re.X)

# Regex for capturing  alias of University before a coma
RE_SUB_FIRST = re.compile(regex_globals_dic['re_sub_first'], re.X)

# Regex for capturing "dddd" as the string giving the year
RE_YEAR = re.compile(regex_globals_dic['re_year'])

# Regex for capturing " dddd " as the year in journal name
RE_YEAR_JOURNAL = re.compile(regex_globals_dic['re_year_journal'])

# Regex for capturing text begining with ', '
# and that possibly contains letters and hyphen-minus
RE_ZIP_CODE = re.compile(regex_globals_dic['re_zip_code'])

# Regex for capturing ';, ' or ';' surrounded by letters
RE_AWA_LIST = regex_globals_dic['re_awa_list']
RE_AWA = re.compile('|'.join(RE_AWA_LIST))

# Regex for normalization of ISSN to the form dddd-dddd or dddd-dddX
RE_ISSN_LIST = regex_globals_dic['re_issn_list']
RE_ISSN = re.compile('|'.join(RE_ISSN_LIST))


# *******************************
# * Affiliations' parsing regex *
# *******************************

# ------------Addresses uniformization-----------
# Patterns for capturing aliases of 'center' for uniformization
_AFFIL_CTR_PATTERN_LIST = regex_globals_dic['affil_ctr_pattern_list']

# Patterns for capturing aliases of 'Department' for uniformization
_AFFIL_DEPT_PATTERN_LIST = regex_globals_dic['affil_dept_pattern_list']

# Patterns for capturing aliases of 'Faculty' for uniformization
_AFFIL_FAC_PATTERN_LIST = regex_globals_dic['affil_fac_pattern_list']

# Patterns for capturing aliases of 'Institute' for uniformization
_AFFIL_INST_PATTERN_LIST =regex_globals_dic['affil_inst_pattern_list']

# Patterns for capturing aliases of 'Laboratory' for uniformization
_AFFIL_LAB_PATTERN_LIST = regex_globals_dic['affil_lab_pattern_list']

# Patterns for capturing aliases of 'School' for uniformization
_AFFIL_SCH_PATTERN_LIST = regex_globals_dic['affil_sch_pattern_list']

# Patterns for capturing aliases of 'University' for uniformization
_AFFIL_UNIV_PATTERN_LIST = regex_globals_dic['affil_univ_pattern_list']

_AFFIL_WORD_SUBSTITUTE_PATTERN_DIC = {'Center'    : _AFFIL_CTR_PATTERN_LIST,
                                      'Department': _AFFIL_DEPT_PATTERN_LIST,
                                      'Institute' : _AFFIL_INST_PATTERN_LIST,
                                      'Faculty'   : _AFFIL_FAC_PATTERN_LIST,
                                      'Laboratory': _AFFIL_LAB_PATTERN_LIST,
                                      'School'    : _AFFIL_SCH_PATTERN_LIST,
                                      'University': _AFFIL_UNIV_PATTERN_LIST,
                                     }

AFFIL_WORD_SUBSTITUTE_PATTERN_DIC = {key:'|'.join(val) for key, val in _AFFIL_WORD_SUBSTITUTE_PATTERN_DIC.items()}


# ------------Address chunck dropping-------------
# Regex for capturing postal box to drop addresses chuncks,
# for instance "bp12" in "azert BP12 yui_OP"
# " bp 156X" in " bp 156X azert" or "08bp" in "azert 08BP yui_OP"
_AFFIL_DROPPING_BP_PATTERN_LIST = regex_globals_dic['affil_dropping_bp_pattern_list']
_AFFIL_DROPPING_BP_PATTERN = '|'.join(_AFFIL_DROPPING_BP_PATTERN_LIST)

# Pattern for capturing embedding digits to drop addresses' chuncks
# In first part, for capturing, for instance, " 1234" in "azert 1234-yui_OP"
# or " 1" in "azert 1-yui_OP" or " 1-23" in "azert 1-23-yui"
# Or, in second part, capturing, for instance, "azert12" in "azert12 UI_OPq"
# or "azerty1234567" in "azerty1234567 ui_OPq"
_AFFIL_DROPPING_DIGITS_PATTERN_LIST = regex_globals_dic['affil_dropping_digits_pattern_list']
_AFFIL_DROPPING_DIGITS_PATTERN = '|'.join(_AFFIL_DROPPING_DIGITS_PATTERN_LIST)

# Template for capturing suffix to drop addresses' chuncks
# For instance, capturing "platz" in "Azertyplatz uiops12"
# Or, for instance, capturing "-gu" in "Yeongtong-gu"
# Suffix list specified in 'AffiliationsGlobals.yaml' file
_AFFIL_DROPPING_SUFFIX_TEMPLATE_LIST = regex_globals_dic['affil_dropping_suffix_template_list']
_AFFIL_DROPPING_SUFFIX_TEMPLATE = Template('|'.join(_AFFIL_DROPPING_SUFFIX_TEMPLATE_LIST))

# Template for capturing words given to drop addresses' chuncks
# For instance, capturing "avenue" in "12 Avenue Azerty" or " cedex" in "azert cedex"
# in "12 Avenue Azerty" or " cedex" in "azert cedex"
# Words list specified in 'AffiliationsGlobals.yaml' file
_AFFIL_DROPPING_WORD_TEMPLATE_LIST = regex_globals_dic['affil_dropping_word_template_list']
_AFFIL_DROPPING_WORD_TEMPLATE = Template('|'.join(_AFFIL_DROPPING_WORD_TEMPLATE_LIST))

# Zip code search in addresses

# Pattern for capturing state code in addresses for North America
# Capturing: for instance, " NY" or ' NI BT48 0SG' or " ON K1N 6N5"
# " az" or " az " + 6 or 7 characters in 2 parts separated by spaces
_AFFIL_NAM_ZIP_PATTERN_LIST = regex_globals_dic['affil_nam_zip_pattern_list']
_AFFIL_NAM_ZIP_PATTERN = '|'.join(_AFFIL_NAM_ZIP_PATTERN_LIST)

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
_AFFIL_UK_ZIP_PATTERN = regex_globals_dic['affil_uk_zip_pattern']

# Template for capturing zip code in addresses for other countries
# Capturing letters and zip-digits as given for each country by the global ZIP_CODES
# defined in BiblioParsing.general_globals module
_AFFIL_ZIP_TEMPLATE = Template(regex_globals_dic['affil_zip_template'])

# Dict of templates for capturing features to drop chuncks of addresses
AFFIL_DROPPING_PATTERNS_DIC = {'bp'                : _AFFIL_DROPPING_BP_PATTERN,
                               'digits'            : _AFFIL_DROPPING_DIGITS_PATTERN,
                               'north_america_zip' : _AFFIL_NAM_ZIP_PATTERN,
                               'other_zip'         : _AFFIL_ZIP_TEMPLATE,
                               'suffix'            : _AFFIL_DROPPING_SUFFIX_TEMPLATE,
                               'united_kingdom_zip': _AFFIL_UK_ZIP_PATTERN,
                               'word'              : _AFFIL_DROPPING_WORD_TEMPLATE,
                              }

# ------------Address chunck keeping-------------
# Template for capturing prefix followed by 4 digits and potentially separated by "-",
# for instance, capturing "umr1234" in "azert UMR1234 YUI_OP"
# or "fr1234" in "azert-fr1234 Yui_OP"
# Prefix list specified in 'AffiliationsGlobals.yaml' file
_AFFIL_DIGITS_KEEPING_PREFIX_TEMPLATE = Template(regex_globals_dic['affil_digits_keeping_prefix_template'])

# Template for capturing prefix attached to 3 or 4 digits to keep the address chunck
# Prefix list specified in 'AffiliationsGlobals.yaml' file
_AFFIL_KEEPING_PREFIX_TEMPLATE = Template(regex_globals_dic['affil_keeping_prefix_template'])

# Template for capturing words to keep the address chunck
# Words list specified in 'AffiliationsGlobals.yaml' file
_AFFIL_KEEPING_WORD_TEMPLATE = Template(regex_globals_dic['affil_keeping_word_template'])

# Dict of templates for capturing features to keep chuncks of addresses
AFFIL_KEEPING_PATTERNS_DIC = {'digits_prefix': _AFFIL_DIGITS_KEEPING_PREFIX_TEMPLATE,
                              'prefix'       : _AFFIL_KEEPING_PREFIX_TEMPLATE,
                              'word'         : _AFFIL_KEEPING_WORD_TEMPLATE,
                             }

# ------------Search of words in words set in addresses------------
# Templates for capturing for instance "word" in "word of set"
# or " word" in "set with word", or "word" in "Azert Word Azerty"
_AFFIL_WORDS_SET_TEMPLATE_LIST = regex_globals_dic['affil_words_set_template_list']
AFFIL_WORDS_SET_TEMPLATE = Template('|'.join(_AFFIL_WORDS_SET_TEMPLATE_LIST))

#
# Templates for capturing small words or accronyms given by the small_words_drop
# and the missing_space_acronyms globals in 'AffiliationsGlobals.yaml' file
# For instance capturing 'of' in 'technical university of denmark'
# capturing 'd' in 'institut d ingenierie'
# capturing 'the' in 'the denmark university'
# or capturing 'umr' in 'umr dddd' or 'umr dd'
_AFFIL_WORD_TO_DROP_TEMPLATE_LIST = regex_globals_dic['affil_word_to_drop_template_list']
AFFIL_WORD_TO_DROP_TEMPLATE = Template('|'.join(_AFFIL_WORD_TO_DROP_TEMPLATE_LIST))


# ****************************
# * References' parsing regex *
# ****************************
#
# ------Search of journals' names for Scopus and WoS data------

# List of journals' accronyms
_JOURNAL_ACRONYMS_LIST = regex_globals_dic['journal_acronyms_list']

# List of journals' short names
_JOURNAL_SHORTS_LIST = regex_globals_dic['journal_shorts_list']
_JOURNAL_SHORTS_LIST = (_JOURNAL_SHORTS_LIST
                       + [x.lower() for x in _JOURNAL_SHORTS_LIST]
                       + [x.upper() for x in _JOURNAL_SHORTS_LIST])

# List of journals' part names
_JOURNAL_WORDS_LIST = regex_globals_dic['journal_words_list']
_JOURNAL_WORDS_LIST = (_JOURNAL_WORDS_LIST
                      + [x.lower() for x in _JOURNAL_WORDS_LIST]
                      + [x.upper() for x in _JOURNAL_WORDS_LIST])

# Full list of journal's aliases (except short names)
_JOURNAL_FULLS_LIST =  _JOURNAL_WORDS_LIST + _JOURNAL_ACRONYMS_LIST

# Regex for capturing accronyms of journals
_SET_ACRONYMS_TEMPLATE = Template(regex_globals_dic['acronyms_template'])
RE_JOURNAL_ACRONYMS = re.compile('|'.join([_SET_ACRONYMS_TEMPLATE.substitute({"word":word})
                                           for word in _JOURNAL_ACRONYMS_LIST]))



# Templates for capturing dotted short names of journals
_SHORTS_TEMPLATES_LIST = regex_globals_dic['shorts_templates_list']
_SET_SHORTS_TEMPLATE = Template('|'.join(_SHORTS_TEMPLATES_LIST))

# Templates for capturing undotted short names of journals
_UNDOTTED_SHORTS_TEMPLATES_LIST = regex_globals_dic['undotted_shorts_templates_list']
_SET_UNDOTTED_SHORTS_TEMPLATE = Template('|'.join(_UNDOTTED_SHORTS_TEMPLATES_LIST))

# Templates for capturing full names of journals
_FULLS_TEMPLATES_LIST = regex_globals_dic['fulls_templates_list']
_SET_FULLS_TEMPLATE = Template('|'.join(_FULLS_TEMPLATES_LIST))


# ************************************************************************
# * Regex specific to parsing of publications' references in Scopus data *
# ************************************************************************

# Regex for capturing journals in references of scopus data
RE_SCOPUS_REF_JOURNAL = re.compile('|'.join([_SET_FULLS_TEMPLATE.substitute({"word":word})
                                             for word in _JOURNAL_FULLS_LIST])
                                   + '|' +
                                   '|'.join([_SET_SHORTS_TEMPLATE.substitute({"word":word})
                                             for word in _JOURNAL_SHORTS_LIST])
                                   + '|' +
                                   '|'.join([_SET_UNDOTTED_SHORTS_TEMPLATE.substitute({"word":word})
                                             for word in _JOURNAL_SHORTS_LIST]))

# ------Search of months' aliases ------

# Lists of aliases of months
_REF_DROPPING_SHORT_MONTHS_LIST = regex_globals_dic['ref_dropping_short_months_list']
_REF_DROPPING_FULL_MONTHS_LIST = regex_globals_dic['ref_dropping_full_months_list']
_REF_DROPING_ALL_MONTHS_LIST = _REF_DROPPING_SHORT_MONTHS_LIST + _REF_DROPPING_FULL_MONTHS_LIST

# Templates for capturing short names of months to drop references' chunck
_DROP_SHORT_MONTH_TEMPLATE_LIST = regex_globals_dic['drop_short_month_template_list']
_SET_DROP_SHORT_MONTH_TEMPLATE = Template('|'.join(_DROP_SHORT_MONTH_TEMPLATE_LIST))

# Templates for capturing short and full names of months to drop references' chunck
_DROP_ALL_MONTH_TEMPLATE_LIST = regex_globals_dic['drop_all_month_template_list']
_SET_DROP_ALL_MONTH_TEMPLATE = Template('|'.join(_DROP_ALL_MONTH_TEMPLATE_LIST))

# Regex for capturing aliases of months to drop references' chunck
RE_SCOPUS_REF_MONTHS_DROP = re.compile('|'.join([_SET_DROP_SHORT_MONTH_TEMPLATE.substitute({"word":word})
                                                 for word in _REF_DROPING_SHORT_MONTHS_LIST])
                                       + '|' +
                                       '|'.join([_SET_DROP_ALL_MONTH_TEMPLATE.substitute({"word":word})
                                                 for word in _REF_DROPING_ALL_MONTHS_LIST]))

# ------Search of other part of references------

# Regex for capturing "and" in references
_RE_SCOPUS_REF_AND_LIST = regex_globals_dic['re_scopus_ref_and_list']
RE_SCOPUS_REF_AND = re.compile('|'.join(_RE_SCOPUS_REF_AND_LIST))

# Regex for capturing authors' names chunks in reférences
_RE_SCOPUS_REF_AUTHOR_LIST = regex_globals_dic['re_scopus_ref_author_list']
RE_SCOPUS_REF_AUTHOR = re.compile('|'.join(_RE_SCOPUS_REF_AUTHOR_LIST))

# Regex for capturing authors' names in references
_RE_SCOPUS_REF_AUTHOR_DROP_LIST = regex_globals_dic['re_scopus_ref_author_drop_list']
RE_REF_AUTHOR_DROP = re.compile('|'.join(_RE_SCOPUS_REF_AUTHOR_DROP_LIST))

# Regex for capturing digits followed by text in references
_RE_SCOPUS_REF_DIGITS_DROP_LIST = regex_globals_dic['re_scopus_ref_digits_drop_list']
RE_SCOPUS_REF_DIGITS_DROP = re.compile('|'.join(_RE_SCOPUS_REF_DIGITS_DROP_LIST))

# Regex for capturing "et al." in references
_RE_SCOPUS_REF_ET_AL_LIST = regex_globals_dic['re_scopus_ref_et_al_list']
RE_SCOPUS_REF_ET_AL = re.compile('|'.join(_RE_SCOPUS_REF_ET_AL_LIST))

# Regex for capturing isolated digits in references
RE_SCOPUS_REF_ONLY_DIGITS = re.compile(regex_globals_dic['re_scopus_ref_only_digits'])

# Regex for capturing "pp. " followed by digits in references
RE_SCOPUS_REF_PAGES = re.compile(regex_globals_dic['re_scopus_ref_pages'])

# Regex for capturing dotted text in references
_RE_SCOPUS_REF_DOT_LIST = regex_globals_dic['re_scopus_ref_dot_list']
RE_SCOPUS_REF_DOT = re.compile('|'.join(_RE_SCOPUS_REF_DOT_LIST))

# Regex for capturing DOI in references
RE_SCOPUS_REF_DOI = re.compile(regex_globals_dic['re_scopus_ref_doi'])

# Regex for capturing 'Conference' in references
RE_SCOPUS_REF_CONF = re.compile(regex_globals_dic['re_scopus_ref_conf'])

# Regex for capturing isolated 4 consecutive digits in references
RE_SCOPUS_REF_DIGITS = re.compile(regex_globals_dic['re_scopus_ref_digits'])

# Regex for capturing 4-digits year inside DOI in references
RE_SCOPUS_REF_DOI_YEAR = re.compile(regex_globals_dic['re_scopus_ref_doi_year'])

# Regex for capturing non-alphanumeric characters in references
RE_SCOPUS_REF_SYMB = re.compile(regex_globals_dic['re_scopus_ref_symb'])

# Regex for capturing "dddd" within parenthesis in string or at end of string in references
RE_SCOPUS_REF_YEAR = re.compile(regex_globals_dic['re_scopus_ref_year'])

# Regex for capturing "dddd" after, before or within parenthesis in string
# or at end of string in references
_RE_SCOPUS_REF_YEARS_LIST = regex_globals_dic['re_scopus_ref_years_list']
RE_SCOPUS_REF_YEARS = re.compile('|'.join(_RE_SCOPUS_REF_YEARS_LIST))


# *********************************************************************
# * Regex specific to parsing of publications' references in WoS data *
# *********************************************************************

# List of journals' full names for references of WoS data)
_JOURNAL_NAMES_LIST = regex_globals_dic['journal_names_list']
_JOURNAL_NAMES_LIST = (_JOURNAL_NAMES_LIST
                      + [x.lower() for x in _JOURNAL_NAMES_LIST]
                      + [x.upper() for x in _JOURNAL_NAMES_LIST])

# Template for capturing journal's names in references of WoS data
_SET_NAMES_TEMPLATE = Template(regex_globals_dic['names_template'])

RE_WOS_REF_JOURNAL = re.compile('|'.join([_SET_FULLS_TEMPLATE.substitute({"word":word})
                                          for word in _JOURNAL_FULLS_LIST])
                                + '|' +
                                '|'.join([_SET_SHORTS_TEMPLATE.substitute({"word":word})
                                          for word in _JOURNAL_SHORTS_LIST])
                                + '|' +
                                '|'.join([_SET_NAMES_TEMPLATE.substitute({"word":word})
                                          for word in _JOURNAL_NAMES_LIST])
                                + '|' +
                                '|'.join([_SET_UNDOTTED_SHORTS_TEMPLATE.substitute({"word":word})
                                          for word in _JOURNAL_SHORTS_LIST]))

# Regex for capturing DOI in references of WoS data'
_RE_WOS_REF_DOI_LIST = regex_globals_dic['re_wos_ref_doi_list']
RE_WOS_REF_DOI = re.compile('|'.join(_RE_WOS_REF_DOI_LIST))

# Regex for capturing year in references of WoS data
RE_WOS_REF_YEAR = re.compile(regex_globals_dic['re_wos_ref_year'])

# Template for capturing special words to drop references' chunck in WoS data
_REF_DROPPING_WORDS_LIST = regex_globals_dic['ref_dropping_words_list']
_SET_DROP_WORD_TEMPLATE = Template(regex_globals_dic['drop_word_template'])
RE_SCOPUS_REF_WORDS_DROP = re.compile('|'.join([_SET_DROP_WORD_TEMPLATE.substitute({"word":word})
                                                for word in _REF_DROPING_WORDS_LIST]))


# **********************
# * No more used regex *
# **********************

# Regex for capturing string between "()" in journal name
RE_ADDS_JOURNAL = re.compile(regex_globals_dic['re_adds_journal'])

# Regex for search of journals' name begenning with one digit in Scopus data
RE_SCOPUS_JOURNAL_DIGITS = re.compile(regex_globals_dic['re_scopus_journal_digits'])

# Regex for search of authors' initials in Scopus data
_RE_SCOPUS_AUTHOR_INITIALS_LIST = regex_globals_dic['re_scopus_author_initials_list']
RE_SCOPUS_AUTHOR_INITIALS = re.compile('|'.join(_RE_SCOPUS_AUTHOR_INITIALS_LIST))

# Regex for search of authors' full name in Scopus data
_RE_SCOPUS_AUTHOR_NAME_LIST = regex_globals_dic['re_scopus_author_name_list']
RE_SCOPUS_AUTHOR_NAME = re.compile('|'.join(_RE_SCOPUS_AUTHOR_NAME_LIST))
