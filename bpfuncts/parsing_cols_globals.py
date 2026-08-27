"""Module of global parameters for setting column-names used 
for rawdata parsings and concatenation/deduplication of parsings.

The parameters values are set from the 'parsing_cols_globals.yaml' file 
available by default in the 'DemoConfig' folder of the package.
"""

__all__ = ['COL_NAMES',
           'COLUMN_LABEL_SCOPUS',
           'COLUMN_LABEL_SCOPUS_PLUS',
           'COLUMN_LABEL_WOS',
           'COLUMN_LABEL_WOS_PLUS',
           'COLUMN_TYPE_SCOPUS',
           'NORM_JOURNAL_COL_NAME',
          ]


# Local imports
from bpfuncts.globals_utils import read_yaml_parsing_cols_globals


# Getting the globals values from the YAML file of parsing globals
parsing_cols_globals_dic = read_yaml_parsing_cols_globals()


# ************************
# * Parsing column names *
# ************************

PARSING_COLS_DIC = parsing_cols_globals_dic['parsing_cols_dic']

COL_NAMES = {'pub_id'      : PARSING_COLS_DIC['pub_id'],
             'wos_id'      : [PARSING_COLS_DIC['wos_id'],
                              PARSING_COLS_DIC['pub_id'],
                             ],
             'scopus_id'   : [PARSING_COLS_DIC['scopus_id'],
                              PARSING_COLS_DIC['pub_id'],
                             ],
             'address'     : [PARSING_COLS_DIC['pub_id'],
                              PARSING_COLS_DIC['address_id'],
                              PARSING_COLS_DIC['address'],
                             ],
             'articles'    : [PARSING_COLS_DIC['pub_id'],
                              PARSING_COLS_DIC['authors'],
                              PARSING_COLS_DIC['year'],
                              PARSING_COLS_DIC['journal'],
                              PARSING_COLS_DIC['volume'],
                              PARSING_COLS_DIC['page'],
                              PARSING_COLS_DIC['doi'],
                              PARSING_COLS_DIC['doctype'],
                              PARSING_COLS_DIC['language'],
                              PARSING_COLS_DIC['title'],
                              PARSING_COLS_DIC['issn'],
                             ],
             'authors'     : [PARSING_COLS_DIC['pub_id'],
                              PARSING_COLS_DIC['author_id'],
                              PARSING_COLS_DIC['co_author'],
                             ],
             'auth_inst'   : [PARSING_COLS_DIC['pub_id'],
                              PARSING_COLS_DIC['author_id'],
                              PARSING_COLS_DIC['address'],
                              PARSING_COLS_DIC['country'],
                              PARSING_COLS_DIC['norm_affils'],
                              PARSING_COLS_DIC['raw_affils'],
                             ],
             'country'     : [PARSING_COLS_DIC['pub_id'],
                              PARSING_COLS_DIC['address_id'],
                              PARSING_COLS_DIC['country'],
                             ],
             'institution' : [PARSING_COLS_DIC['pub_id'],
                              PARSING_COLS_DIC['address_id'],
                             PARSING_COLS_DIC['affiliation]',
                             ],
             'keywords'    : [PARSING_COLS_DIC['pub_id'],
                              PARSING_COLS_DIC['keyword'],
                             ],
             'references'  : [PARSING_COLS_DIC['pub_id'],
                              PARSING_COLS_DIC['authors'],
                              PARSING_COLS_DIC['year'],
                              PARSING_COLS_DIC['journal'],
                              PARSING_COLS_DIC['doi'],
                              PARSING_COLS_DIC['title'],
                              PARSING_COLS_DIC['full_ref'],
                             ],
             'subject'     : [PARSING_COLS_DIC['pub_id'],
                              PARSING_COLS_DIC['subject'],
                             ],
             'sub_subject' : [PARSING_COLS_DIC['pub_id'],
                              PARSING_COLS_DIC['sub_subject'],
                             ],
             'temp_col'    : [PARSING_COLS_DIC['title_lowercase'],
                              PARSING_COLS_DIC['dedup_same_journal'],
                              PARSING_COLS_DIC['title'],
                              PARSING_COLS_DIC['title_tokens'],
                              PARSING_COLS_DIC['kept_tokens'],
                              PARSING_COLS_DIC['doctype_lowercase'],
                              PARSING_COLS_DIC['doi_lowercase'],
                             ],
            }

# Particular column names
NORM_JOURNAL_COL_NAME = PARSING_COLS_DIC['norm_journal']

# ***********************
# * Scopus column names *
# ***********************

SCOPUS_COLS_LABELS_DIC = parsing_cols_globals_dic['scopus_cols_labels_dic']

SCOPUS_COL_LABEL_LIST = ['affiliations',
                         'author_keywords',
                         'authors',
                         'authors_with_affiliations',
                         'document_type',
                         'doi',
                         'index_keywords',
                         'issn',
                         'journal',
                         'language',
                         'page_start',
                         'references',
                         'title',
                         'volume',
                         'year',
                        ]

SCOPUS_COL_LABEL_PLUS_LIST = ['scopus_id',
                              'auth_fullnames',
                             ]

COLUMN_LABEL_SCOPUS = [SCOPUS_COLS_LABELS_DIC[key][0] for key in SCOPUS_COL_LABEL_LIST]

COLUMN_TYPE_SCOPUS =  [SCOPUS_COLS_LABELS_DIC[key][1] for key in SCOPUS_COL_LABEL_LIST]

COLUMN_LABEL_SCOPUS_PLUS = [SCOPUS_COLS_LABELS_DIC[key][0] for key in SCOPUS_COL_LABEL_PLUS_LIST]


# ********************
# * WoS column names *
# ********************

WOS_COLS_LABELS_DIC = parsing_cols_globals_dic['wos_cols_labels_dic']

WOS_COL_LABEL_LIST = ['affiliations',
                      'author_keywords',
                      'authors',
                      'authors_with_affiliations',
                      'document_type',
                      'doi',
                      'index_keywords',
                      'issn',
                      'journal',
                      'language',
                      'page_start',
                      'references',
                      'subjects',
                      'sub_subjects',
                      'title',
                      'volume',
                      'year',
                     ]

WOS_COL_LABEL_PLUS_LIST = ['e_issn',
                           'wos_id',
                          ]

COLUMN_LABEL_WOS = [WOS_COLS_LABELS_DIC[key] for key in WOS_COL_LABEL_LIST]

COLUMN_LABEL_WOS_PLUS = [WOS_COLS_LABELS_DIC[key] for key in WOS_COL_LABEL_PLUS_LIST]
