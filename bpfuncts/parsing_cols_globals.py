"""Module of global parameters for setting column-names used 
for rawdata parsings and concatenation/deduplication of parsings.
"""

__all__ = ['COL_NAMES',
           'COLUMN_LABEL_SCOPUS',
           'COLUMN_LABEL_SCOPUS_PLUS',
           'COLUMN_LABEL_WOS',
           'COLUMN_LABEL_WOS_PLUS',
           'COLUMN_TYPE_SCOPUS',
           'NORM_JOURNAL_COLUMN_LABEL',
           'USECOLS_SCOPUS',
           'USECOLS_WOS',
          ]


# *****************************
# * Parsing data column names *
# *****************************

# Particular column names
NORM_JOURNAL_COLUMN_LABEL = 'Norm_journal'

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


# ***********************
# * Scopus column names *
# ***********************

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


# This global is only useful for the merge of Scopus rawdata
_USECOLS_SCOPUS = '''Abstract,Affiliations,Authors,Author Keywords,Authors with affiliations,
                     CODEN,Document Type,DOI,EID,Index Keywords,ISBN,ISSN,Issue,Language of Original Document,
                     Page start,References,Source title,Title,Volume,Year'''
USECOLS_SCOPUS  = [x.strip() for x in _USECOLS_SCOPUS.split(',')]


# ********************
# * WoS column names *
# ********************

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


# This global is only useful for the merge of WoS rawdata
_USECOLS_WOS ='''AB,AU,BP,BS,C1,CR,DE,DI,DT,ID,IS,LA,PY,RP,
                SC,SN,SO,TI,UT,VL,WC'''
USECOLS_WOS  = [x.strip() for x in _USECOLS_WOS.split(',')]
