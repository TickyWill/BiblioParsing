__all__ = ['biblio_parser_scopus',
           'read_database_scopus']


# Standard library imports
import json
import re
from collections import namedtuple
from collections import Counter
from operator import attrgetter
from pathlib import Path

# 3rd party library imports
import numpy as np
import pandas as pd

# Local libray imports
import BiblioParsing.BiblioGeneralGlobals as bp_gg
import BiblioParsing.BiblioRegexpGlobals as bp_rg
import BiblioParsing.BiblioSpecificGlobals as bp_sg
from BiblioParsing.BiblioParsingInstitutions import address_inst_full_list
from BiblioParsing.BiblioParsingInstitutions import build_norm_raw_affiliations_dict
from BiblioParsing.BiblioParsingInstitutions import extend_author_institutions
from BiblioParsing.BiblioParsingInstitutions import read_inst_types
from BiblioParsing.BiblioParsingInstitutions import read_towns_per_country
from BiblioParsing.BiblioParsingUtils import build_item_df_from_tup
from BiblioParsing.BiblioParsingUtils import build_pub_db_ids
from BiblioParsing.BiblioParsingUtils import build_title_keywords
from BiblioParsing.BiblioParsingUtils import check_and_drop_columns
from BiblioParsing.BiblioParsingUtils import check_and_get_rawdata_file_path
from BiblioParsing.BiblioParsingUtils import clean_authors_countries_institutions
from BiblioParsing.BiblioParsingUtils import normalize_country
from BiblioParsing.BiblioParsingUtils import normalize_journal_names
from BiblioParsing.BiblioParsingUtils import normalize_name
from BiblioParsing.BiblioParsingUtils import remove_special_symbol
from BiblioParsing.BiblioParsingUtils import set_unknown_address
from BiblioParsing.BiblioParsingUtils import standardize_address


def _set_scopus_parsing_cols():
    """Builds 3 dict setting columns list and selected columns names 
    for the process of parsing Scopus rawdata.

    Returns:
        (tup): (A dict valued by column-names lists for each parsing item \
        and temporary column names defined by the 'COL_NAMES' global, \
        A dict valued by column names of parsing results defined by the \
        'COL_NAMES' global, A dict valued by column names of rawdata defined \
        by the 'COLUMN_LABEL_SCOPUS' and 'COLUMN_LABEL_SCOPUS_PLUS' globals).
    """
    cols_lists_dic = {'articles_cols_list' : bp_sg.COL_NAMES['articles'],
                      'address_cols_List'  : bp_sg.COL_NAMES['address'],
                      'auth_cols_list'     : bp_sg.COL_NAMES['authors'],
                      'auth_inst_cols_list': bp_sg.COL_NAMES['auth_inst'],
                      'country_cols_list'  : bp_sg.COL_NAMES['country'],
                      'inst_cols_list'     : bp_sg.COL_NAMES['institution'],
                      'kw_cols_List'       : bp_sg.COL_NAMES['keywords'],
                      'tmp_cols_list'      : bp_sg.COL_NAMES['temp_col'],
                      'ref_cols_list'      : bp_sg.COL_NAMES['references'],
                     }

    cols_dic = {'scopus_id_col'           : bp_sg.COL_NAMES['scopus_id'][0],
                'pub_id_col'              : bp_sg.COL_NAMES['pub_id'],
                'subject_col'             : bp_sg.COL_NAMES['subject'][1],
                'sub_subject_col'         : bp_sg.COL_NAMES['sub_subject'][1],
                'auth_inst_author_idx_col': bp_sg.COL_NAMES['auth_inst'][1],
                'norm_institution_col'    : bp_sg.COL_NAMES['auth_inst'][4],
                'address_col'             : bp_sg.COL_NAMES['address'][2],
                'country_col'             : bp_sg.COL_NAMES['country'][2],
                'institution_col'         : bp_sg.COL_NAMES['institution'][2],
                'author_idx_col'          : bp_sg.COL_NAMES['authors'][1],
                'co_authors_col'          : bp_sg.COL_NAMES['authors'][2],
                'keyword_col'             : bp_sg.COL_NAMES['keywords'][1],
                'title_temp_col'          : bp_sg.COL_NAMES['temp_col'][2],
                'kept_tokens_col'         : bp_sg.COL_NAMES['temp_col'][4],
                'author_col'              : bp_sg.COL_NAMES['articles'][1],
                'year_col'                : bp_sg.COL_NAMES['articles'][2],
                'doc_type_col'            : bp_sg.COL_NAMES['articles'][7], 
                'title_col'               : bp_sg.COL_NAMES['articles'][9],
                'issn_col'                : bp_sg.COL_NAMES['articles'][10],
                'norm_journal_col'        : bp_sg.NORM_JOURNAL_COLUMN_LABEL,
               }
    
    scopus_cols_dic = {'scopus_auth_col'         : bp_sg.COLUMN_LABEL_SCOPUS['authors'],                   
                       'scopus_title_kw_col'     : bp_sg.COLUMN_LABEL_SCOPUS['title'],
                       'scopus_year_col'         : bp_sg.COLUMN_LABEL_SCOPUS['year'],
                       'scopus_journal_col'      : bp_sg.COLUMN_LABEL_SCOPUS['journal'],
                       'scopus_volume_col'       : bp_sg.COLUMN_LABEL_SCOPUS['volume'],
                       'scopus_page_col'         : bp_sg.COLUMN_LABEL_SCOPUS['page_start'],
                       'scopus_doi_col'          : bp_sg.COLUMN_LABEL_SCOPUS['doi'],
                       'scopus_aff_col'          : bp_sg.COLUMN_LABEL_SCOPUS['affiliations'],
                       'scopus_auth_with_aff_col': bp_sg.COLUMN_LABEL_SCOPUS['authors_with_affiliations'],
                       'scopus_auth_kw_col'      : bp_sg.COLUMN_LABEL_SCOPUS['author_keywords'],
                       'scopus_idx_kw_col'       : bp_sg.COLUMN_LABEL_SCOPUS['index_keywords'],
                       'scopus_ref_col'          : bp_sg.COLUMN_LABEL_SCOPUS['references'],
                       'scopus_issn_col'         : bp_sg.COLUMN_LABEL_SCOPUS['issn'],
                       'scopus_language_col'     : bp_sg.COLUMN_LABEL_SCOPUS['language'],
                       'scopus_doctype_col'      : bp_sg.COLUMN_LABEL_SCOPUS['document_type'],
                       'scopus_fullnames_col'    : bp_sg.COLUMN_LABEL_SCOPUS_PLUS['auth_fullnames'],
                       'init_scopus_id_col'      : bp_sg.COLUMN_LABEL_SCOPUS_PLUS['scopus_id'],
                      }

    return cols_lists_dic, cols_dic, scopus_cols_dic


def _set_author_idx(author, author_counter_params):
    # Updating author's counter and last-author name
    author_idx, last_author = author_counter_params
    if author!=last_author:
        author_idx += 1
    last_author = author
    author_counter_params = author_idx, last_author
    return author_counter_params


def _get_author_affiliations_list(raw_author_affiliations_str, affiliations_list,
                                  author_counter_params):
    std_author_affiliations_str = standardize_address(raw_author_affiliations_str,
                                                      add_unknown_country=False)
    author_affiliations_list = std_author_affiliations_str.split(',')

    # Using change in scopus on 07/2023 for authors' names
    auth_item_nbr = 2
    if "." in author_affiliations_list[0]:
        auth_item_nbr = 1
    author = (','.join(author_affiliations_list[0:auth_item_nbr])).strip()        
    author_counter_params = _set_author_idx(author, author_counter_params)

    # Building "addr_country_inst" namedtuple for the author of the publication
    author_affiliations_str = ','.join(author_affiliations_list[auth_item_nbr:])

    author_std_affiliations_list = []
    for raw_affiliation in affiliations_list:
        std_affiliation = standardize_address(raw_affiliation,
                                              add_unknown_country=False)
        if std_affiliation in author_affiliations_str:
            full_std_affiliation = standardize_address(raw_affiliation,
                                                       add_unknown_country=True)
            author_std_affiliations_list.append(full_std_affiliation)
    return author, author_std_affiliations_list, author_counter_params


def _build_authors_scopus(corpus_df, fails_dic, cols_tup):
    """Builds the data of the co-authors of each publication of the corpus 
    and updates the parsing success rate data.

    The structure of the built data is composed of 3 columns and one row 
    per publication and per co-author.
        Ex:
            pub_id  idx_author   co-author
               0      0          Boujjat H.
               0      1          Rodat S.

    Args:
        corpus_df (dataframe): The selected rawdata of the corpus.
        fails_dic (dict): Parsing success rate data.
        cols_tup (tup): Columns information as built through \
        the `_set_scopus_parsing_cols` internal function.
    Returns:
        (dataframe): The built data.
    """
    # Setting useful column names
    cols_lists_dic, cols_dic, scopus_cols_dic = cols_tup
    auth_cols_list = cols_lists_dic['auth_cols_list']
    cols_keys = ['pub_id_col', 'co_authors_col', ]
    (pub_id_col, co_authors_col) = [cols_dic[key] for key in cols_keys]
    scopus_auth_col = scopus_cols_dic['scopus_auth_col']
    
    # Setting named tuple
    co_author = namedtuple('co_author', auth_cols_list)
    
    authors_list = []
    for pub_id, scopus_auth_str in zip(corpus_df[pub_id_col], corpus_df[scopus_auth_col]):
        author_idx = 0
        authors_sep = ',' 
        if ';' in scopus_auth_str:
            # Change in scopus on 07/2023
            authors_sep = ';'
        scopus_auth_list = scopus_auth_str.split(authors_sep)
        for scopus_auth in scopus_auth_list:
            author = scopus_auth.replace('.','')
            if author not in ['Dr','Pr','Dr ','Pr ']:
                authors_list.append(co_author(pub_id, author_idx, author))
                author_idx += 1

    # Building a clean co-authors dataframe
    # and accordingly updating the parsing success rate dict
    co_authors_df, fails_dic = build_item_df_from_tup(authors_list, auth_cols_list,
                                                      co_authors_col, pub_id_col, fails_dic)    
    return co_authors_df


def _build_keywords_scopus(corpus_df, fails_dic, cols_tup):
    """Builds the data of keyword" per publication of the corpus 
    and updates the parsing success rate data.

    The structure of the built data is composed of 3 columns and one row 
    per publication and per keyword type.
        Ex:
           pub_id  type  keyword
             0      AK    Biomass
             0      IK    Gasification
             0      TK    Solar energy
        with: 
             type = AK for author's keywords 
             type = IK for indexed keywords
             type = TK for title keywords 
         
    The author's keywords and the indexed keywords are directly extracted from \
    the corpus data. 
    The title keywords are builds out of the 'TK_corpus' set of the most cited nouns 
    (at leat N times) in the set of all the publications. The keywords of type TK of a 
    publication, referenced by the 'pub_id' key, are the elements of the intersection 
    between the 'TK_corpus' set and the set of the nouns of the publication title.

    Args:
        corpus_df (dataframe): The selected rawdata of the corpus.
        fails_dic (dict): Parsing success rate data.
        cols_tup (tup): Columns information as built through \
        the `_set_scopus_parsing_cols` internal function.
    Returns:
        (dataframe): The built data.
    """
    # To Do: Check the use of UNKNOWN versus '"null"'
    # Setting useful column names
    cols_lists_dic, cols_dic, scopus_cols_dic = cols_tup
    kw_cols_List = cols_lists_dic['kw_cols_List']
    cols_keys = ['pub_id_col', 'keyword_col', 'title_temp_col', 'kept_tokens_col']
    (pub_id_col, keyword_col, title_temp_col, kept_tokens_col) = [cols_dic[key] for key in cols_keys]
    scopus_cols_keys = ['scopus_auth_kw_col', 'scopus_idx_kw_col', 'scopus_title_kw_col']
    (scopus_auth_kw_col, scopus_idx_kw_col,
     scopus_title_kw_col )= [scopus_cols_dic[key] for key in scopus_cols_keys]

    # Setting named tuple
    key_word = namedtuple('key_word', kw_cols_List)

    aks_list = []
    aks_df = corpus_df[scopus_auth_kw_col].fillna('')
    for pub_id, pub_aks_str in zip(corpus_df[pub_id_col], aks_df):
        pub_aks_list = pub_aks_str.split(';')
        for pub_ak in pub_aks_list:
            pub_ak = pub_ak.lower().strip()
            aks_list.append(key_word(pub_id, pub_ak if pub_ak!='null' else bp_sg.UNKNOWN))

    iks_list = []
    iks_df = corpus_df[scopus_idx_kw_col].fillna('')
    for pub_id, pub_iks_str in zip(corpus_df[pub_id_col], iks_df):
        pub_iks_list = pub_iks_str.split(';')
        for pub_ik in pub_iks_list:
            pub_ik = pub_ik.lower().strip()
            iks_list.append(key_word(pub_id, pub_ik if pub_ik!='null' else bp_sg.UNKNOWN))

    tks_list = []
    title_df = pd.DataFrame(corpus_df[scopus_title_kw_col].fillna(''))
    title_df.columns = [title_temp_col]
    tks_df, list_of_words_occurrences = build_title_keywords(title_df)
    for pub_id in corpus_df[pub_id_col]:
        for token in tks_df.loc[pub_id, kept_tokens_col]:
            token = token.lower().strip()
            tks_list.append(key_word(pub_id, token if token!='null' else bp_sg.UNKNOWN))

    # Building a clean author keywords dataframe and accordingly updating the parsing success rate dict
    ak_keywords_df, fails_dic = build_item_df_from_tup(aks_list, kw_cols_List,
                                                       keyword_col, pub_id_col, fails_dic)

    # Building a clean index keywords dataframe and accordingly updating the parsing success rate dict
    ik_keywords_df, fails_dic = build_item_df_from_tup(iks_list, kw_cols_List,
                                                       keyword_col, pub_id_col, fails_dic)

    # Building a clean title keywords dataframe and accordingly updating the parsing success rate dict
    tk_keywords_df, fails_dic = build_item_df_from_tup(tks_list, kw_cols_List,
                                                       keyword_col, pub_id_col, fails_dic)

    return ak_keywords_df, ik_keywords_df, tk_keywords_df


def _build_addresses_countries_institutions_scopus(corpus_df, fails_dic, cols_tup):
    """Builds the data of addresses, countries and main affiliations 
    per publications of the corpus and updates the parsing success rate data.

    The structure of the built data is composed of 3 columns and one row 
    per publication and per address identifier.
        Ex:
        From the following affiliations information of Scopus raw data 
        for the publication identified by Pub_id=0:

            'NaMLab, TU Dresden, Nothnitzer Str. 64a, Dresden, 01187, Germany; 
            Univ. Grenoble Alpes, Grenoble, F-38000, France;
            Hitachi Cambridge Laboratory, Cambridge, United Kingdom' 

        The built data will be as follows.
        - for the addresses data:

             Pub_id  Idx_address  Address
               0         0         NaMLab, TU Dresden, Nothnitzer Str. 64a, Dresden, 01187, Germany
               0         1         University Grenoble Alpes, Grenoble, F-38000, France
               0         2         Hitachi Cambridge Laboratory, Cambridge, United Kingdom

        - for the countries data:

             Pub_id  Idx_address  Country
               0         0         Germany
               0         1         France
               0         2         United Kingdom

        - for the main affiliations data:

             Pub_id  Idx_address  Institution
               0         0         NaMLab
               0         1         University Grenoble Alpes
               0         2         Hitachi Cambridge Laboratory

    Args:
        corpus_df (dataframe): The selected rawdata of the corpus.
        fails_dic (dict): Parsing success rate data.
        cols_tup (tup): Columns information as built through \
        the `_set_scopus_parsing_cols` internal function.
    Returns:
        (tup): (The built addresses data (dataframe), tha built countries data (dataframe), \
        The built main affiliations data (dataframe)).
    """
    # Setting useful column names
    cols_lists_dic, cols_dic, scopus_cols_dic = cols_tup
    cols_lists_keys = ['address_cols_List', 'country_cols_list', 'inst_cols_list']
    address_cols_List, country_cols_list, inst_cols_list = [cols_lists_dic[key] for key in cols_lists_keys]
    cols_keys = ['pub_id_col', 'address_col', 'country_col', 'institution_col']
    (pub_id_col, address_col, country_col, institution_col) = [cols_dic[key] for key in cols_keys]
    scopus_cols_keys = ['scopus_aff_col', 'scopus_auth_with_aff_col']
    (scopus_aff_col, scopus_auth_with_aff_col) = [scopus_cols_dic[key] for key in scopus_cols_keys]

    # Setting named tuples
    address_tup = namedtuple('address', address_cols_List)
    country_tup = namedtuple('country', country_cols_list)
    institution_tup = namedtuple('institution', inst_cols_list)

    # Building "addresses_list", "countries_list", "institutions_list" lists 
    # with one item per publication and per address identifier
    corpus_series_zip = zip(corpus_df[pub_id_col],
                            corpus_df[scopus_aff_col],
                            corpus_df[scopus_auth_with_aff_col])
    addresses_list, countries_list, institutions_list = [], [], []       
    for pub_id, affiliations_str, authors_affiliations_str in corpus_series_zip:
        affiliations_list = affiliations_str.split(';')

        # Initializing the authors' counter and the last-author name
        author_counter_params = [-1, '']

        # Checking if all authors have affiliation
        authors_affiliations_list = authors_affiliations_str.split(';')
        for raw_author_affiliations_str in authors_affiliations_list:
            return_tup = _get_author_affiliations_list(raw_author_affiliations_str, affiliations_list,
                                                       author_counter_params)
            author, author_std_affiliations_list, author_counter_params = return_tup
            author_idx = author_counter_params[0]
            if not author_std_affiliations_list:
                affiliations_list.append(set_unknown_address(author_idx)) 

        if affiliations_list:
            for address_idx, pub_address in enumerate(affiliations_list):
                addresses_list.append(address_tup(pub_id, address_idx, pub_address))

                addresses_split = pub_address.split(',')
                inst_nb = len(addresses_split)
                inst_num = 0
                main_institution = addresses_split[inst_num]                    
                if not main_institution and inst_nb:
                    while not main_institution and inst_num<inst_nb:
                        inst_num += 1
                        main_institution = pub_address.split(',')[inst_num]
                institutions_list.append(institution_tup(pub_id, address_idx,
                                                         main_institution))
                
                country_raw = pub_address.split(',')[-1].replace(';','').strip()  
                country = normalize_country(country_raw)
                countries_list.append(country_tup(pub_id, address_idx, country))
        else:
            addresses_list.append(address_tup(pub_id, 0, ''))
            institutions_list.append(institution_tup(pub_id, 0, ''))
            countries_list.append(country_tup(pub_id, 0, ''))

    # Building a clean addresses dataframe and accordingly updating the parsing success rate dict
    address_df, fails_dic = build_item_df_from_tup(addresses_list, address_cols_List,
                                                   address_col, pub_id_col, fails_dic)

    # Building a clean countries dataframe and accordingly updating the parsing success rate dict
    country_df, fails_dic = build_item_df_from_tup(countries_list, country_cols_list,
                                                   country_col, pub_id_col, fails_dic)
    
    # Building a clean institutions dataframe and accordingly updating the parsing success rate dict
    institution_df, fails_dic = build_item_df_from_tup(institutions_list, inst_cols_list,
                                                       institution_col, pub_id_col, fails_dic)
    
    if not(len(address_df)==len(country_df)==len(institution_df)):
        warning = ('\nWARNING: Lengths of "address_df", "country_df" and "institution_df" dataframes are not equal '
                   'in "_build_addresses_countries_institutions_scopus" function of "BiblioParsingScopus.py" module')
        print(warning)
    return address_df, country_df, institution_df


def _build_authors_countries_institutions_scopus(corpus_df, fails_dic, cols_tup, inst_filter_list=None,
                                                 country_affiliations_file_path=None,
                                                 inst_types_file_path=None,
                                                 country_towns_file=None,
                                                 country_towns_folder_path=None):
    """Parses the fields 'Affiliations' and 'Authors with affiliations' of the corpus to build 
    the data of authors their addresses, country and normalized affiliations per publication of the corpus. 

    The parsing success rate data are updated. 
    In addition, the built data may be expanded according to a filtering of affiliations. 
    The parsing is effective only for the format of the following example. Otherwise, the parsing 
    fields are set to empty strings.
       
    For example, the 'Authors with affiliations' field string:

       'Boujjat, H., CEA, LITEN Solar & Thermodynam Syst Lab L2ST, F-38054 Grenoble, France,
        Univ Grenoble Alpes, F-38000 Grenoble, France; 
        Rodat, S., CNRS, Proc Mat & Solar Energy Lab, PROMES, 7 Rue Four Solaire, F-66120 Font Romeu, France;
        Chuayboon, S., CNRS, Proc Mat & Solar Energy Lab, PROMES, 7 Rue Four Solaire, F-66120 Font Romeu, France;
        Abanades, S., CEA, Leti, 17 rue des Martyrs, F-38054 Grenoble, France;
        Dupont, S., CEA, Liten, INES. 50 avenue du Lac Leman, F-73370 Le Bourget-du-Lac, France;
        Durand, M., CEA, INES, DTS, 50 avenue du Lac Leman, F-73370 Le Bourget-du-Lac, France;
        David, D., Lund University, Department of Physical Geography and Ecosystem Science (INES), Lund, Sweden'

     will be parsed in the "affil_country_inst_df" dataframe if affiliation filter is not defined (initialization step):
   
        Pub_id  Idx_author                     Address               Country    Norm_institutions              Raw_institutions     
            0       0        CEA, LITEN Solar & Thermodynam , ...    France     CEA Nro;LITEN Rto              F-38054 Grenoble
            0       0        Univ Grenoble Alpes,...                 France     UGA Univ                       F-38000 Grenoble
            0       1        CNRS, Proc Mat Lab, PROMES,...          France     CNRS Nro;PROMES CNRS-Lab       7 Rue Four Solaire;...                          
            0       2        CNRS, Proc Mat Lab, PROMES, ...         France     CNRS Nro;PROMES CNRS-Lab       7 Rue Four Solaire;...    
            0       3        CEA, Leti, 17 rue des Martyrs,...       France     CEA Nro;LETI Rto               17 rue des Martyrs;...         
            0       4        CEA, Liten, INES. 50 avenue...          France     CEA Nro;LITEN Rto;INES Site    50 avenue du Lac Leman;...           
            0       5        CEA, INES, DTS, 50 avenue...            France     CEA Nro;INES Site              DTS;...
            0       6        Lund University,...(INES),...           Sweden     Lund Univ                      Department of Physical ...
        
    given that the 'Affiliations' field string is:
        
        'CEA, LITEN Solar & Thermodynam Syst Lab L2ST, F-38054 Grenoble, France; 
         Univ Grenoble Alpes, F-38000 Grenoble, France; 
         CNRS, Proc Mat & Solar Energy Lab, PROMES, 7 Rue Four Solaire, F-66120 Font Romeu, France; 
         CEA, Leti, 17 rue des Martyrs, F-38054 Grenoble, France; 
         CEA, Liten, INES. 50 avenue du Lac Leman, F-73370 Le Bourget-du-Lac, France; 
         CEA, INES, DTS, 50 avenue du Lac Leman, F-73370 Le Bourget-du-Lac, France; 
         Lund University, Department of Physical Geography and Ecosystem Science (INES), Lund, Sweden'
        
    The institutions are identified and normalized using dedicated data that should be specified by the user.
        
    If affiliation filter is defined based on the following list of normalized affiliations: 
        inst_filter_list = ['LITEN Rto', 'INES Campus', 'PROMES CNRS-Lab'), 'Lund Univ']. 
        
    The "addr_country_inst_df" dataframe will be expended with the following columns (for pub_id = 0):
            LITEN Rto  INES Campus    PROMES CNRS-Lab     Lund Univ                   
                 1            0              0                0             
                 0            0              0                0                              
                 0            0              1                0                     
                 0            0              1                0             
                 0            0              0                0             
                 1            1              0                0                              
                 0            1              0                0                              
                 0            0              0                1                      

    Args:
        corpus_df (dataframe): The selected rawdata of the corpus.
        fails_dic (dict): Parsing success rate data.
        cols_tup (tup): Columns information as built through the `_set_scopus_parsing_cols` internal function.
        inst_filter_list (list): The affiliations-filter composed of a list of normalized affiliations (str), \
        optional (default=None).
        country_affiliations_file_path (path): The full path to the data per country of raw affiliations \
        per normalized one, optional (default=None).
        inst_types_file_path (path): The full path to the data of institutions-types used to normalize \
        the affiliations, optional (default=None).
        country_towns_file (str): The name of the file of the data of towns per country, optional (default=None).
        country_towns_folder_path (path): The full path to the folder where the 'country_towns_file' file \
        is available, optional (default=None).
    Returns:
        (dataframe): The built data.
    Notes:
        When the 'country_affiliations_file_path', 'inst_types_file_path', 'country_towns_folder_path' \
        and 'country_towns_file' args are set to None, the values are defined by default internally to \
        the `build_norm_raw_affiliations_dict`, `read_inst_types` and `read_towns_per_country` \
        functions imported from the `BiblioParsingInstitutions` module.
    """
    # Setting useful column names
    cols_lists_dic, cols_dic, scopus_cols_dic = cols_tup
    auth_inst_cols_list = cols_lists_dic['auth_inst_cols_list']
    cols_keys = ['pub_id_col', 'auth_inst_author_idx_col', 'norm_institution_col']
    (pub_id_col, author_idx_col, norm_institution_col) = [cols_dic[key] for key in cols_keys]
    scopus_cols_keys = ['scopus_aff_col', 'scopus_auth_with_aff_col']
    (scopus_aff_col, scopus_auth_with_aff_col) = [scopus_cols_dic[key] for key in scopus_cols_keys]

    # Setting named tuples
    addr_country_inst  = namedtuple('address', auth_inst_cols_list[:-1])

    # Building the useful data for affiliations normalization
    norm_raw_aff_dict = build_norm_raw_affiliations_dict(country_affiliations_file_path=country_affiliations_file_path,
                                                         verbose=False)
    aff_type_dict = read_inst_types(inst_types_file_path=inst_types_file_path, inst_types_usecols=None)
    towns_dict = read_towns_per_country(country_towns_file=country_towns_file,
                                        country_towns_folder_path=country_towns_folder_path)

    # Building the "addr_country_inst_list" list
    # with one item per publication and per author identifier
    corpus_series_zip = zip(corpus_df[pub_id_col],
                            corpus_df[scopus_aff_col],
                            corpus_df[scopus_auth_with_aff_col])
    pub_nb = len(corpus_df[pub_id_col])
    pub_num = 0
    addr_country_inst_list = []
    for pub_id, affiliations_str, authors_affiliations_str in corpus_series_zip:
        pub_num += 1 
        print("    Publications number:", pub_num, f"/ {pub_nb}", end="\r")
        # Initializing the authors' counter and the last-author name
        author_counter_params = [-1, '']

        affiliations_list = affiliations_str.split(';')
        authors_affiliations_list = authors_affiliations_str.split(';')

        for raw_author_affiliations_str in authors_affiliations_list:
            return_tup = _get_author_affiliations_list(raw_author_affiliations_str, affiliations_list,
                                                       author_counter_params)
            author, author_std_affiliations_list, author_counter_params = return_tup
            author_idx = author_counter_params[0]
            if not author_std_affiliations_list:
                full_unknown_address = set_unknown_address(author_idx, add_unknown_country=True)
                author_std_affiliations_list.append(full_unknown_address)

            for author_std_affiliation in author_std_affiliations_list:
                author_country_raw = author_std_affiliation.split(',')[-1].strip()
                author_country = normalize_country(author_country_raw)
                author_institutions_tup = address_inst_full_list(author_std_affiliation, norm_raw_aff_dict,
                                                                 aff_type_dict, towns_dict,
                                                                 drop_status=False)
                addr_country_inst_list.append(addr_country_inst(pub_id, author_idx, author_std_affiliation, author_country,
                                                                author_institutions_tup.norm_inst_list,
                                                                author_institutions_tup.raw_inst_list,))
    # Building a clean author-country-institutions data and accordingly updating the parsing success rate dict
    addr_country_inst_df, fails_dic = build_item_df_from_tup(addr_country_inst_list, auth_inst_cols_list[:-1],
                                                             norm_institution_col, pub_id_col, fails_dic)    
    addr_country_inst_df = clean_authors_countries_institutions(addr_country_inst_df)

    if inst_filter_list is not None:
        addr_country_inst_df = extend_author_institutions(addr_country_inst_df, inst_filter_list)

    # Sorting the values in the dataframe returned by two columns
    addr_country_inst_df.sort_values(by = [pub_id_col, author_idx_col], inplace=True)
    return addr_country_inst_df


def _build_subjects_scopus(corpus_df, scopus_cat_codes_path,
                           scopus_journals_issn_cat_path, fails_dic, cols_tup):
    """Builds the data of subject per publication of the corpus 
    and updates the parsing success rate data.
    
    The structure of the built data is composed of 2 columns and one row 
    per publication and subject.
        Ex:
            Pub_id   Subject
              0      Mathematics
              0      Engineering
              1	     Physics and Astronomy
              1	     Biochemistry, Genetics and Molecular Biology

    The subjects are attributed using 2 files provided by Elsevier.
    The "scopus_cat_codes.txt" file gives a code per category:
    
                         Category            Code    
            General Medicine                 2700    => Subject
            Medicine (miscellaneous)         2701    => Sub-subject
            Anatomy                          2702
            Anesthesiology and Pain Medicine 2703
            Biochemistry, medical            2704
            ...
    
    The "scopus_journals_issn_cat.txt" file give the categories codes 
    attached to a journal:
    
                Journal            ISSN           Codes
            21st Century Music   15343219     1210; 
            2D Materials                      2210; 2211; 3104; 2500; 1600; 
            3 Biotech            2190572X     1101; 2301; 1305;
            ...

    For "2D Materials journal":
        - The subjects are given by the codes multiple of 100: 2500; 1600
        - The sub-subjects are given by the other codes: 2210; 2211; 3104

    Args:
        corpus_df (dataframe): The selected rawdata of the corpus.
        scopus_cat_codes_path (path): The full path to the file \
        "scopus_cat_codes.txt".
        scopus_journals_issn_cat_path=None): The full path to the file \
        "scopus_journals_issn_cat.txt".
        fails_dic (dict): Parsing success rate data.
        cols_tup (tup): Columns information as built through \
        the `_set_scopus_parsing_cols` internal function.
    Returns:
        (dataframe): The built data.
    """
    # Setting useful column names
    _, cols_dic, scopus_cols_dic = cols_tup
    cols_keys = ['pub_id_col', 'subject_col']
    (pub_id_col, subject_col) = [cols_dic[key] for key in cols_keys]
    scopus_cols_keys = ['scopus_journal_col', 'scopus_issn_col']
    (scopus_journal_col, scopus_issn_col) = [scopus_cols_dic[key] for key in scopus_cols_keys]

    # Builds the dict "code_cat" {ASJC classification codes:description} out 
    # of the file "scopus_cat_codes.txt"
    # ex: {1000: 'Multidisciplinary', 1100: 'General Agricultural',...}
    # -----------------------------------------------------------------------
    scopus_cat_codes_df = pd.read_csv(scopus_cat_codes_path, sep='\t', header=None)
    code_cat = dict(zip(scopus_cat_codes_df[1].fillna(0.0).astype(int), scopus_cat_codes_df[0]))

    # Builds the dataframe "scopus_journals_issn_cat_df" out of the file
    # "scopus_journals_issn_cat.txt"
    # "scopus_journals_issn_cat_df" has 3 columns:
    #       "journal": scopus journal name
    #       "issn": journal issn
    #       "keyword_id": list of keywords id asociated to the journal or the issn
    # -----------------------------------------------------------------------------
    scopus_journals_issn_cat_df = pd.read_csv(scopus_journals_issn_cat_path, sep='\t',
                                              header=None).fillna(0) 
    scopus_journals_issn_cat_df[2] = scopus_journals_issn_cat_df[2].str.split(';')
    scopus_journals_issn_cat_df.columns = ['journal','issn','keyword_id']

    # Builds the list "res" of tuples [(publi_id,scopus category),...]
    # ex: [(0, 'Applied Mathematics'), (0, 'Materials Chemistry'),...]
    # ----------------------------------------------------------------
    corpus_series_zip = zip(corpus_df[pub_id_col], corpus_df[scopus_journal_col],
                            corpus_df[scopus_issn_col])
    res = [] 
    for pub_id, journal, issn in corpus_series_zip:
        # Searching journal by name or by ISSN
        keywords = scopus_journals_issn_cat_df.query('journal==@journal')['keyword_id']
        if len(keywords):
            try:
                keywords = keywords.tolist()
                # appending keyword without care of duplicates
                for keyword in keywords:
                    # Selecting codes multiple of 100
                    res.extend([(pub_id,code_cat[int(i.strip()[0:2] + "00")].replace("General",""))
                                for i in keyword[:-1]])
            except:
                res.extend([(pub_id,'')])
        else:
            keywords = scopus_journals_issn_cat_df.query('issn==@issn')['keyword_id']

            if len(keywords):
                try:
                    keywords = keywords.tolist()
                    # appending keyword without care of duplicates
                    for keyword in keywords:
                        # Selecting codes multiple of 100
                        res.extend([(pub_id,code_cat[int(i.strip()[0:2] + "00")].\
                                     replace("General",""))
                                     for i in keyword[:-1]])
                except:
                    res.extend([(pub_id,'')])

    # Builds the data of subjects per publication
    # "subjects_df" has two columns "pub_id" and "scopus_keywords". 
    # The duplicated rows are supressed.
    # ----------------------------------------------------------------            
    pub_ids_list, keywords_list = zip(*res)
    subjects_df = pd.DataFrame.from_dict({pub_id_col :pub_ids_list,
                                          subject_col:keywords_list})
    out_pub_ids_list = subjects_df[subjects_df[subject_col]==''][pub_id_col].values
    fails_dic[subject_col] = {'success (%)':100*(1-len(out_pub_ids_list)/len(corpus_df)),
                                 pub_id_col:[int(x) for x in list(out_pub_ids_list)]}
    subjects_df.drop_duplicates(inplace=True)
    subjects_df = subjects_df[subjects_df[subject_col]!='']
    return subjects_df


def _build_sub_subjects_scopus(corpus_df, scopus_cat_codes_path,
                               scopus_journals_issn_cat_path, fails_dic, cols_tup):
    """Builds the data of sub-subject per publication of the corpus 
    and updates the parsing success rate data.
    
    The structure of the built data is composed of 2 columns and one row 
    per publication.
        Ex:
            Pub_id   Sub_subject
              0      Mathematics
              0      Engineering
              1	     Physics and Astronomy
              1	     Biochemistry, Genetics and Molecular Biology

    The subjects are attributed using 2 files provided by Elsevier.
    The "scopus_cat_codes.txt" file gives a code per category:
    
                         Category            Code    
            General Medicine                 2700    => Subject
            Medicine (miscellaneous)         2701    => Sub-subject
            Anatomy                          2702
            Anesthesiology and Pain Medicine 2703
            Biochemistry, medical            2704
            ...
    
    The "scopus_journals_issn_cat.txt" file give the categories codes 
    attached to a journal:
    
                Journal            ISSN           Codes
            21st Century Music   15343219     1210; 
            2D Materials                      2210; 2211; 3104; 2500; 1600; 
            3 Biotech            2190572X     1101; 2301; 1305;
            ...

    For "2D Materials journal":
        - The subjects are given by the codes multiple of 100: 2500; 1600
        - The sub-subjects are given by the other codes: 2210; 2211; 3104

    Args:
        corpus_df (dataframe): The selected rawdata of the corpus.
        scopus_cat_codes_path (path): The full path to the file \
        "scopus_cat_codes.txt".
        scopus_journals_issn_cat_path=None): The full path to the file \
        "scopus_journals_issn_cat.txt".
        fails_dic (dict): Parsing success rate data.
        cols_tup (tup): Columns information as built through \
        the `_set_scopus_parsing_cols` internal function.
    Returns:
        (dataframe): The built data.
    """
    # Setting useful column names
    _, cols_dic, scopus_cols_dic = cols_tup
    cols_keys = ['pub_id_col', 'sub_subject_col']
    (pub_id_col, sub_subject_col) = [cols_dic[key] for key in cols_keys]
    scopus_cols_keys = ['scopus_journal_col', 'scopus_issn_col']
    (scopus_journal_col, scopus_issn_col) = [scopus_cols_dic[key] for key in scopus_cols_keys]

    # Builds the dict "code_cat" {ASJC classification codes:description} out of the file "scopus_cat_codes.txt"
    # ex: {1000: 'Multidisciplinary', 1100: 'General Agricultural and Biological Sciences',...}
    # -------------------------------------------------------------------------------------------------
    scopus_cat_codes_df = pd.read_csv(scopus_cat_codes_path, sep='\t', header=None)
    code_cat = dict(zip(scopus_cat_codes_df[1].fillna(0.0).astype(int), scopus_cat_codes_df[0]))

    # Builds the dataframe "scopus_journals_issn_cat_df" out of the file "scopus_journals_issn_cat.txt"
    # "scopus_journals_issn_cat_df" has three columns:
    #       "journal": scopus journal name
    #       "issn": journal issn
    #       "keyword_id": list of keywords id asoociated to the journal or the issn
    # -----------------------------------------------------------------------------
    scopus_journals_issn_cat_df = pd.read_csv(scopus_journals_issn_cat_path, sep='\t',
                                              header=None).fillna(0) 
    scopus_journals_issn_cat_df[2] = scopus_journals_issn_cat_df[2].str.split(';')
    scopus_journals_issn_cat_df.columns = ['journal','issn','keyword_id']


    # Builds the list "res" of tuples [(publi_id,scopus category),...]
    # ex: [(0, 'Applied Mathematics'), (0, 'Materials Chemistry'),...]
    # ----------------------------------------------------------------
    corpus_series_zip = zip(corpus_df[pub_id_col], corpus_df[scopus_journal_col],
                            corpus_df[scopus_issn_col])
    res = [] 
    for pub_id, journal, issn in corpus_series_zip:
        # Searching journal by name or by ISSN
        keywords = scopus_journals_issn_cat_df.query('journal==@journal')['keyword_id']
        if len(keywords):
            try:
                keywords = keywords.tolist()
                # appending keyword without care of duplicates
                for keyword in keywords:
                    res.extend([(pub_id,code_cat[int(i)]) for i in keyword[:-1]])
            except:
                res.extend([(pub_id,'')])
        else:
            keywords = scopus_journals_issn_cat_df.query('issn==@issn')['keyword_id']
            if len(keywords):
                try:
                    keywords = keywords.tolist()
                    # appending keyword without care of duplicates
                    for keyword in keywords:
                        res.extend([(pub_id,code_cat[int(i)]) for i in keyword[:-1]])
                except:
                    res.extend([(pub_id,'')])

    # Builds the dataframe "df_keyword" out of tuples [(publ_id, scopus category),...]
    # "df_keyword" has two columns "pub_id" and "scopus_keywords". 
    # The duplicated rows are supressed.
    # ----------------------------------------------------------------            
    pub_ids_list, keywords_list = zip(*res)
    sub_subjects_df = pd.DataFrame.from_dict({pub_id_col     :pub_ids_list,
                                             sub_subject_col:keywords_list})
    out_pub_ids_list = sub_subjects_df[sub_subjects_df[sub_subject_col]==''][pub_id_col].values
    fails_dic[sub_subject_col] = {'success (%)':100*(1-len(out_pub_ids_list)/len(corpus_df)),
                                  pub_id_col:[int(x) for x in list(out_pub_ids_list)]}
    sub_subjects_df.drop_duplicates(inplace=True)
    sub_subjects_df = sub_subjects_df[sub_subjects_df[sub_subject_col]!='']
    return sub_subjects_df


def _build_articles_scopus(corpus_df, cols_tup):
    """Builds selected data of publications.

    The structure of the built data is composed of 12 columns and one row 
    per publication.
        Ex:

            Pub_id	Authors	 Year  Journal	 Volume	  Page	        DOI	          \
            0	    Hut M	 2025   Small	   21	 unknown	10.1002/smll.20...\ 
            1       Turck C	 2025	Commu...	4	 unknown	10.1038/s44172-...\	

            Document_type	Language      Title         ISSN      Norm_journal
               Article	    English	   Automated...   1613-6810      small
               Article	    English	   The logari...  2731-3395      eng
    
    Args:
        corpus_df (dataframe): The selected rawdata of the corpus.
        cols_tup (tup): Columns information as built through \
        the `_set_scopus_parsing_cols` internal function.
    Returns:
        (dataframe): The built data.
    """
    # Internal functions
    def _convert_issn(text):        
        y = ''.join(re.findall(re_issn, text))
        if len(y)!=0:
            new_text = y[0:4] + "-" + y[4:]
        else:
            new_text = bp_sg.UNKNOWN
        return new_text
   
    def _str_int_convertor(x):
        try:
            return(int(float(x)))
        except:
            return 0
        
    def _treat_author(authors_list):
        authors_sep = ',' 
        if ';' in authors_list:
            # Change in scopus on 07/2023
            authors_sep = ';'
        # Picking the first author
        raw_first_author = authors_list.split(authors_sep)[0] 
        first_author = normalize_name(raw_first_author) 
        return first_author
    
    def _treat_doctype(doctype):
        for doctype_key, doctype_list in bp_sg.DIC_DOCTYPE.items():
            if doctype in doctype_list:
                doctype = doctype_key
        return doctype 
    
    def _treat_title(title):
        title = title.translate(bp_gg.DASHES_CHANGE)
        title = title.translate(bp_gg.LANG_CHAR_CHANGE)
        title = title.translate(bp_gg.PONCT_CHANGE)
        return title

    # Setting regex for normalization of ISSN to the form dddd-dddd or dddd-dddX
    re_issn = re.compile(r'^[0-9]{8}|[0-9]{4}|[0-9]{3}X')

    # Setting useful column names
    cols_lists_dic, cols_dic, scopus_cols_dic = cols_tup
    articles_cols_list = cols_lists_dic['articles_cols_list']
    cols_keys = ['pub_id_col', 'author_col', 'year_col', 'doc_type_col',
                 'title_col', 'issn_col', 'norm_journal_col']
    (pub_id_col, author_col, year_col, doc_type_col, title_col,
     issn_col, norm_journal_col) = [cols_dic[key] for key in cols_keys]

    scopus_cols_keys = ['scopus_auth_col', 'scopus_year_col', 'scopus_journal_col',
                        'scopus_volume_col', 'scopus_page_col', 'scopus_doi_col',
                        'scopus_doctype_col', 'scopus_language_col', 'scopus_title_kw_col',
                        'scopus_issn_col']
    scopus_cols_list = [scopus_cols_dic[key] for key in scopus_cols_keys]
    

    articles_scopus_cols = scopus_cols_list + [norm_journal_col]
    articles_df = corpus_df[articles_scopus_cols].astype(str)
    articles_df.rename(columns=dict(zip(articles_scopus_cols, articles_cols_list[1:])),
                       inplace=True)                      
   
    articles_df[author_col] = articles_df[author_col].apply(_treat_author)
    articles_df[year_col] = articles_df[year_col].apply(_str_int_convertor)
    articles_df[doc_type_col] = articles_df[doc_type_col].apply(_treat_doctype)
    articles_df[title_col] = articles_df[title_col].apply(_treat_title)
    articles_df[issn_col] = articles_df[issn_col].apply(_convert_issn)
    
    articles_df.insert(0, pub_id_col, list(corpus_df[pub_id_col]))
    return articles_df


def _build_references_scopus(corpus_df, cols_tup):
    """Builds the data of cited references per publication of the corpus.

    The structure of the built data is composed of 6 columns and one row 
    per reference and per publication.
        Ex:

           Pub_id  Author     Year         Journal           Volume  Page
            0    Bellouard Q  2017   Int. J. Hydrog. Energy    42    13486
            0    Nishinaka H  2020   Energy Fuels              31    10933
            0    Bellouard Q  2018   Int. J. Hydrog. Energy    44    19193

    Args:
        corpus_df (dataframe): The selected rawdata of the corpus.
        cols_tup (tup): Columns information as built through \
        the `_set_scopus_parsing_cols` internal function.
    Returns:
        (dataframe): The built data.
    """
    #To Do: Check the regex
    # Setting useful column names
    cols_lists_dic, cols_dic, scopus_cols_dic = cols_tup
    ref_cols_list = cols_lists_dic['ref_cols_list']
    pub_id_col = cols_dic['pub_id_col']
    scopus_ref_col = scopus_cols_dic['scopus_ref_col']
    
    # Setting named tuple
    article_ref = namedtuple('article_ref', ref_cols_list)
    
    refs_list =[]
    refs_dic = {}               
    for pub_id, row in zip(list(corpus_df[pub_id_col]),
                                corpus_df[scopus_ref_col]):
        if isinstance(row, str):
            # If the reference field is not empty and not an URL
            for field in row.split(";"):
                if bp_rg.RE_DETECT_SCOPUS_NEW.search(field): 
                    # Using new SCOPUS coding 2023
                    year = re.findall(bp_rg.RE_REF_YEAR_SCOPUS, field)
                    if len(year):
                        year = year[0]
                    else:
                        year = 0

                    author = re.findall(bp_rg.RE_REF_AUTHOR_SCOPUS_NEW, field)
                    if len(author):
                        author = normalize_name(author[0])
                    else:
                        author = bp_sg.UNKNOWN            

                    page = re.findall(bp_rg.RE_REF_PAGE_SCOPUS_NEW, field)
                    if len(page) == 0:
                        page = 0
                    else:
                        page = page[0].split('p.')[1]

                    journal_vol =  re.findall(bp_rg.RE_REF_JOURNAL_SCOPUS_NEW , field)
                    if journal_vol:
                        journal_split = journal_vol[0].split(',')
                        journal = journal_split[0]
                        vol  = journal_split[1]
                    else:
                        journal = bp_sg.UNKNOWN
                        vol = 0

                else:
                    # Using old parsing
                    year = re.findall(bp_rg.RE_REF_YEAR_SCOPUS, field)
                    if len(year):
                        year = year[0]
                    else:
                        year = 0
                        
                    author = re.findall(bp_rg.RE_REF_AUTHOR_SCOPUS, field)
                    if len(author):
                        author = normalize_name(author[0])
                    else:
                        author = bp_sg.UNKNOWN

                    proceeding = re.findall(bp_rg.RE_REF_PROC_SCOPUS, field)
                    if not proceeding:
                        journal = re.findall(bp_rg.RE_REF_JOURNAL_SCOPUS, field)
                        if journal:
                            if ',' in journal[0] :
                                journal = journal[0][6:-1]
                            else:
                                journal = journal[0][6:]
                        else:
                            journal = bp_sg.UNKNOWN
                    else:
                        journal = proceeding

                    vol = re.findall(bp_rg.RE_REF_VOL_SCOPUS, field)
                    if len(vol):
                        if ',' in vol[0]:
                            vol = re.findall(r'\d{1,6}',vol[0])[0]
                        else:
                            vol = vol[0].strip()
                    else:
                        vol = 0

                    page = re.findall(bp_rg.RE_REF_PAGE_SCOPUS, field)
                    if len(page) == 0:
                        page = 0
                    else:
                        page = page[0].split('p.')[1]

            if author==bp_sg.UNKNOWN or journal==bp_sg.UNKNOWN:
                author = bp_sg.PARTIAL 
            refs_list.append(article_ref(pub_id, author, year, journal, vol, page))
    
    references_df = pd.DataFrame.from_dict({label:[s[idx] for s in refs_list] 
                                            for idx, label in enumerate(ref_cols_list)})    
    return references_df


def _check_authors_with_affiliations(corpus_df, check_cols, verbose=False):
    """Corrects the list of affiliations and the list of authors-with-affiliations 
    when irregular sequence of separators induces a discrepancy between number 
    of authors and number of authors-with-affiliations.

    Args:
        corpus_df (dataframe): The full rawdata of the corpus.
        check_cols (list): The column names where the authors \
        names or affiliations are present.
        verbose (bool): Optional for printing if True the list \
        of publications IDs corrected (default=False).
    Returns:
        (tup): (The corrected full rawdata of the corpus (dataframe), \
        The data (dataframe) of corrected affiliations).
    """
    pub_id_col, authors_col, affil_col, auth_affil_col = check_cols
    corrected_addresses_data = []
    new_corpus_df = corpus_df.copy()
    for row_idx, row in corpus_df.iterrows():
        pub_id = row[pub_id_col]
        init_authors_str = row[authors_col]
        init_affil_str = row[affil_col]
        init_auth_affil_str = row[auth_affil_col]

        std_sep = "; "
        authors_list = init_authors_str.split(std_sep)        
        affil_list = init_affil_str.split(std_sep)
        auth_affil_list = init_auth_affil_str.split(std_sep)

        check_sep = ";"
        check_auth_affil_list = init_auth_affil_str.split(check_sep)
        
        authors_nb = len(authors_list)
        auth_affil_nb = len(check_auth_affil_list)
        if authors_nb!=auth_affil_nb:
            authors_status, affil_status = 0, 0
            auth_false_sep = ";"
            auth_correct_sep = ""
            if any(auth_false_sep in s for s in authors_list):
                new_authors_list = [x.replace(auth_false_sep, auth_correct_sep) for x in authors_list]
                new_auth_affil_list = [x.replace(auth_false_sep, auth_correct_sep) for x in auth_affil_list]
                authors_status = 1
            else:
                new_authors_list = authors_list
                new_auth_affil_list = auth_affil_list

            addr_false_sep = ";, "
            addr_correct_sep = ", "
            if any(addr_false_sep in s for s in affil_list):
                new_affil_list = [x.replace(addr_false_sep, addr_correct_sep) for x in affil_list]
                new_auth_affil_list = [x.replace(addr_false_sep, addr_correct_sep) for x in new_auth_affil_list]
                affil_status = 1
            else:
                new_affil_list = affil_list
            
            new_authors_str = std_sep.join(new_authors_list)
            new_affil_str = std_sep.join(new_affil_list)
            new_auth_affil_str = std_sep.join(new_auth_affil_list)   
            corrected_addresses_data.append([pub_id, authors_status, affil_status,
                                             init_authors_str, new_authors_str,
                                             init_affil_str, new_affil_str,
                                             init_auth_affil_str, new_auth_affil_str])
        else:
            new_authors_str = init_authors_str
            new_affil_str = init_affil_str
            new_auth_affil_str = init_auth_affil_str

        # Updating corpus data 
        new_corpus_df.loc[row_idx, authors_col] = new_authors_str    
        new_corpus_df.loc[row_idx, affil_col] = new_affil_str
        new_corpus_df.loc[row_idx, auth_affil_col] = new_auth_affil_str
        
    correction_cols = [pub_id_col, "Authors status", "Address status",
                       authors_col, "Corrected " + authors_col,
                       affil_col, "Corrected " + affil_col,
                       auth_affil_col, "Corrected " + auth_affil_col]
    corrected_addresses_df = pd.DataFrame(corrected_addresses_data, columns=correction_cols)                                   
    return new_corpus_df, corrected_addresses_df


def _correct_firstname_initials(author, fullname):
    # Remove author digital identifier
    if "(" in fullname:
        fullname = fullname.split(" (")[0]
    lastname, firstname = fullname.split(", ")
    
    # Normalizing author's name and last-name with ponctuation drop (specically ";")
    author = normalize_name(author, drop_ponct=True)
    lastname = normalize_name(lastname, drop_ponct=True, lastname_only=True)

    # Normalizing author's first name keeping ponctuation (specifically ".")
    firstname = normalize_name(firstname, drop_ponct=False, firstname_only=True)

    # Building firstname initials
    firstname = firstname.replace('-',' ').strip(' ')
    firstname_sapce_list = firstname.split(' ')
    firstname_list = sum([x.split('.') for x in firstname.split(' ')], [])
    initials_list = [x[0] + "." for x in firstname_list if x]
    initials = ''.join(initials_list)

    # Building new author name
    new_author = ' '.join([lastname, initials])
    return new_author


def _correct_auth_data(author, auth_tup):
    fullname, auth_affil = auth_tup

    # Correcting author name
    new_author = _correct_firstname_initials(author, fullname)

    # Updating author-with-affiliations with the corrected author name
    auth_affil_split = auth_affil.split(", ")
    affil = ", ".join(auth_affil_split[1:])
    new_auth_affil = ", ".join([new_author, affil])
    return new_author, new_auth_affil


def _check_authors(corpus_df, check_cols, verbose=False):
    """Corrects the firstname initials for the authors using 
    the fullnames given in the full corpus data.

    Args:
        corpus_df (dataframe): The full rawdata of the corpus.
        check_cols (list): The column names where the authors \
        names are present.
        verbose (bool): Optional for printing if True the list \
        of publications IDs corrected (default=False).
    Returns:
        (tup): (The corrected full rawdata of the corpus (dataframe), \
        The data (dataframe) of corrected authors).
    """
    pub_id_col, authors_col, fullname_col, auth_affil_col = check_cols
    corrected_authors_data = []
    new_corpus_df = corpus_df.copy()
    for row_idx, row in corpus_df.iterrows():
        pub_id = row[pub_id_col]

        # Removing accentuated characters
        authors_str = remove_special_symbol(row[authors_col])
        fullnames_str = remove_special_symbol(row[fullname_col])
        auth_affil_str = remove_special_symbol(row[auth_affil_col])

        # Building dict keyyed by author and valued by a tuple 
        # composed of fullname and author-with-affiliations
        authors_list = authors_str.split("; ")
        fullnames_list = fullnames_str.split("; ")        
        auth_affil_list = auth_affil_str.split("; ")
        author_tup_list = list(zip(fullnames_list, auth_affil_list))
        auth_data_dict = dict(zip(authors_list, author_tup_list))

        # Correcting list of authors and list of authors-with-affiliations
        new_authors_list = []
        new_auth_affils_list = []
        for author, auth_tup in auth_data_dict.items():
            new_author, new_auth_affil = _correct_auth_data(author, auth_tup)
            if author!=new_author:
                corrected_authors_data.append([pub_id, author, new_author])
            new_authors_list.append(new_author)
            new_auth_affils_list.append(new_auth_affil)

        # Updating the corpus data with the corrected lists
        new_corpus_df.loc[row_idx, authors_col] = "; ".join(new_authors_list)
        new_corpus_df.loc[row_idx, auth_affil_col] = "; ".join(new_auth_affils_list)
    correction_cols = [pub_id_col, authors_col, "Corrected " + authors_col]
    corrected_authors_df = pd.DataFrame(corrected_authors_data, columns=correction_cols)
    return new_corpus_df, corrected_authors_df
            
        
def _correct_full_rawdata(corpus_df, cols_tup):
    """Corrects firstname initials and affiliations of authors 
    in the full rawdata of the corpus.

    Args:
        corpus_df (dataframe): The full rawdata of the corpus.
        cols_tup (tup): Columns information as built through \
        the `_set_scopus_parsing_cols` internal function.
    Returns:
        (dataframe): The corrected full rawdata of the corpus.
    """
    # Setting useful column names
    _, cols_dic, scopus_cols_dic = cols_tup
    cols_keys = ['pub_id_col', ]
    pub_id_col = cols_dic['pub_id_col']
    scopus_cols_keys = ['scopus_auth_col','scopus_aff_col', 'scopus_auth_with_aff_col',
                        'scopus_fullnames_col']
    (scopus_auth_col, scopus_aff_col, scopus_auth_with_aff_col,
     scopus_fullnames_col) = [scopus_cols_dic[key] for key in scopus_cols_keys]

    affil_check_cols = [pub_id_col, scopus_auth_col,
                        scopus_aff_col, scopus_auth_with_aff_col]
    auth_check_cols = [pub_id_col, scopus_auth_col,
                       scopus_fullnames_col, scopus_auth_with_aff_col]

    # Setting the pub_id in df index
    corpus_df.index = range(len(corpus_df))

    # Setting the pub-id as a column
    corpus_df = corpus_df.rename_axis(pub_id_col).reset_index()

    # Correcting corpus data
    new_corpus_df, corrected_addresses_df = _check_authors_with_affiliations(corpus_df, affil_check_cols)
    new_corpus_df, corrected_authors_df = _check_authors(new_corpus_df, auth_check_cols)

    # Droping pub_id_col column
    new_corpus_df.drop(columns=[pub_id_col], inplace=True)
    return new_corpus_df, corrected_authors_df, corrected_addresses_df


def _check_affiliation_column_scopus(df, scopus_aff_col):
    
    """The `_check_affiliation_column_scopus` function checks the correcteness of the column affiliation of a df 
    read from a csv scopus file.
    A cell of the column affiliation should read:
    address<0>, country<0>;...; address<i>, country<i>;...
    
    Some cells can be misformatted with an uncorrect country field. The function eliminates, for each
    cell of the column, those items address<i>, country<i> uncorrectly formatted. When such an item is detected
    a warning message is printed.    
    """
    #To Do: Doc string update
    def _valid_affiliation(row):
        nonlocal idx
        idx += 1
        valid_affiliation_list = []
        for affiliation in row[scopus_aff_col].split('; '):
            raw_country = affiliation.split(', ')[-1].strip()
            if normalize_country(raw_country):
                valid_affiliation_list.append(affiliation)
            else:
                warning = (f'\nWARNING in "_check_affiliation_column_scopus" function of "BiblioParsingScopus.py" module:'
                           f'\nAt row {idx} of the scopus corpus, the invalid affiliation "{affiliation}" '
                           f'has been droped from the list of affiliations. '
                           f'\nTherefore, attention should be given to the resulting list of affiliations '
                           f'for each of the authors of this publication.\n' )           
                print(warning)
        if  valid_affiliation_list:  
            return '; '.join(valid_affiliation_list)
        else:
            return 'unknown'
    
    idx = -1
    df[scopus_aff_col] = df.apply(_valid_affiliation, axis=1) 
    
    return df


def read_database_scopus(rawdata_path, correct_data=False, scopus_ids=False):
    """Reads the file of Scopus rawdata available in the indicated folder.

    First, it can corrects the firsname initials and the affiliations 
    of the authors when required using the `_correct_full_rawdata` 
    internal function. 
    Then, the function:
    - Checks columns and drops unuseful columns using the \
    `check_and_drop_columns` function imported from `BiblioParsingUtils` module.
    - Checks the affilation column content using the `_check_affiliation_column_scopus` \
    internal function. 
    - Replaces the unavailable items values by a string set in the global UNKNOWN.
    - Normalizes the journal names using the `normalize_journal_names` function \
    imported from the `BiblioParsingUtils` module.
    Finally, the function can built data of Scopus identifiers of the publications.
    The returned data are initialized to empty dataframes.

    Args:
        rawdata_path (path): The full path to the Scopus-rawdata file.
        correct_data (bool): Optional, true for correcting authors' names \
        and addresses (dafault=False).
        scopus_ids (bool): Optional, true for building the data of Scopus IDs of \
        publications (dafault=False).
    Returns:
        (tup): (The cleaned corpus data (dataframe), The optional data of corrected \
        authors' names (dataframe), The optional data of corrected \
        addresses (dataframe), The optional Scopus-IDs data (dataframe)). 
    """
    # Setting columns for scopus parsing process
    cols_tup = _set_scopus_parsing_cols()
    _, cols_dic, scopus_cols_dic = cols_tup
    scopus_id_col = cols_dic['scopus_id_col']
    scopus_cols_keys = ['init_scopus_id_col', 'scopus_aff_col']
    (init_scopus_id_col, scopus_aff_col) = [scopus_cols_dic[key] for key in scopus_cols_keys]

    # Initializing returned data to empty dataframes
    scopus_rawdata_df = pd.DataFrame()
    corrected_authors_df = pd.DataFrame()
    corrected_addresses_df = pd.DataFrame()
    scopus_ids_df = pd.DataFrame()

    # Check if rawdata file is available and get its full path if it is 
    rawdata_file_path = check_and_get_rawdata_file_path(rawdata_path, bp_sg.SCOPUS_RAWDATA_EXTENT)

    if rawdata_file_path:    
        full_scopus_rawdata_df = pd.read_csv(rawdata_file_path, dtype=bp_sg.COLUMN_TYPE_SCOPUS)

        if len(full_scopus_rawdata_df):
            if correct_data:
                return_tup = _correct_full_rawdata(full_scopus_rawdata_df, cols_tup)
                full_scopus_rawdata_df, corrected_authors_df, corrected_addresses_df = return_tup
            
            # Selecting useful rawdata for parsing
            scopus_rawdata_df = check_and_drop_columns(bp_sg.SCOPUS, full_scopus_rawdata_df)
            scopus_rawdata_df = _check_affiliation_column_scopus(scopus_rawdata_df, scopus_aff_col)
            scopus_rawdata_df = scopus_rawdata_df.replace(np.nan, bp_sg.UNKNOWN, regex=True)
            scopus_rawdata_df = normalize_journal_names(bp_sg.SCOPUS, scopus_rawdata_df)

            if scopus_ids:
                # Building the Scopus-IDs data
                scopus_ids_df = build_pub_db_ids(full_scopus_rawdata_df, init_scopus_id_col, scopus_id_col)
    return_tup = (scopus_rawdata_df, corrected_authors_df, corrected_addresses_df, scopus_ids_df)               
    return return_tup 


def biblio_parser_scopus(rawdata_path, inst_filter_list=None, country_affiliations_file_path=None,
                         inst_types_file_path=None, country_towns_file=None,
                         country_towns_folder_path=None):
    """Builds parsing data from the corpus rawdata.

    The list of the parsed items (keys of the returned dict which values are the dataframes \
    of the parsing results) is given by the PARSING_ITEMS_LIST global. 
    The rawdata are parsed using the following internal functions:
    - `_build_authors_scopus` which parses the authors' rawdata;
    - `_build_keywords_scopus` which parses the authors' keywords and the indexed keywords rawdata \
    and builds the title keywords from the publication title;
    - `_build_addresses_countries_institutions_scopus` which parses the affilations rawdata \
    by publication;
    - `_build_authors_countries_institutions_scopus` which parses the author-with-affilations \
    rawdata and affilations rawdata by authors;
    - `_build_subjects_scopus` which attributes the subjects to each publication using Scopus \
    dedicated files;
    - `_build_sub_subjects_scopus` which attributes the secondary subjects to each publication \
    using Scopus dedicated files;
    - `_build_articles_scopus` which parses selected attributes of the publications given \
    by the corpus rawdata.
    - `_build_references_scopus` which parses the references rawdata by publication.

    Args:
        rawdata_path (path): The full path to the corpus rawdata.
        inst_filter_list (list): The affiliations-filter composed of a list of normalized affiliations (str), \
        optional (default=None).
        country_affiliations_file_path (path): The full path to the data per country of raw affiliations \
        per normalized one, optional (default=None).
        inst_types_file_path (path): The full path to the data of institutions-types used to normalize \
        the affiliations, optional (default=None).
        country_towns_file (str): The name of the file of the data of towns per country, optional (default=None).
        country_towns_folder_path (path): The full path to the folder where the 'country_towns_file' file \
        is available, optional (default=None).
    Returns:
        (tup): (The parsed data (dataframes) as values of a dict keyed by parsing items, \
        The parsing success rate data (dict), The data (dataframe) of the corrected author names, \
        The data (dataframe) of the corrected addresses, The data (dataframe) of Scopus IDs of publications.
    """
    # Internal functions    
    def _keeping_item_parsing_results(item, item_df):
        scopus_parsing_dict[item] = item_df

    # Setting columns for scopus parsing process
    cols_tup = _set_scopus_parsing_cols()
    cols_lists_dic, cols_dic, scopus_cols_dic = cols_tup

    # Setting items list and values
    items_list = [bp_sg.PARSING_ITEMS_LIST[x] for x in range(12)]
    (articles_item, authors_item, addresses_item, countries_item, institutions_item,
     auth_inst_item, authors_kw_item, index_kw_item, title_kw_item, subjects_item,
     sub_subjects_item, references_item) = items_list
    
    # Setting the specific file paths for subjects ans sub-subjects assignement for Scopus corpuses    
    path_scopus_cat_codes = Path(__file__).parent / Path(bp_gg.REP_UTILS) / Path(bp_sg.SCOPUS_CAT_CODES)
    path_scopus_journals_issn_cat = Path(__file__).parent / Path(bp_gg.REP_UTILS) / Path(bp_sg.SCOPUS_JOURNALS_ISSN_CAT)   

    # Reading and checking the corpus file
    raw_data_return_tup = read_database_scopus(rawdata_path, correct_data=True, scopus_ids=True)
    corpus_df, corrected_authors_df, corrected_addresses_df, scopus_ids_df = raw_data_return_tup
    
    # Initializing the scopus_fails_dic dict for the parsing control
    scopus_fails_dic = {}
    
    # Initializing the dict of dataframes resulting from the parsing
    scopus_parsing_dict = {}
    
    if corpus_df is not None:                      
        # Keeping the number of articles in scopus_fails_dic dict
        scopus_fails_dic['number of article'] = len(corpus_df)
        if len(corpus_df):
            # Building the dataframe of articles
            print(f"  - {articles_item} parsing...", end="\r")
            articles_df = _build_articles_scopus(corpus_df, cols_tup)
            _keeping_item_parsing_results(articles_item, articles_df)
            print(f"  - {articles_item} parsed    ")
            
            # Building the dataframe of authors
            print(f"  - {authors_item} parsing...", end="\r")
            authors_df = _build_authors_scopus(corpus_df, scopus_fails_dic, cols_tup)
            _keeping_item_parsing_results(authors_item, authors_df)
            print(f"  - {authors_item} parsed    ")
            
            # Building the dataframe of addresses, countries and institutions
            print(f"  - {addresses_item}, {countries_item} and {institutions_item} parsing...", end="\r")
            addresses_tup = _build_addresses_countries_institutions_scopus(corpus_df, scopus_fails_dic, cols_tup)
            addresses_df, countries_df, institutions_df = addresses_tup
            _keeping_item_parsing_results(addresses_item, addresses_df)
            _keeping_item_parsing_results(countries_item, countries_df)
            _keeping_item_parsing_results(institutions_item, institutions_df)
            print(f"  - {addresses_item}, {countries_item} and {institutions_item} parsed    ")
            
            # Building the dataframe of authors and their institutions
            print(f"  - {auth_inst_item} parsing...")
            auth_inst_df = _build_authors_countries_institutions_scopus(corpus_df, scopus_fails_dic, cols_tup,
                                                                        inst_filter_list=inst_filter_list ,
                                                                        country_affiliations_file_path=country_affiliations_file_path,
                                                                        inst_types_file_path=inst_types_file_path,
                                                                        country_towns_file=country_towns_file,
                                                                        country_towns_folder_path=country_towns_folder_path)
            _keeping_item_parsing_results(auth_inst_item, auth_inst_df)
            print(f"      {auth_inst_item} parsed    ")
            
            # Building the dataframes of keywords
            print(f"  - {authors_kw_item}, {index_kw_item} and {title_kw_item} parsing...", end="\r")
            keywords_tup = _build_keywords_scopus(corpus_df, scopus_fails_dic, cols_tup) 
            AK_keywords_df, IK_keywords_df, TK_keywords_df = keywords_tup
            _keeping_item_parsing_results(authors_kw_item, AK_keywords_df)
            _keeping_item_parsing_results(index_kw_item, IK_keywords_df)
            _keeping_item_parsing_results(title_kw_item, TK_keywords_df)
            print(f"  - {authors_kw_item}, {index_kw_item} and {title_kw_item} parsed    ")
            
            # Building the dataframe of subjects
            print(f"  - {subjects_item} parsing...", end="\r")
            subjects_df = _build_subjects_scopus(corpus_df,
                                                 path_scopus_cat_codes,
                                                 path_scopus_journals_issn_cat,
                                                 scopus_fails_dic, cols_tup)
            _keeping_item_parsing_results(subjects_item, subjects_df)
            print(f"  - {subjects_item} parsed    ")
           
            # Building the dataframe of sub-subjects
            print(f"  - {sub_subjects_item} parsing...", end="\r")
            sub_subjects_df = _build_sub_subjects_scopus(corpus_df,
                                                         path_scopus_cat_codes,
                                                         path_scopus_journals_issn_cat,
                                                         scopus_fails_dic, cols_tup)
            _keeping_item_parsing_results(sub_subjects_item, sub_subjects_df)
            print(f"  - {sub_subjects_item} parsed    ")
            
            # Building the dataframe of references
            print(f"  - {references_item} parsing...", end="\r")
            references_df = _build_references_scopus(corpus_df, cols_tup)
            _keeping_item_parsing_results(references_item, references_df)
            print(f"  - {references_item} parsed    ")
    
        else:
            empty_df = pd.DataFrame()
            for item in items_list:
                _keeping_item_parsing_results(item, empty_df)
    return_tup = (scopus_parsing_dict, scopus_fails_dic, scopus_ids_df,
                  corrected_authors_df, corrected_addresses_df)
    return return_tup 
        