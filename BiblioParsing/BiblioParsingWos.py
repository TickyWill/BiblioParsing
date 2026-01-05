__all__ = ['biblio_parser_wos',
           'read_database_wos']


# Standard library imports 
import csv
import json
import numpy as np
import re
import os
from collections import namedtuple
from pathlib import Path

# 3rd party library imports
import pandas as pd

# Local library imports
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


def _set_wos_parsing_cols():
    """Builds 3 dict setting columns list and selected columns names 
    for the process of parsing WoS rawdata.

    Returns:
        (tup): (A dict valued by column-names lists for each parsing item \
        and temporary column names defined by the 'COL_NAMES' global, \
        A dict valued by column names of parsing results defined by the \
        'COL_NAMES' global, A dict valued by column names of rawdata defined \
        by the 'COLUMN_LABEL_WOS' and 'COLUMN_LABEL_WOS_PLUS' globals).
    """
    cols_lists_dic = {'articles_cols_list'   : bp_sg.COL_NAMES['articles'],
                      'address_cols_List'    : bp_sg.COL_NAMES['address'],
                      'auth_cols_list'       : bp_sg.COL_NAMES['authors'],
                      'auth_inst_cols_list'  : bp_sg.COL_NAMES['auth_inst'],
                      'country_cols_list'    : bp_sg.COL_NAMES['country'],
                      'inst_cols_list'       : bp_sg.COL_NAMES['institution'],
                      'kw_cols_List'         : bp_sg.COL_NAMES['keywords'],
                      'ref_cols_list'        : bp_sg.COL_NAMES['references'],
                      'subject_cols_list'    : bp_sg.COL_NAMES['subject'],
                      'sub_subject_cols_list': bp_sg.COL_NAMES['sub_subject'],
                      'tmp_cols_list'        : bp_sg.COL_NAMES['temp_col'],
                     }

    cols_dic = {'wos_id_col'              : bp_sg.COL_NAMES['wos_id'][0],
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
    
    wos_cols_dic = {'wos_auth_col'         : bp_sg.COLUMN_LABEL_WOS['authors'],                   
                    'wos_title_kw_col'     : bp_sg.COLUMN_LABEL_WOS['title'],
                    'wos_year_col'         : bp_sg.COLUMN_LABEL_WOS['year'],
                    'wos_journal_col'      : bp_sg.COLUMN_LABEL_WOS['journal'],
                    'wos_volume_col'       : bp_sg.COLUMN_LABEL_WOS['volume'],
                    'wos_page_col'         : bp_sg.COLUMN_LABEL_WOS['page_start'],
                    'wos_doi_col'          : bp_sg.COLUMN_LABEL_WOS['doi'],
                    'wos_aff_col'          : bp_sg.COLUMN_LABEL_WOS['affiliations'],
                    'wos_auth_with_aff_col': bp_sg.COLUMN_LABEL_WOS['authors_with_affiliations'],
                    'wos_auth_kw_col'      : bp_sg.COLUMN_LABEL_WOS['author_keywords'],
                    'wos_idx_kw_col'       : bp_sg.COLUMN_LABEL_WOS['index_keywords'],
                    'wos_ref_col'          : bp_sg.COLUMN_LABEL_WOS['references'],
                    'wos_issn_col'         : bp_sg.COLUMN_LABEL_WOS['issn'],
                    'wos_language_col'     : bp_sg.COLUMN_LABEL_WOS['language'],
                    'wos_doctype_col'      : bp_sg.COLUMN_LABEL_WOS['document_type'],
                    'wos_fullnames_col'    : bp_sg.COLUMN_LABEL_WOS['authors_fullnames'],
                    'wos_subjects_col'     : bp_sg.COLUMN_LABEL_WOS['subjects'],
                    'wos_sub_subjects_col' : bp_sg.COLUMN_LABEL_WOS['sub_subjects'],
                    'init_wos_id_col'      : bp_sg.COLUMN_LABEL_WOS_PLUS['wos_id'],
                   }

    return cols_lists_dic, cols_dic, wos_cols_dic


def _check_authors_list(authors_str, affiliations_str):
    # Building the full list of ordered authors full names
    authors_ordered_list = authors_str.split("; ")
    authors_ordered_list = [author.strip() for author in authors_ordered_list]

    # Building the list of authors full names in authors-with-affiliation
    affil_authors_list = [[x.strip() for x in authors.split(';')]
                    for authors in bp_rg.RE_AUTHOR.findall(affiliations_str)]
    flat_authors_list  = list(set(sum(affil_authors_list, [])))

    # Building the list of authors out of authors-with-affiliation
    out_authors_list = list(set(authors_ordered_list) - set(flat_authors_list))
    return authors_ordered_list, affil_authors_list, out_authors_list


def _build_authors_wos(corpus_df, fails_dic, cols_tup):
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
        the `_set_wos_parsing_cols` internal function.
    Returns:
        (dataframe): The built data.
    """
    # Setting useful column names
    cols_lists_dic, cols_dic, wos_cols_dic = cols_tup
    auth_cols_list = cols_lists_dic['auth_cols_list']
    cols_keys = ['pub_id_col', 'co_authors_col', ]
    (pub_id_col, co_authors_col) = [cols_dic[key] for key in cols_keys]
    wos_auth_col = wos_cols_dic['wos_auth_col']

    # Setting named tuple
    co_author = namedtuple('co_author', auth_cols_list)

    authors_list = []
    for pub_id, wos_auth_str in zip(corpus_df[pub_id_col], corpus_df[wos_auth_col]):
        author_idx = 0
        for wos_auth in wos_auth_str.split(';'):
            author = normalize_name(wos_auth.replace('.','').replace(',',''))
            if author not in ['Dr','Pr','Dr ','Pr ']:
                authors_list.append(co_author(pub_id, author_idx, author))
                author_idx += 1

    # Building a clean co-authors dataframe and accordingly updating the parsing success rate dict
    co_authors_df, fails_dic = build_item_df_from_tup(authors_list, auth_cols_list,
                                                      co_authors_col, pub_id_col, fails_dic)
    return co_authors_df


def _build_keywords_wos(corpus_df, fails_dic, cols_tup):
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
        the `_set_wos_parsing_cols` internal function.
    Returns:
        (dataframe): The built data.
    """
    # Setting useful column names
    cols_lists_dic, cols_dic, wos_cols_dic = cols_tup
    kw_cols_List = cols_lists_dic['kw_cols_List']
    cols_keys = ['pub_id_col', 'keyword_col', 'title_temp_col', 'kept_tokens_col']
    (pub_id_col, keyword_col, title_temp_col, kept_tokens_col) = [cols_dic[key] for key in cols_keys]
    wos_cols_keys = ['wos_auth_kw_col', 'wos_idx_kw_col', 'wos_title_kw_col']
    (wos_auth_kw_col, wos_idx_kw_col,
     wos_title_kw_col )= [wos_cols_dic[key] for key in wos_cols_keys]
    
    # Setting named tuple
    key_word = namedtuple('key_word', kw_cols_List)    
    
    aks_list = [] 
    aks_df = corpus_df[wos_auth_kw_col].fillna('')
    for pub_id, pub_aks_str in zip(corpus_df[pub_id_col], aks_df):
        pub_aks_list = pub_aks_str.split(';')      
        for pub_ak in pub_aks_list:
            pub_ak = pub_ak.lower().strip()
            aks_list.append(key_word(pub_id,
                                     pub_ak if pub_ak!='null' else bp_sg.UNKNOWN))
    iks_list = []
    iks_df = corpus_df[wos_idx_kw_col].fillna('')
    for pub_id, pub_iks_str in zip(corpus_df[pub_id_col], iks_df):
        pub_iks_list = pub_iks_str.split(';')
        for pub_ik in pub_iks_list:
            pub_ik = pub_ik.lower().strip()
            iks_list.append(key_word(pub_id, pub_ik if pub_ik!='null' else bp_sg.UNKNOWN))
            
    tks_list = []
    title_df = pd.DataFrame(corpus_df[wos_title_kw_col].fillna(''))
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


def _build_addresses_countries_institutions_wos(corpus_df, fails_dic, cols_tup):
    """Builds the data of addresses, countries and main affiliations 
    per publications of the corpus and updates the parsing success rate data.

    Beware, multiple formats may exist for the field parsed from the WoS rawdata. 
    We take care for two different formats in the present function. 
    The structure of the built data is composed of 3 columns and one row 
    per publication and per address identifier.
        Ex:
        From the following authors with affiliations information of WoS raw data 
        for the publication identified by Pub_id=0:

            '[Jung, Bo Kum; Elesina, Varvara V.; Kuerner, Thomas] Tech Univ Carolo Wilhelmina \
             Braunschweig, Inst Nachrichtentechn, Braunschweig, Germany; 
             [Matos, Sergio] Univ Inst Lisbon, Inst Telecomunicac, Lisbon, Portugal; 
             [Koutsos, Orestis; Clemente, Antonio; D'Errico, Raffaele] Univ Grenoble Alpes, CEA, \
             Leti, Grenoble, France'

        The built data will be as follows.
        - for the addresses data:

             Pub_id  Idx_address                     Address   
               0         0        Tech Univ Carolo Wilhelmina Braunschweig, ...
               0         1        Univ Inst Lisbon, Inst Telecomunicac, Lisbon, Portugal
               0         2        Univ Grenoble Alpes, CEA, Leti, Grenoble, France

        - for the countries data:

             Pub_id  Idx_address   Country
               0         0         Germany
               0         1         Portugal
               0         2         France

        - for the main affiliations data:

             Pub_id  Idx_address  Institution
               0         0         Tech University Carolo Wilhelmina Braunschweig
               0         1         University Institute Lisbon
               0         2         University Grenoble Alpes

    Args:
        corpus_df (dataframe): The selected rawdata of the corpus.
        fails_dic (dict): Parsing success rate data.
        cols_tup (tup): Columns information as built through \
        the `_set_wos_parsing_cols` internal function.
    Returns:
        (tup): (The built addresses data (dataframe), tha built countries data (dataframe), \
        The built main affiliations data (dataframe)).
    """
    # Setting useful column names
    cols_lists_dic, cols_dic, wos_cols_dic = cols_tup
    cols_lists_keys = ['address_cols_List', 'country_cols_list', 'inst_cols_list']
    address_cols_List, country_cols_list, inst_cols_list = [cols_lists_dic[key] for key in cols_lists_keys]
    cols_keys = ['pub_id_col', 'address_col', 'country_col', 'institution_col']
    (pub_id_col, address_col, country_col, institution_col) = [cols_dic[key] for key in cols_keys]
    wos_cols_keys = ['wos_auth_with_aff_col', 'wos_fullnames_col']
    (wos_auth_with_aff_col, wos_fullnames_col) = [wos_cols_dic[key] for key in wos_cols_keys]
    
    # Setting named tuples
    address_tup = namedtuple('address', address_cols_List)
    country_tup = namedtuple('country', country_cols_list)
    institution_tup = namedtuple('institution', inst_cols_list)
    
    corpus_series_zip = zip(corpus_df[pub_id_col],
                            corpus_df[wos_fullnames_col],
                            corpus_df[wos_auth_with_aff_col])
    
    addresses_list, countries_list, institutions_list = [], [], []
    for pub_id, authors_str, affiliations_str in corpus_series_zip:
        pub_addresses_list = []
        if '[' in affiliations_str:
            # Format case: '[Author1] address1; [Author1, Author2] address2...'
            # authors = bp_rg.RE_AUTHOR.findall(affiliations_str) # for future use
            pub_addresses_list = bp_rg.RE_ADDRESS.findall(affiliations_str)

            # Checking authors in authors list and authors-with-affiliation data
            authors_ordered_list, _, out_authors_list = _check_authors_list(authors_str, affiliations_str)
            for out_author in out_authors_list:
                out_author_idx = authors_ordered_list.index(out_author)
                out_author_address = set_unknown_address(out_author_idx)
                pub_addresses_list.append(out_author_address)  
        else:
            # Format case: 'address1;address2...'
            pub_addresses_list = affiliations_str.split(';')
        
        if pub_addresses_list:
            for address_idx, pub_raw_address in enumerate(pub_addresses_list):
                pub_address = standardize_address(pub_raw_address)
                addresses_list.append(address_tup(pub_id, address_idx, pub_address))
                
                main_institution = pub_address.split(',')[0]
                institutions_list.append(institution_tup(pub_id, address_idx, main_institution))

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
        warning = (f'WARNING: Lengths of "address_df", "country_df" and "institution_df" dataframes are not equal'
                   f'in "_build_addresses_countries_institutions_wos" function of "BiblioParsingWos.py" module')
        print(warning)
    
    return address_df, country_df, institution_df


def _build_authors_countries_institutions_wos(corpus_df, fails_dic, cols_tup, inst_filter_list=None,
                                              country_affiliations_file_path=None,
                                              inst_types_file_path=None,
                                              country_towns_file=None,
                                              country_towns_folder_path=None):
    """Parses the field of authors with affiliations of the corpus data to build the data of authors 
    with their addresses, country and normalized affiliations per publication of the corpus. 

    The parsing success rate data are updated. 
    In addition, the built data may be expanded according to a filtering of affiliations. 
    The parsing is effective only for the format of the following example. Otherwise, the parsing 
    fields are set to UNKNOWN global.
       
    For example, the 'Authors with affiliations' field string:
       '[Boujjat, Houssame] CEA, LITEN Solar & Thermodynam Syst Lab L2ST, F-38054 Grenoble, France; 
        [Boujjat, Houssame] Univ Grenoble Alpes, F-38000 Grenoble, France; 
        [Rodat, Sylvain; Chuayboon, Srirat] CNRS, Proc Mat & Solar Energy Lab, 
        PROMES, 7 Rue Four Solaire, F-66120 Font Romeu, France; 
        [Abanades, Stephane] CEA, Leti, 17 rue des Martyrs, F-38054 Grenoble, France; 
        [Dupont, Sylvain] CEA, Liten, INES. 50 avenue du Lac Leman, F-73370 Le Bourget-du-Lac, France; 
        [Durand, Maurice] CEA, INES, DTS, 50 avenue du Lac Leman, F-73370 Le Bourget-du-Lac, France; 
        [David, David] Lund University, Department of Physical Geography and Ecosystem Science (INES), Lund, Sweden'

    will be parsed in the "addr_country_inst_df" dataframe if affiliation filter is not defined (initialization step):
   
        Pub_id  Idx_author                     Address               Country    Norm_institutions              Raw_institutions     
            0       0        CEA, LITEN Solar & Thermodynam , ...    France     CEA Nro;LITEN Rto              F-38054 Grenoble
            0       0        Univ Grenoble Alpes,...                 France     UGA Univ                       F-38000 Grenoble
            0       1        CNRS, Proc Mat Lab, PROMES,...          France     CNRS Nro;PROMES CNRS-Lab       7 Rue Four Solaire;...                          
            0       2        CNRS, Proc Mat Lab, PROMES, ...         France     CNRS Nro;PROMES CNRS-Lab       7 Rue Four Solaire;...    
            0       3        CEA, Leti, 17 rue des Martyrs,...       France     CEA Nro;LETI Rto               17 rue des Martyrs;...         
            0       4        CEA, Liten, INES. 50 avenue...          France     CEA Nro;LITEN Rto;INES Site    50 avenue du Lac Leman;...           
            0       5        CEA, INES, DTS, 50 avenue...            France     CEA Nro;INES Site              DTS;...
            0       6        Lund University,...(INES),...           Sweden     Lund Univ                      Department of Physical ...
    
    the authors' identifiers are defined using the ordered list of the authors given by the corpus data.
    The institutions are identified and normalized using dedicated data that should be specified by the user.
        
    If affiliation filter is defined based on the following list of normalized affiliations: 
        inst_filter_list = ['LITEN Rto', 'INES Campus', 'PROMES CNRS-Lab'), 'Lund Univ']. 
        
    The "addr_country_inst_df" dataframe will be expended with the following columns (for pub_id = 0):
            LITEN Rto    INES Site    PROMES CNRS-Lab     Lund Univ                   
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
        cols_tup (tup): Columns information as built through the `_set_wos_parsing_cols` internal function.
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
    cols_lists_dic, cols_dic, wos_cols_dic = cols_tup
    auth_inst_cols_list = cols_lists_dic['auth_inst_cols_list']
    cols_keys = ['pub_id_col', 'auth_inst_author_idx_col', 'norm_institution_col']
    (pub_id_col, author_idx_col, norm_institution_col) = [cols_dic[key] for key in cols_keys]
    wos_cols_keys = ['wos_auth_with_aff_col', 'wos_fullnames_col']
    (wos_auth_with_aff_col, wos_fullnames_col) = [wos_cols_dic[key] for key in wos_cols_keys]
    
    # Setting namedtuples
    addr_country_inst  = namedtuple('address',auth_inst_cols_list[:-1] )
    author_address_tup = namedtuple('author_address','author address')
    
    # Building the useful data for affiliations normalization
    norm_raw_aff_dict = build_norm_raw_affiliations_dict(country_affiliations_file_path=country_affiliations_file_path,
                                                         verbose=False)
    aff_type_dict = read_inst_types(inst_types_file_path=inst_types_file_path, inst_types_usecols=None)
    towns_dict = read_towns_per_country(country_towns_file=country_towns_file,
                                        country_towns_folder_path=country_towns_folder_path)

    # Building the "addr_country_inst_list" list
    # with one item per publication and per author identifier
    corpus_series_zip = zip(corpus_df[pub_id_col],
                            corpus_df[wos_fullnames_col],
                            corpus_df[wos_auth_with_aff_col])
    pub_nb = len(corpus_df[pub_id_col])
    pub_num = 0
    addr_country_inst_list = []
    for pub_id, authors_str, affiliations_str in corpus_series_zip:
        pub_num += 1 
        print("    Publications number:", pub_num, f"/ {pub_nb}", end="\r")
        if '[' in affiliations_str:
            # Proceeding if the field author is present in affiliations.

            # Checking authors in authors list and authors-with-affiliation data
            authors_ordered_list, affil_authors_list, out_authors_list = _check_authors_list(authors_str, affiliations_str)

            # Building the list of tuples [([Author1, Author2,...], address1),...]
            # from the author-with-affiliations field in the corpus data
            affiliations_list = [x.strip() for x in bp_rg.RE_ADDRESS.findall(affiliations_str)]
            affiliations_list = affiliations_list if affiliations_list else ['']
            tuples_list = tuple(zip(affil_authors_list, affiliations_list)) 

            # Builds the list of tuples [(Author<0>, address<0>),(Author<0>, address<1>),...,(Author<i>, address<j>)...]
            author_address_tup_list = [author_address_tup(y, x[1]) for x in tuples_list for y in x[0]]

            for tup_num, tup in enumerate(author_address_tup_list):
                if tup.author in authors_ordered_list: 
                    author_idx = authors_ordered_list.index(tup.author)

                    author_country_raw = tup.address.split(',')[-1].replace(';','').strip()
                    author_country = normalize_country(author_country_raw)

                    author_raw_address = tup.address
                    author_std_address = standardize_address(author_raw_address)

                    author_institutions_tup = address_inst_full_list(author_std_address, norm_raw_aff_dict,
                                                                     aff_type_dict, towns_dict,
                                                                     drop_status=False)
                    addr_country_inst_list.append(addr_country_inst(pub_id, author_idx, author_std_address, author_country,
                                                                    author_institutions_tup.norm_inst_list,
                                                                    author_institutions_tup.raw_inst_list,))
            if out_authors_list:
                for out_author in out_authors_list:
                    out_author_idx = authors_ordered_list.index(out_author)
                    out_author_address = set_unknown_address(out_author_idx)
                    addr_country_inst_list.append(addr_country_inst(pub_id, out_author_idx, out_author_address,
                                                                    bp_sg.UNKNOWN_COUNTRY, bp_sg.EMPTY, bp_sg.EMPTY,))
        else:
            # If the field author is not present in affiliations complete namedtuple with the global UNKNOWN
            addr_country_inst_list.append(addr_country_inst(pub_id, bp_sg.UNKNOWN, bp_sg.UNKNOWN, 
                                                            bp_sg.UNKNOWN, bp_sg.UNKNOWN, bp_sg.UNKNOWN,))
    # Building a clean addresses-country-inst dataframe and accordingly updating the parsing success rate dict
    addr_country_inst_df, fails_dic = build_item_df_from_tup(addr_country_inst_list, auth_inst_cols_list[:-1],
                                                             norm_institution_col, pub_id_col, fails_dic)   
    addr_country_inst_df = clean_authors_countries_institutions(addr_country_inst_df)
    
    if inst_filter_list is not None:
        addr_country_inst_df = extend_author_institutions(addr_country_inst_df, inst_filter_list)
        
    # Sorting the values in the dataframe returned by two columns
    addr_country_inst_df.sort_values(by=[pub_id_col, author_idx_col], inplace=True)

    return addr_country_inst_df


def _build_subjects_wos(corpus_df, fails_dic, cols_tup):
    """Builds the data of subject per publication of the corpus 
    and updates the parsing success rate data.
    
    The structure of the built data is composed of 2 columns and one row 
    per publication and subject.
        Ex:
            Pub_id   Subject
               0    Neurosciences & Neurology
               1    Psychology
               1    Environmental Sciences & Ecology
               2    Engineering
               2    Physics
               3    Philosophy

    Args:
        corpus_df (dataframe): The selected rawdata of the corpus.
        fails_dic (dict): Parsing success rate data.
        cols_tup (tup): Columns information as built through \
        the `_set_wos_parsing_cols` internal function.
    Returns:
        (dataframe): The built data.
    """
    # Setting useful column names
    cols_lists_dic, cols_dic, wos_cols_dic = cols_tup
    subject_cols_list = cols_lists_dic['subject_cols_list']
    cols_keys = ['pub_id_col', 'subject_col']
    (pub_id_col, subject_col) = [cols_dic[key] for key in cols_keys]
    wos_subjects_col = wos_cols_dic['wos_subjects_col']
    
    # Setting named tuple
    subject = namedtuple('subject', subject_cols_list)    
    
    subjects_list = []
    for pub_id, pub_subjects_str in zip(corpus_df[pub_id_col], corpus_df[wos_subjects_col]):
        for pub_subject in pub_subjects_str.split(';'):
            subjects_list.append(subject(pub_id, pub_subject.strip()))
     
    # Building a clean subjects dataframe and accordingly updating the parsing success rate dict
    subjects_df, fails_dic = build_item_df_from_tup(subjects_list, subject_cols_list, 
                                                    subject_col, pub_id_col, fails_dic)
    return subjects_df


def _build_sub_subjects_wos(corpus_df, fails_dic, cols_tup):
    """Builds the data of sub-subject per publication of the corpus 
    and updates the parsing success rate data.
    
    The structure of the built data is composed of 2 columns and one row 
    per publication and sub-subject.
        Ex:
            Pub_id   Sub_subject
               0    Engineering
               1    Materials Science
               1    Physics
               2    Materials Science
               2    Physics
               3    Chemistry

    Args:
        corpus_df (dataframe): The selected rawdata of the corpus.
        fails_dic (dict): Parsing success rate data.
        cols_tup (tup): Columns information as built through \
        the `_set_wos_parsing_cols` internal function.
    Returns:
        (dataframe): The built data.
    """
    # Setting useful column names
    cols_lists_dic, cols_dic, wos_cols_dic = cols_tup
    sub_subject_cols_list = cols_lists_dic['sub_subject_cols_list']
    cols_keys = ['pub_id_col', 'sub_subject_col']
    (pub_id_col, sub_subject_col) = [cols_dic[key] for key in cols_keys]
    wos_sub_subjects_col = wos_cols_dic['wos_sub_subjects_col']
    
    # Setting named tuple
    sub_subject = namedtuple('sub_subject', sub_subject_cols_list ) 

    sub_subjects_list = []
    for pub_id, pub_sub_subjects_str in zip(corpus_df[pub_id_col], corpus_df[wos_sub_subjects_col]):
        if isinstance(pub_sub_subjects_str, str):
            for pub_sub_subject in pub_sub_subjects_str.split(';'):
                sub_subjects_list.append(sub_subject(pub_id, pub_sub_subject.strip()))
    
    # Building a clean sub_subjects dataframe and accordingly updating the parsing success rate dict
    sub_subjects_df, fails_dic = build_item_df_from_tup(sub_subjects_list, sub_subject_cols_list, 
                                                        sub_subject_col, pub_id_col, fails_dic)    
    return sub_subjects_df


def _build_articles_wos(corpus_df, cols_tup):
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
        the `_set_wos_parsing_cols` internal function.
    Returns:
        (dataframe): The built data.
    """
    # Internal functions
    def _str_int_convertor(x):
        try:
            return(int(float(x)))
        except:
            return 0
    
    def _treat_author(authors_list):
        # Picking the first author
        raw_first_author = authors_list.split(';')[0] 
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

    # Setting useful column names
    cols_lists_dic, cols_dic, wos_cols_dic = cols_tup
    articles_cols_list = cols_lists_dic['articles_cols_list']
    cols_keys = ['pub_id_col', 'author_col', 'year_col', 'doc_type_col',
                 'title_col', 'issn_col', 'norm_journal_col']
    (pub_id_col, author_col, year_col, doc_type_col, title_col,
     issn_col, norm_journal_col) = [cols_dic[key] for key in cols_keys]

    wos_cols_keys = ['wos_auth_col', 'wos_year_col', 'wos_journal_col',
                     'wos_volume_col', 'wos_page_col', 'wos_doi_col',
                     'wos_doctype_col', 'wos_language_col', 'wos_title_kw_col',
                     'wos_issn_col']
    wos_cols_list = [wos_cols_dic[key] for key in wos_cols_keys]
    
    articles_wos_cols = wos_cols_list + [norm_journal_col]
    articles_df = corpus_df[articles_wos_cols].astype(str)
    articles_df.rename(columns=dict(zip(articles_wos_cols, articles_cols_list[1:])),
                       inplace=True)    
                                                                                                
    articles_df[author_col] = articles_df[author_col].apply(_treat_author)    
    articles_df[year_col] = articles_df[year_col].apply(_str_int_convertor)
    articles_df[doc_type_col] = articles_df[doc_type_col].apply(_treat_doctype)
    articles_df[title_col] = articles_df[title_col].apply(_treat_title)
    
    articles_df.insert(0, pub_id_col, list(corpus_df[pub_id_col]))
   
    return articles_df


def _build_references_wos(corpus_df, cols_tup):
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
        the `_set_wos_parsing_cols` internal function.
    Returns:
        (dataframe): The built data.
    """
    # Setting useful column names
    cols_lists_dic, cols_dic, wos_cols_dic = cols_tup
    ref_cols_list = cols_lists_dic['ref_cols_list']
    pub_id_col = cols_dic['pub_id_col']
    wos_ref_col = wos_cols_dic['wos_ref_col']
 
    # Setting named tuple
    article_ref = namedtuple('article_ref', ref_cols_list)
    
    refs_list =[]
    for pub_id, row in zip(list(corpus_df[pub_id_col]),
                                corpus_df[wos_ref_col]):
        if isinstance(row, str):
            # If the reference field is not empty and not an URL
            for field in row.split(";"):
                year = re.findall(bp_rg.RE_REF_YEAR_WOS, field) 
                if len(year):
                    year = year[0][1:-1]
                else:
                    year = 0

                vol = re.findall(bp_rg.RE_REF_VOL_WOS, field)
                if len(vol):
                    vol = vol[0][3:]
                else:
                    vol = 0

                page = re.findall(bp_rg.RE_REF_PAGE_WOS, field)
                if len(page):
                    page = page[0][3:]
                else:
                    page = 0

                journal = re.findall(bp_rg.RE_REF_JOURNAL_WOS, field)
                if len(journal):
                    journal = journal[0].strip()
                else:
                    journal = bp_sg.UNKNOWN

                author = re.findall(bp_rg.RE_REF_AUTHOR_WOS, field)
                if len(author):
                    author = normalize_name(author[0][:-1])
                else:
                    author = bp_sg.UNKNOWN

                if (author!=bp_sg.UNKNOWN) and (journal!=bp_sg.UNKNOWN):
                    refs_list.append(article_ref(pub_id, author, year, journal, vol,page))

                if (vol==0) & (page==0) & (author!=bp_sg.UNKNOWN):
                    pass
    
    references_df = pd.DataFrame.from_dict({label:[s[idx] for s in refs_list] 
                                            for idx,label in enumerate(ref_cols_list)})
    return references_df


def read_database_wos(rawdata_path, wos_ids=False):
    """Reads the file of WoS rawdata available in the indicated folder.
 
    The function:
    - Allows to circumvent the error ParserError ('	' expected after '"') generated \
    by the method `pd.read_csv` when reading the raw wos-database file
    - Checks columns and drops unused columns by the parsing process using the \
    `check_and_drop_columns` function imported from `BiblioParsingUtils` module.
    - Replaces the unavailable items values by a string set in the global UNKNOWN.
    - Adds an index column.
    - Normalizes the journal names using the `normalize_journal_names` function \
    imported from the `BiblioParsingUtils` module.
    Finally, the function can built data of WoS identifiers of the publications.
    The returned data are initialized to empty dataframes.

    Args:
        rawdata_path (path): The full path to the WoS-rawdata file.
        correct_data (bool): Optional, true for correcting authors' names \
        and addresses (dafault=False).
        wos_ids (bool): Optional, true for building the data of WoS IDs of \
        publications (dafault=False).
    Returns:
        (tup): (The cleaned corpus data (dataframe), The WoS-IDs data (dataframe)). 
    """
    # Setting columns for wos parsing process
    cols_tup = _set_wos_parsing_cols()
    _, cols_dic, wos_cols_dic = cols_tup
    wos_id_col = cols_dic['wos_id_col']
    init_wos_id_col = wos_cols_dic['init_wos_id_col']

    # Initializing returned data to empty dataframes
    wos_rawdata_df = pd.DataFrame()
    wos_ids_df = pd.DataFrame()
    
    # Check if rawdata file is available and get its full path if it is
    rawdata_file_path = check_and_get_rawdata_file_path(rawdata_path, bp_sg.WOS_RAWDATA_EXTENT)

    if rawdata_file_path: 
        # Extending the field size limit for reading .txt files
        csv.field_size_limit(bp_sg.FIELD_SIZE_LIMIT)

        with open(rawdata_file_path, 'rt', encoding=bp_sg.ENCODING) as csv_file: 
            csv_reader = csv.reader(csv_file, delimiter='\t')
            csv_list = []
            for row in csv_reader:
                csv_list.append(row)
        full_wos_rawdata_df = pd.DataFrame(csv_list)

        if len(full_wos_rawdata_df):
            # Setting columns name to raw 0
            full_wos_rawdata_df.columns = full_wos_rawdata_df.iloc[0]
            full_wos_rawdata_df = full_wos_rawdata_df.drop(0)

            # Selecting useful rawdata
            wos_rawdata_df = check_and_drop_columns(bp_sg.WOS, full_wos_rawdata_df)
            wos_rawdata_df = wos_rawdata_df.replace(np.nan, bp_sg.UNKNOWN, regex=True)
            wos_rawdata_df = normalize_journal_names(bp_sg.WOS, wos_rawdata_df)
            return_tup = (wos_rawdata_df)
        
            if wos_ids:
                # Building the WoS-IDs data
                wos_ids_df = build_pub_db_ids(full_wos_rawdata_df, init_wos_id_col, wos_id_col)
    return_tup = (wos_rawdata_df, wos_ids_df)
    return return_tup


def biblio_parser_wos(rawdata_path, inst_filter_list=None, country_affiliations_file_path=None,
                      inst_types_file_path=None, country_towns_file=None,
                      country_towns_folder_path=None):
    """Builds parsing data from the corpus rawdata.

    The list of the parsed items (keys of the returned dict which values are the dataframes \
    of the parsing results) is given by the PARSING_ITEMS_LIST global. 
    The rawdata are parsed using the following internal functions:
    - `_build_authors_wos` which parses the authors' rawdata;
    - `_build_keywords_wos` which parses the authors' keywords and the indexed keywords rawdata \
    and builds the title keywords from the publication title;
    - `_build_addresses_countries_institutions_wos` which parses the author-with-affilations \
    rawdata by publication;
    - `_build_authors_countries_institutions_wos` which parses the author-with-affilations \
    rawdata by authors;
    - `_build_subjects_wos` which parses the subjects rawdata;
    - `_build_sub_subjects_wos` which parses the secondary subjects rawdata;
    - `_build_articles_wos` which parses selected attributes of the publications given \
    by the corpus rawdata.
    - `_build_references_wos` which parses the references rawdata by publication.

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
        The parsing success rate data (dict), The data (dataframe) of WoS IDs of publications.
    """
    # Internal functions    
    def _keeping_item_parsing_results(item, item_df):
        wos_parsing_dict[item] = item_df

    # Setting columns for wos parsing process
    cols_tup = _set_wos_parsing_cols()
    cols_lists_dic, cols_dic, wos_cols_dic = cols_tup

    # Setting items list and values
    items_list = [bp_sg.PARSING_ITEMS_LIST[x] for x in range(12)]
    (articles_item, authors_item, addresses_item, countries_item, institutions_item,
     auth_inst_item, authors_kw_item, index_kw_item, title_kw_item, subjects_item,
     sub_subjects_item, references_item) = items_list
    
    # Reading and checking the raw corpus file
    corpus_df, wos_ids_df = read_database_wos(rawdata_path, wos_ids=True)
    
    # Initializing the fails_dic dict for the parsing control
    wos_fails_dic = {}    
    
    # Initializing the dict of dataframes resulting from the parsing
    wos_parsing_dict = {}
    
    if corpus_df is not None:
        
        # Keeping the number of articles in wos_fails_dic dict
        wos_fails_dic['number of article'] = len(corpus_df)
    
        # Building the dataframe of articles
        print(f"  - {articles_item} parsing...", end="\r")
        articles_df = _build_articles_wos(corpus_df, cols_tup)
        _keeping_item_parsing_results(articles_item, articles_df)
        print(f"  - {articles_item} parsed    ")    

        # Building the dataframe of authors
        print(f"  - {authors_item} parsing...", end="\r")
        authors_df = _build_authors_wos(corpus_df, wos_fails_dic, cols_tup)
        _keeping_item_parsing_results(authors_item, authors_df)
        print(f"  - {authors_item} parsed    ")

        # Building the dataframe of addresses, countries and institutions
        print(f"  - {addresses_item}, {countries_item} and {institutions_item} parsing...", end="\r")
        addresses_tup = _build_addresses_countries_institutions_wos(corpus_df, wos_fails_dic, cols_tup)
        addresses_df, countries_df, institutions_df = addresses_tup
        _keeping_item_parsing_results(addresses_item, addresses_df)
        _keeping_item_parsing_results(countries_item, countries_df)
        _keeping_item_parsing_results(institutions_item, institutions_df)
        print(f"  - {addresses_item}, {countries_item} and {institutions_item} parsed    ")

        # Building the dataframe of authors and their institutions
        print(f"  - {auth_inst_item} parsing...")
        auth_inst_df = _build_authors_countries_institutions_wos(corpus_df, wos_fails_dic, cols_tup, 
                                                                 inst_filter_list = inst_filter_list ,
                                                                 country_affiliations_file_path = country_affiliations_file_path,
                                                                 inst_types_file_path = inst_types_file_path,
                                                                 country_towns_file = country_towns_file,
                                                                 country_towns_folder_path = country_towns_folder_path)
        _keeping_item_parsing_results(auth_inst_item, auth_inst_df)
        print(f"       {auth_inst_item} parsed    ")

        # Building the dataframes of keywords
        print(f"  - {authors_kw_item}, {index_kw_item} and {title_kw_item} parsing...", end="\r")
        AK_keywords_df, IK_keywords_df, TK_keywords_df = _build_keywords_wos(corpus_df, wos_fails_dic, cols_tup)
        _keeping_item_parsing_results(authors_kw_item, AK_keywords_df)
        _keeping_item_parsing_results(index_kw_item, IK_keywords_df)
        _keeping_item_parsing_results(title_kw_item, TK_keywords_df)
        print(f"  - {authors_kw_item}, {index_kw_item} and {title_kw_item} parsed    ")

        # Building the dataframe of subjects
        print(f"  - {subjects_item} parsing...", end="\r")
        subjects_df = _build_subjects_wos(corpus_df, wos_fails_dic, cols_tup)
        _keeping_item_parsing_results(subjects_item, subjects_df)
        print(f"  - {subjects_item} parsed    ")

        # Building the dataframe of sub-subjects
        print(f"  - {sub_subjects_item} parsing...", end="\r")
        sub_subjects_df = _build_sub_subjects_wos(corpus_df, wos_fails_dic, cols_tup)
        _keeping_item_parsing_results(sub_subjects_item, sub_subjects_df)
        print(f"  - {sub_subjects_item} parsed    ")

        # Building the dataframe of references
        print(f"  - {references_item} parsing...", end="\r")
        references_df = _build_references_wos(corpus_df, cols_tup)
        _keeping_item_parsing_results(references_item, references_df)
        print(f"  - {references_item} parsed    ")
        
    return wos_parsing_dict, wos_fails_dic, wos_ids_df
