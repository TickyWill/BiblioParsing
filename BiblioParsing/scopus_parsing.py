"""Module of functions for parsing of Scopus rawdata.
"""

__all__ = ['scopus_parser']


# Standard library imports
from collections import namedtuple
from pathlib import Path

# 3rd party library imports
import pandas as pd

# Local libray imports
import BiblioParsing.general_globals as bp_gg
import BiblioParsing.parsing_cols_globals as bp_pcg
import BiblioParsing.parsing_globals as bp_pg
from BiblioParsing.affiliations_parsing import build_addr_affils_tup
from BiblioParsing.affiliations_parsing import extend_author_affils
from BiblioParsing.parsing_utils import build_item_df_from_tup
from BiblioParsing.parsing_utils import build_title_keywords
from BiblioParsing.parsing_utils import clean_authors_countries_affils
from BiblioParsing.parsing_utils import convert_issn
from BiblioParsing.parsing_utils import normalize_country
from BiblioParsing.parsing_utils import set_unknown_address
from BiblioParsing.parsing_utils import standardize_address
from BiblioParsing.parsing_utils import str_int_convertor
from BiblioParsing.parsing_utils import treat_author
from BiblioParsing.parsing_utils import treat_doctype
from BiblioParsing.parsing_utils import treat_title
from BiblioParsing.scopus_parsing_complements import build_scopus_references
from BiblioParsing.scopus_parsing_complements import build_scopus_subjects_and_sub_subjects
from BiblioParsing.scopus_rawdata_utils import read_scopus_rawdata


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
    cols_lists_dic = {'articles_cols_list'  : bp_pcg.COL_NAMES['articles'],
                      'address_cols_list'   : bp_pcg.COL_NAMES['address'],
                      'auth_cols_list'      : bp_pcg.COL_NAMES['authors'],
                      'auth_affil_cols_list': bp_pcg.COL_NAMES['auth_inst'],
                      'country_cols_list'   : bp_pcg.COL_NAMES['country'],
                      'affil_cols_list'     : bp_pcg.COL_NAMES['institution'],
                      'kw_cols_list'        : bp_pcg.COL_NAMES['keywords'],
                      'tmp_cols_list'       : bp_pcg.COL_NAMES['temp_col'],
                      'ref_cols_list'       : bp_pcg.COL_NAMES['references'],
                     }

    cols_dic = {'scopus_id_col'       : bp_pcg.COL_NAMES['scopus_id'][0],
                'pub_id_col'          : bp_pcg.COL_NAMES['pub_id'],
                'subject_col'         : bp_pcg.COL_NAMES['subject'][1],
                'sub_subject_col'     : bp_pcg.COL_NAMES['sub_subject'][1],
                'affil_author_idx_col': bp_pcg.COL_NAMES['auth_inst'][1],
                'norm_affils_col'     : bp_pcg.COL_NAMES['auth_inst'][4],
                'address_col'         : bp_pcg.COL_NAMES['address'][2],
                'country_col'         : bp_pcg.COL_NAMES['country'][2],
                'affil_col'           : bp_pcg.COL_NAMES['institution'][2],
                'author_idx_col'      : bp_pcg.COL_NAMES['authors'][1],
                'co_authors_col'      : bp_pcg.COL_NAMES['authors'][2],
                'keyword_col'         : bp_pcg.COL_NAMES['keywords'][1],
                'title_temp_col'      : bp_pcg.COL_NAMES['temp_col'][2],
                'kept_tokens_col'     : bp_pcg.COL_NAMES['temp_col'][4],
                'author_col'          : bp_pcg.COL_NAMES['articles'][1],
                'year_col'            : bp_pcg.COL_NAMES['articles'][2],
                'doc_type_col'        : bp_pcg.COL_NAMES['articles'][7],
                'title_col'           : bp_pcg.COL_NAMES['articles'][9],
                'issn_col'            : bp_pcg.COL_NAMES['articles'][10],
                'norm_journal_col'    : bp_pcg.NORM_JOURNAL_COLUMN_LABEL,
               }

    scopus_cols_dic = {'scopus_auth_col'         : bp_pcg.COLUMN_LABEL_SCOPUS['authors'],
                       'scopus_title_kw_col'     : bp_pcg.COLUMN_LABEL_SCOPUS['title'],
                       'scopus_year_col'         : bp_pcg.COLUMN_LABEL_SCOPUS['year'],
                       'scopus_journal_col'      : bp_pcg.COLUMN_LABEL_SCOPUS['journal'],
                       'scopus_volume_col'       : bp_pcg.COLUMN_LABEL_SCOPUS['volume'],
                       'scopus_page_col'         : bp_pcg.COLUMN_LABEL_SCOPUS['page_start'],
                       'scopus_doi_col'          : bp_pcg.COLUMN_LABEL_SCOPUS['doi'],
                       'scopus_aff_col'          : bp_pcg.COLUMN_LABEL_SCOPUS['affiliations'],
                       'scopus_auth_with_aff_col': bp_pcg.COLUMN_LABEL_SCOPUS['authors_with_affiliations'],
                       'scopus_auth_kw_col'      : bp_pcg.COLUMN_LABEL_SCOPUS['author_keywords'],
                       'scopus_idx_kw_col'       : bp_pcg.COLUMN_LABEL_SCOPUS['index_keywords'],
                       'scopus_ref_col'          : bp_pcg.COLUMN_LABEL_SCOPUS['references'],
                       'scopus_issn_col'         : bp_pcg.COLUMN_LABEL_SCOPUS['issn'],
                       'scopus_language_col'     : bp_pcg.COLUMN_LABEL_SCOPUS['language'],
                       'scopus_doctype_col'      : bp_pcg.COLUMN_LABEL_SCOPUS['document_type'],
                       'scopus_fullnames_col'    : bp_pcg.COLUMN_LABEL_SCOPUS_PLUS['auth_fullnames'],
                       'init_scopus_id_col'      : bp_pcg.COLUMN_LABEL_SCOPUS_PLUS['scopus_id'],
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

    # Building "addr_country_affil" namedtuple for the author of the publication
    author_affiliations_str = ','.join(author_affiliations_list[auth_item_nbr:])

    author_std_affiliations_list = []
    for raw_affiliation in affiliations_list:
        std_affiliation = standardize_address(raw_affiliation,
                                              add_unknown_country=False)
        if std_affiliation in author_affiliations_str:
            full_std_affiliation = standardize_address(raw_affiliation,
                                                       add_unknown_country=True)
            author_std_affiliations_list.append(full_std_affiliation)
    return author_std_affiliations_list, author_counter_params


def _build_scopus_authors(corpus_df, fails_dic, cols_tup):
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


def _build_scopus_keywords(corpus_df, fails_dic, cols_tup):
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
    kw_cols_list = cols_lists_dic['kw_cols_list']
    cols_keys = ['pub_id_col', 'keyword_col', 'title_temp_col', 'kept_tokens_col']
    (pub_id_col, keyword_col, title_temp_col, kept_tokens_col) = [cols_dic[key] for key in cols_keys]
    scopus_cols_keys = ['scopus_auth_kw_col', 'scopus_idx_kw_col', 'scopus_title_kw_col']
    (scopus_auth_kw_col, scopus_idx_kw_col,
     scopus_title_kw_col )= [scopus_cols_dic[key] for key in scopus_cols_keys]

    # Setting named tuple
    key_word = namedtuple('key_word', kw_cols_list)

    aks_list = []
    aks_df = corpus_df[scopus_auth_kw_col].fillna('')
    for pub_id, pub_aks_str in zip(corpus_df[pub_id_col], aks_df):
        pub_aks_list = pub_aks_str.split(';')
        for pub_ak in pub_aks_list:
            pub_ak = pub_ak.lower().strip()
            aks_list.append(key_word(pub_id, pub_ak if pub_ak!='null' else bp_pg.UNKNOWN))

    iks_list = []
    iks_df = corpus_df[scopus_idx_kw_col].fillna('')
    for pub_id, pub_iks_str in zip(corpus_df[pub_id_col], iks_df):
        pub_iks_list = pub_iks_str.split(';')
        for pub_ik in pub_iks_list:
            pub_ik = pub_ik.lower().strip()
            iks_list.append(key_word(pub_id, pub_ik if pub_ik!='null' else bp_pg.UNKNOWN))

    tks_list = []
    title_df = pd.DataFrame(corpus_df[scopus_title_kw_col].fillna(''))
    title_df.columns = [title_temp_col]
    tks_df, _ = build_title_keywords(title_df)
    for pub_id in corpus_df[pub_id_col]:
        for token in tks_df.loc[pub_id, kept_tokens_col]:
            token = token.lower().strip()
            tks_list.append(key_word(pub_id, token if token!='null' else bp_pg.UNKNOWN))

    # Building a clean author keywords dataframe and accordingly updating the parsing success rate dict
    auth_kw_df, fails_dic = build_item_df_from_tup(aks_list, kw_cols_list,
                                                   keyword_col, pub_id_col, fails_dic)

    # Building a clean index keywords dataframe and accordingly updating the parsing success rate dict
    index_kw_df, fails_dic = build_item_df_from_tup(iks_list, kw_cols_list,
                                                    keyword_col, pub_id_col, fails_dic)

    # Building a clean title keywords dataframe and accordingly updating the parsing success rate dict
    title_kw_df, fails_dic = build_item_df_from_tup(tks_list, kw_cols_list,
                                                    keyword_col, pub_id_col, fails_dic)

    return auth_kw_df, index_kw_df, title_kw_df


def _build_scopus_addresses_countries_affiliations(corpus_df, fails_dic, cols_tup):
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

             Pub-index  Address-index         Address
                 0         0              NaMLab, TU Dresden, Nothnitzer Str. 64a, Dresden, 01187, Germany
                 0         1              University Grenoble Alpes, Grenoble, F-38000, France
                 0         2              Hitachi Cambridge Laboratory, Cambridge, United Kingdom

        - for the countries data:

             Pub-index  Address-index       Country
                 0         0               Germany
                 0         1               France
                 0         2               United Kingdom

        - for the main affiliations data:

             Pub-index  Address-index        Main affiliation
                 0         0            NaMLab
                 0         1            University Grenoble Alpes
                 0         2            Hitachi Cambridge Laboratory

    Args:
        corpus_df (dataframe): The selected rawdata of the corpus.
        fails_dic (dict): Parsing success rate data.
        cols_tup (tup): Columns information as built through the `_set_scopus_parsing_cols` internal function.
    Returns:
        (tup): (The built addresses data (dataframe), tha built countries data (dataframe), \
        The built main affiliations data (dataframe)).
    """
    # Setting useful column names
    cols_lists_dic, cols_dic, scopus_cols_dic = cols_tup
    cols_lists_keys = ['address_cols_list', 'country_cols_list', 'affil_cols_list']
    address_cols_list, country_cols_list, affil_cols_list = [cols_lists_dic[key] for key in cols_lists_keys]
    cols_keys = ['pub_id_col', 'address_col', 'country_col', 'affil_col']
    (pub_id_col, address_col, country_col, affil_col) = [cols_dic[key] for key in cols_keys]
    scopus_cols_keys = ['scopus_aff_col', 'scopus_auth_with_aff_col']
    (scopus_aff_col, scopus_auth_with_aff_col) = [scopus_cols_dic[key] for key in scopus_cols_keys]

    # Setting named tuples
    address_tup = namedtuple('address', address_cols_list)
    country_tup = namedtuple('country', country_cols_list)
    affil_tup = namedtuple('affiliation', affil_cols_list)

    # Building "addresses_list", "countries_list", "affils_list" lists
    # with one item per publication and per address identifier
    corpus_series_zip = zip(corpus_df[pub_id_col],
                            corpus_df[scopus_aff_col],
                            corpus_df[scopus_auth_with_aff_col])
    addresses_list, countries_list, affils_list = [], [], []
    for pub_id, affiliations_str, authors_affiliations_str in corpus_series_zip:
        affiliations_list = affiliations_str.split(';')

        # Initializing the authors' counter and the last-author name
        author_counter_params = [-1, '']

        # Checking if all authors have affiliation
        authors_affiliations_list = authors_affiliations_str.split(';')
        for raw_author_affiliations_str in authors_affiliations_list:
            return_tup = _get_author_affiliations_list(raw_author_affiliations_str, affiliations_list,
                                                       author_counter_params)
            author_std_affiliations_list, author_counter_params = return_tup
            author_idx = author_counter_params[0]
            if not author_std_affiliations_list:
                affiliations_list.append(set_unknown_address(author_idx))

        if affiliations_list:
            for address_idx, pub_address in enumerate(affiliations_list):
                addresses_list.append(address_tup(pub_id, address_idx, pub_address))

                addresses_split = pub_address.split(',')
                affils_nb = len(addresses_split)
                affil_num = 0
                main_affil = addresses_split[affil_num]
                if not main_affil and affils_nb:
                    while not main_affil and affil_num<affils_nb:
                        affil_num += 1
                        main_affil = pub_address.split(',')[affil_num]
                affils_list.append(affil_tup(pub_id, address_idx, main_affil))

                country_raw = pub_address.split(',')[-1].replace(';','').strip()
                country = normalize_country(country_raw)
                countries_list.append(country_tup(pub_id, address_idx, country))
        else:
            addresses_list.append(address_tup(pub_id, 0, ''))
            affils_list.append(affil_tup(pub_id, 0, ''))
            countries_list.append(country_tup(pub_id, 0, ''))

    # Building a clean addresses dataframe and accordingly updating the parsing success rate dict
    addresses_df, fails_dic = build_item_df_from_tup(addresses_list, address_cols_list,
                                                     address_col, pub_id_col, fails_dic)

    # Building a clean countries dataframe and accordingly updating the parsing success rate dict
    countries_df, fails_dic = build_item_df_from_tup(countries_list, country_cols_list,
                                                     country_col, pub_id_col, fails_dic)

    # Building a clean affiliations data and accordingly updating the parsing success rate dict
    affiliations_df, fails_dic = build_item_df_from_tup(affils_list, affil_cols_list,
                                                        affil_col, pub_id_col, fails_dic)

    if not len(addresses_df)==len(countries_df)==len(affiliations_df):
        warning = ('\nWARNING: Lengths of "addresses_df", "countries_df" and "affiliations_df" data are not equal '
                   'in "_build_scopus_addresses_countries_affiliations" function of "scopus_parsing.py" module')
        print(warning)
    return addresses_df, countries_df, affiliations_df


def _build_scopus_authors_countries_affiliations(corpus_df, fails_dic, cols_tup,
                                                 affil_filter_list=None, affil_params_dic=None):
    """Parses the fields 'Affiliations' and 'Authors with affiliations' of the corpus to build 
    the data of authors their addresses, country and normalized affiliations per publication of the corpus. 

    The parsing success rate data are updated. 
    In addition, the built data may be expanded according to a filtering of affiliations. 
    The parsing is effective only for the format of the following example. Otherwise, the parsing 
    fields are set to empty strings.

    For example, the 'Authors with affiliations' field string:

       'Boujjat, H., CEA, LITEN Solar & Thermod Syst Lab L2ST, F-38054 Grenoble, France,
        Univ Grenoble Alpes, F-38000 Grenoble, France;
        Rodat, S., CNRS, Proc Mat & Solar Energy Lab, PROMES, 7 Rue Four Solaire, F-66120 Font Romeu, France;
        Chuayboon, S., CNRS, Proc Mat & Solar Energy Lab, PROMES, 7 Rue Four Solaire, F-66120 Font Romeu, France;
        Abanades, S., CEA, Leti, 17 rue des Martyrs, F-38054 Grenoble, France;
        Dupont, S., CEA, Liten, INES. 50 avenue du Lac, F-73370 Le Bourget-du-Lac, France;
        Durand, M., CEA, INES, DTS, 50 avenue du Lac, F-73370 Le Bourget-du-Lac, France;
        David, D., Lund University, Department of Phys Geography and Ecosystem Science (INES), Lund, Sweden'

     will be parsed in the "auth_affils_df" dataframe if affiliation filter is not defined (initialization step):

        Pub_id  Idx_author                   Address         Country Norm_affiliations            Raw_affiliations
            0       0      CEA, LITEN Solar & Thermod , ...  France  CEA Nro;LITEN Rto            F-38054 Grenoble
            0       0      Univ Grenoble Alpes,...           France  UGA Univ                     F-38000 Grenoble
            0       1      CNRS, Proc Mat Lab, PROMES,...    France  CNRS Nro;PROMES CNRS-Lab     7 Rue Four Solaire;...
            0       2      CNRS, Proc Mat Lab, PROMES, ...   France  CNRS Nro;PROMES CNRS-Lab     7 Rue Four Solaire;...
            0       3      CEA, Leti, 17 rue des Martyrs,... France  CEA Nro;LETI Rto             17 rue des Martyrs;...
            0       4      CEA, Liten, INES. 50 avenue...    France  CEA Nro;LITEN Rto;INES Site  50 avenue du Lac;...
            0       5      CEA, INES, DTS, 50 avenue...      France  CEA Nro;INES Site            DTS;...
            0       6      Lund University,...(INES),...     Sweden  Lund Univ                    Department of Phys ...

    given that the 'Affiliations' field string is:

        'CEA, LITEN Solar & Thermod Syst Lab L2ST, F-38054 Grenoble, France;
         Univ Grenoble Alpes, F-38000 Grenoble, France;
         CNRS, Proc Mat & Solar Energy Lab, PROMES, 7 Rue Four Solaire, F-66120 Font Romeu, France;
         CEA, Leti, 17 rue des Martyrs, F-38054 Grenoble, France;
         CEA, Liten, INES. 50 avenue du Lac, F-73370 Le Bourget-du-Lac, France;
         CEA, INES, DTS, 50 avenue du Lac, F-73370 Le Bourget-du-Lac, France;
         Lund University, Department of Physical Geography and Ecosystem Science (INES), Lund, Sweden'

    The affiliations are identified and normalized using dedicated data that should be specified by the user.

    If affiliation filter is defined based on the following list of normalized affiliations:
        affil_filter_list = ['LITEN Rto', 'INES Campus', 'PROMES CNRS-Lab'), 'Lund Univ'].

    The "auth_affils_df" dataframe will be expended with the following columns (for pub_id = 0):
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
        affil_filter_list (list): The affiliations-filter composed of a list of normalized affiliations (str), \
        optional (default=None).
        affil_params_dic (dict): Optional dict (default=None) keyed by ['affil_types_file_path', \
        'country_affils_file_path', 'country_towns_folder_path', 'country_towns_file'] and valued by the user as \
        the full path to the data per country of raw affiliations per normalized one, the full path to the data of \
        affiliations-types used to normalize the affiliations, the name of the file of the data of towns per country \
        and the full path to the folder where these data are available.
    Returns:
        (dataframe): The built data.
    """
    # Setting useful column names
    cols_lists_dic, cols_dic, scopus_cols_dic = cols_tup
    auth_affil_cols_list = cols_lists_dic['auth_affil_cols_list']
    cols_keys = ['pub_id_col', 'affil_author_idx_col', 'norm_affils_col']
    (pub_id_col, author_idx_col, norm_affils_col) = [cols_dic[key] for key in cols_keys]
    scopus_cols_keys = ['scopus_aff_col', 'scopus_auth_with_aff_col']
    (scopus_aff_col, scopus_auth_with_aff_col) = [scopus_cols_dic[key] for key in scopus_cols_keys]

    # Setting named tuples
    addr_country_affil = namedtuple('address', auth_affil_cols_list[:-1])

    # Building the "addr_country_affil_list" list
    # with one item per publication and per author identifier
    corpus_series_zip = zip(corpus_df[pub_id_col],
                            corpus_df[scopus_aff_col],
                            corpus_df[scopus_auth_with_aff_col])
    pub_nb = len(corpus_df[pub_id_col])
    pub_num = 0
    addr_country_affil_list = []
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
            author_std_affiliations_list, author_counter_params = return_tup
            author_idx = author_counter_params[0]
            if not author_std_affiliations_list:
                full_unknown_address = set_unknown_address(author_idx, add_unknown_country=True)
                author_std_affiliations_list.append(full_unknown_address)

            for author_std_affiliation in author_std_affiliations_list:
                author_country_raw = author_std_affiliation.split(',')[-1].strip()
                author_country = normalize_country(author_country_raw)
                author_affiliations_tup = build_addr_affils_tup(author_std_affiliation, affil_params_dic,
                                                                drop_status=False)
                addr_country_affil_list.append(addr_country_affil(pub_id, author_idx,
                                                                  author_std_affiliation, author_country,
                                                                  author_affiliations_tup.norm_affils_list,
                                                                  author_affiliations_tup.raw_affils_list,))
    # Building a clean author-country-affiliations data and accordingly updating the parsing success rate dict
    auth_affils_df, fails_dic = build_item_df_from_tup(addr_country_affil_list, auth_affil_cols_list[:-1],
                                                       norm_affils_col, pub_id_col, fails_dic)
    auth_affils_df = clean_authors_countries_affils(auth_affils_df)

    if affil_filter_list is not None:
        auth_affils_df = extend_author_affils(auth_affils_df, affil_filter_list)

    # Sorting the values in the dataframe returned by two columns
    auth_affils_df.sort_values(by = [pub_id_col, author_idx_col], inplace=True)
    return auth_affils_df


def _build_scopus_articles(corpus_df, fails_dic, cols_tup):
    """Builds selected data of publications.

    The structure of the built data is composed of 12 columns and one row per publication.
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
    # Keeping the number of articles in fails_dic dict
    fails_dic['number of article'] = len(corpus_df)

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

    articles_df[author_col] = articles_df[author_col].apply(treat_author)
    articles_df[year_col] = articles_df[year_col].apply(str_int_convertor)
    articles_df[doc_type_col] = articles_df[doc_type_col].apply(treat_doctype)
    articles_df[title_col] = articles_df[title_col].apply(treat_title)
    articles_df[issn_col] = articles_df[issn_col].apply(convert_issn)

    articles_df.insert(0, pub_id_col, list(corpus_df[pub_id_col]))
    return articles_df


def scopus_parser(rawdata_path, affil_filter_list=None, affil_params_dic=None):
    """Builds parsing data from the corpus rawdata.

    The list of the parsed items (keys of the returned dict which values are the dataframes \
    of the parsing results) is given by the PARSING_ITEMS_LIST global. 
    The rawdata are parsed using the following internal functions:
    - `_build_scopus_articles` which parses the articles' core data from the corpus rawdata
    - `_build_scopus_authors` which parses the authors' field of rawdata;
    - `_build_scopus_addresses_countries_affiliations` which parses the author-with-affilations \
    field of rawdata by publication;
    - `_build_scopus_authors_countries_affiliations` which parses the author-with-affilations \
    field of rawdata by authors;
    - `_build_scopus_keywords` which parses the authors' keywords and the indexed keywords fields \
    of rawdata and builds the title keywords from the publication title field of rawdata;
    - `build_scopus_subjects_and_sub_subjects` which parses the subjects and the secondary \
    subjects fields of rawdata.
    - `build_scopus_references` which parses the references field of rawdata by publication.

    Args:
        rawdata_path (path): The full path to the corpus rawdata.
        affil_filter_list (list): The affiliations-filter composed of a list of normalized affiliations (str), \
        optional (default=None).
        affil_params_dic (dict): Optional dict (default=None) keyed by ['affil_types_file_path', \
        'country_affils_file_path', 'country_towns_folder_path', 'country_towns_file'] and valued by the user as \
        the full path to the data per country of raw affiliations per normalized one, the full path to the data of \
        affiliations-types used to normalize the affiliations, the name of the file of the data of towns per country \
        and the full path to the folder where these data are available.
    Returns:
        (tup): (The parsed data (dataframes) as values of a dict keyed by parsing items, \
        The parsing success rate data (dict), The data (dataframe) of the corrected author names, \
        The data (dataframe) of the corrected addresses, The data (dataframe) of Scopus IDs of publications.
    """
    # Setting columns for scopus parsing process
    cols_tup = _set_scopus_parsing_cols()

    # Setting items list and values
    items_list = [bp_pg.PARSING_ITEMS_LIST[x] for x in range(12)]

    # Setting the specific file paths for subjects and sub-subjects assignment for Scopus corpuses
    scopus_cat_codes_path = Path(__file__).parent / Path(bp_gg.REP_UTILS) / Path(bp_pg.SCOPUS_CAT_CODES)
    scopus_journals_issn_cat_path = Path(__file__).parent / Path(bp_gg.REP_UTILS) / Path(bp_pg.SCOPUS_JOURNALS_ISSN_CAT)

    # Reading and checking the corpus file
    raw_data_return_tup = read_scopus_rawdata(rawdata_path, correct_data=True, scopus_ids=True)
    corpus_df, corrected_authors_df, corrected_addresses_df, scopus_ids_df = raw_data_return_tup

    # Initializing the dicts of dataframes resulting from the parsing
    scopus_parsing_dict, scopus_fails_dic = {}, {}
    empty_df = pd.DataFrame()
    for item in items_list:
        scopus_parsing_dict[item] = empty_df

    if not corpus_df.empty:

        # Building the dataframe of articles
        print("  - Publications main data parsing...", end="\r")
        articles_df = _build_scopus_articles(corpus_df, scopus_fails_dic, cols_tup)
        print("  - Publications main data parsed    ")

        # Building the dataframe of authors
        print("  - Authors parsing...", end="\r")
        authors_df = _build_scopus_authors(corpus_df, scopus_fails_dic, cols_tup)
        print("  - Authors parsed    ")

        # Building the dataframe of addresses, countries and affiliations
        print("  - Addresses, countries and affiliations parsing...", end="\r")
        addresses_tup = _build_scopus_addresses_countries_affiliations(corpus_df, scopus_fails_dic, cols_tup)
        addresses_df, countries_df, affiliations_df = addresses_tup
        print("  - Addresses, countries and affiliations parsed    ")

        # Building the dataframe of authors and their affiliations
        print("  - Authors with affiliations parsing...")
        auth_affils_df = _build_scopus_authors_countries_affiliations(corpus_df, scopus_fails_dic, cols_tup,
                                                                      affil_filter_list=affil_filter_list,
                                                                      affil_params_dic=affil_params_dic)
        print("  - Authors with affiliations parsed                     ")

        # Building the dataframes of keywords
        print("  - Authors' keywords, indexed keywords and title keywords parsing...", end="\r")
        authors_kw_df, index_kw_df, title_kw_df = _build_scopus_keywords(corpus_df, scopus_fails_dic, cols_tup)
        print("  - Authors' keywords, indexed keywords and title keywords parsed    ")

        # Building the dataframe of subjects and sub-subjects
        print("  - Subjects and secondary subjects parsing...", end="\r")
        subjects_df, sub_subjects_df = build_scopus_subjects_and_sub_subjects(corpus_df, scopus_cat_codes_path,
                                                                              scopus_journals_issn_cat_path,
                                                                              scopus_fails_dic, cols_tup)
        print("  - Subjects and secondary subjects parsed    ")

        # Building the dataframe of references
        print("  - References parsing...", end="\r")
        references_df = build_scopus_references(corpus_df, cols_tup)
        print("  - References parsed    ")

        # Building the scopus data dict
        scopus_parsing_list = [articles_df, authors_df, addresses_df, countries_df, affiliations_df, auth_affils_df,
                               authors_kw_df, index_kw_df, title_kw_df, subjects_df, sub_subjects_df, references_df]
        scopus_parsing_dict = dict(zip(items_list, scopus_parsing_list))

    return_tup = (scopus_parsing_dict, scopus_fails_dic, scopus_ids_df,
                  corrected_authors_df, corrected_addresses_df)
    return return_tup
