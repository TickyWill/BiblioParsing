"""Module of functions for parsing of WoS rawdata.
"""

__all__ = ['wos_parser',]


# Standard library imports
from collections import namedtuple

# 3rd party library imports
import pandas as pd

# Local library imports
import bpfuncts.affiliations_globals as bp_ag
import bpfuncts.parsing_cols_globals as bp_pcg
import bpfuncts.parsing_globals as bp_pg
import bpfuncts.regex_globals as bp_rg
from bpfuncts.affiliations_parsing import build_addr_affils_tup
from bpfuncts.affiliations_parsing import extend_author_affils
from bpfuncts.parsing_utils import build_item_df_from_tup
from bpfuncts.parsing_utils import build_title_keywords
from bpfuncts.parsing_utils import clean_authors_countries_affils
from bpfuncts.parsing_utils import normalize_country
from bpfuncts.parsing_utils import normalize_name
from bpfuncts.parsing_utils import set_shared_parsing_cols
from bpfuncts.parsing_utils import set_unknown_address
from bpfuncts.parsing_utils import standardize_address
from bpfuncts.parsing_utils import str_int_convertor
from bpfuncts.parsing_utils import treat_author
from bpfuncts.parsing_utils import treat_doctype
from bpfuncts.parsing_utils import treat_title
from bpfuncts.wos_rawdata_utils import read_wos_rawdata
from bpfuncts.wos_parsing_complements import build_wos_subjects_and_sub_subjects
from bpfuncts.wos_parsing_complements import build_wos_references


def _set_wos_parsing_cols():
    """Builds 3 dict setting columns list and selected columns names 
    for the process of parsing WoS rawdata.

    The shared columns info with other rawdata types are set through 
    the `set_shared_parsing_cols`function imported from the 
    `parsing_utils` module. 
    Globals are imported from the `parsing_cols_globals` module (imported as bp_pcg).

    Returns:
        (tup): (A dict valued by column-names lists for each parsing item \
        and temporary column names defined by the 'COL_NAMES' global, \
        A dict valued by column names of parsing results defined by the \
        'COL_NAMES' global, A dict valued by column names of rawdata defined \
        by the 'COLUMN_LABEL_WOS' and 'COLUMN_LABEL_WOS_PLUS' globals).
    """
    cols_lists_dic, cols_dic = set_shared_parsing_cols()

    cols_lists_dic['subject_cols_list'] = bp_pcg.COL_NAMES['subject']
    cols_lists_dic['sub_subject_cols_list'] = bp_pcg.COL_NAMES['sub_subject']

    cols_dic['wos_id_col'] = bp_pcg.COL_NAMES['wos_id'][0]

    wos_cols_dic = {'wos_auth_col'         : bp_pcg.COLUMN_LABEL_WOS['authors'],
                    'wos_title_kw_col'     : bp_pcg.COLUMN_LABEL_WOS['title'],
                    'wos_year_col'         : bp_pcg.COLUMN_LABEL_WOS['year'],
                    'wos_journal_col'      : bp_pcg.COLUMN_LABEL_WOS['journal'],
                    'wos_volume_col'       : bp_pcg.COLUMN_LABEL_WOS['volume'],
                    'wos_page_col'         : bp_pcg.COLUMN_LABEL_WOS['page_start'],
                    'wos_doi_col'          : bp_pcg.COLUMN_LABEL_WOS['doi'],
                    'wos_aff_col'          : bp_pcg.COLUMN_LABEL_WOS['affiliations'],
                    'wos_auth_with_aff_col': bp_pcg.COLUMN_LABEL_WOS['authors_with_affiliations'],
                    'wos_auth_kw_col'      : bp_pcg.COLUMN_LABEL_WOS['author_keywords'],
                    'wos_idx_kw_col'       : bp_pcg.COLUMN_LABEL_WOS['index_keywords'],
                    'wos_ref_col'          : bp_pcg.COLUMN_LABEL_WOS['references'],
                    'wos_issn_col'         : bp_pcg.COLUMN_LABEL_WOS['issn'],
                    'wos_language_col'     : bp_pcg.COLUMN_LABEL_WOS['language'],
                    'wos_doctype_col'      : bp_pcg.COLUMN_LABEL_WOS['document_type'],
                    'wos_fullnames_col'    : bp_pcg.COLUMN_LABEL_WOS['authors_fullnames'],
                    'wos_subjects_col'     : bp_pcg.COLUMN_LABEL_WOS['subjects'],
                    'wos_sub_subjects_col' : bp_pcg.COLUMN_LABEL_WOS['sub_subjects'],
                    'init_wos_id_col'      : bp_pcg.COLUMN_LABEL_WOS_PLUS['wos_id'],
                   }

    return cols_lists_dic, cols_dic, wos_cols_dic


def _check_authors_list(authors_str, affiliations_str):
    # Building the full list of ordered authors full names
    authors_ordered_list = authors_str.split("; ")
    authors_ordered_list = [author.strip() for author in authors_ordered_list]

    # Building the list of authors full names in authors-with-affiliation
    affil_authors_list = [[x.strip() for x in authors.split(';')]
                    for authors in bp_rg.RE_AUTHOR.findall(affiliations_str)]
    flat_authors_set  = set(sum(affil_authors_list, []))

    # Building the list of authors out of authors-with-affiliation
    out_authors_list = list(set(authors_ordered_list) - flat_authors_set)
    return authors_ordered_list, affil_authors_list, out_authors_list


def _set_upper_initials(author):
    names_list = author.split(" ")
    names_list[-1] = names_list[-1].upper()
    new_author = " ".join(names_list)
    return new_author


def _build_wos_authors(corpus_df, fails_dic, cols_tup):
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
            author = normalize_name(wos_auth, drop_ponct=True)
            author = _set_upper_initials(author)
            if author not in ['Dr','Pr','Dr ','Pr ']:
                authors_list.append(co_author(pub_id, author_idx, author))
                author_idx += 1

    # Building a clean co-authors dataframe and accordingly updating the parsing success rate dict
    co_authors_df, fails_dic = build_item_df_from_tup(authors_list, auth_cols_list,
                                                      co_authors_col, pub_id_col, fails_dic)
    return co_authors_df


def _build_wos_keywords(corpus_df, fails_dic, cols_tup):
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
        (tup): The 3 built data (dataframes).
    """
    # Setting useful column names
    cols_lists_dic, cols_dic, wos_cols_dic = cols_tup
    kw_cols_list = cols_lists_dic['kw_cols_list']
    cols_keys = ['pub_id_col', 'keyword_col', 'title_temp_col', 'kept_tokens_col']
    (pub_id_col, keyword_col, title_temp_col, kept_tokens_col) = [cols_dic[key] for key in cols_keys]
    wos_cols_keys = ['wos_auth_kw_col', 'wos_idx_kw_col', 'wos_title_kw_col']
    (wos_auth_kw_col, wos_idx_kw_col,
     wos_title_kw_col )= [wos_cols_dic[key] for key in wos_cols_keys]

    # Setting named tuple
    key_word = namedtuple('key_word', kw_cols_list)

    aks_list = []
    aks_df = corpus_df[wos_auth_kw_col].fillna('')
    for pub_id, pub_aks_str in zip(corpus_df[pub_id_col], aks_df):
        pub_aks_list = pub_aks_str.split(';')
        for pub_ak in pub_aks_list:
            pub_ak = pub_ak.lower().strip()
            aks_list.append(key_word(pub_id,
                                     pub_ak if pub_ak!='null' else bp_pg.UNKNOWN))
    iks_list = []
    iks_df = corpus_df[wos_idx_kw_col].fillna('')
    for pub_id, pub_iks_str in zip(corpus_df[pub_id_col], iks_df):
        pub_iks_list = pub_iks_str.split(';')
        for pub_ik in pub_iks_list:
            pub_ik = pub_ik.lower().strip()
            iks_list.append(key_word(pub_id, pub_ik if pub_ik!='null' else bp_pg.UNKNOWN))

    tks_list = []
    title_df = pd.DataFrame(corpus_df[wos_title_kw_col].fillna(''))
    title_df.columns = [title_temp_col]
    tks_df, _ = build_title_keywords(title_df)
    for pub_id in corpus_df[pub_id_col]:
        for token in tks_df.loc[pub_id, kept_tokens_col]:
            token = token.lower().strip()
            tks_list.append(key_word(pub_id, token if token!='null' else bp_pg.UNKNOWN))

    # Building a clean author keywords data and accordingly updating the parsing success rate dict
    ak_kw_df, fails_dic = build_item_df_from_tup(aks_list, kw_cols_list,
                                                 keyword_col, pub_id_col, fails_dic)

    # Building a clean index keywords data and accordingly updating the parsing success rate dict
    ik_kw_df, fails_dic = build_item_df_from_tup(iks_list, kw_cols_list,
                                                 keyword_col, pub_id_col, fails_dic)

    # Building a clean title keywords data and accordingly updating the parsing success rate dict
    tk_kw_df, fails_dic = build_item_df_from_tup(tks_list, kw_cols_list,
                                                 keyword_col, pub_id_col, fails_dic)

    return ak_kw_df, ik_kw_df, tk_kw_df


def _build_wos_addresses_countries_affiliations(corpus_df, fails_dic, cols_tup):
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
        - for the addresses data, the column names are given by 'address_cols_list':

             Pub-index  Address-index                     Address
                 0         0            Tech Univ Carolo Wilhelmina Braunschweig, ...
                 0         1            Univ Inst Lisbon, Inst Telecomunicac, Lisbon, Portugal
                 0         2            Univ Grenoble Alpes, CEA, Leti, Grenoble, France

        - for the countries data, the column names are given by 'country_cols_list':

             Pub-index  Address-index       Country
                 0         0            Germany
                 0         1            Portugal
                 0         2            France

        - for the main affiliations data, the column names are given by 'affil_cols_list':

             Pub-index  Address-index        Main affiliation
                 0         0            Tech University Carolo Wilhelmina Braunschweig
                 0         1            University Institute Lisbon
                 0         2            University Grenoble Alpes

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
    cols_lists_keys = ['address_cols_list', 'country_cols_list', 'affil_cols_list']
    address_cols_list, country_cols_list, affil_cols_list = [cols_lists_dic[key] for key in cols_lists_keys]
    cols_keys = ['pub_id_col', 'address_col', 'country_col', 'affil_col']
    (pub_id_col, address_col, country_col, affil_col) = [cols_dic[key] for key in cols_keys]
    wos_cols_keys = ['wos_auth_with_aff_col', 'wos_fullnames_col']
    (wos_auth_with_aff_col, wos_fullnames_col) = [wos_cols_dic[key] for key in wos_cols_keys]

    # Setting named tuples
    address_tup = namedtuple('address', address_cols_list)
    country_tup = namedtuple('country', country_cols_list)
    affiliation_tup = namedtuple('affiliation', affil_cols_list)

    corpus_series_zip = zip(corpus_df[pub_id_col],
                            corpus_df[wos_fullnames_col],
                            corpus_df[wos_auth_with_aff_col])

    addresses_list, countries_list, affiliations_list = [], [], []
    for pub_id, authors_str, affiliations_str in corpus_series_zip:
        if '[' in affiliations_str:
            # Format case: '[Author1] address1; [Author1, Author2] address2...'
            # authors = bp_rg.RE_AUTHOR.findall(affiliations_str) # for future use
            pub_addresses_list = bp_rg.RE_ADDRESS.findall(affiliations_str)

            # Checking authors in authors list and authors-with-affiliation data
            authors_ordered_list, _, out_authors_list = _check_authors_list(authors_str, affiliations_str)
            for out_author in out_authors_list:
                out_author_idx = authors_ordered_list.index(out_author)
                out_author_address = set_unknown_address(out_author_idx, add_unknown_country=False)
                pub_addresses_list.append(out_author_address)
        else:
            # Format case: 'address1;address2...'
            pub_addresses_list = affiliations_str.split(';')

        if pub_addresses_list:
            for address_idx, pub_raw_address in enumerate(pub_addresses_list):
                pub_address = standardize_address(pub_raw_address, add_unknown_country=False)
                addresses_list.append(address_tup(pub_id, address_idx, pub_address))

                main_affiliation = pub_address.split(',')[0]
                affiliations_list.append(affiliation_tup(pub_id, address_idx, main_affiliation))

                country_raw = pub_address.split(',')[-1].replace(';','').strip()
                country = normalize_country(country_raw)
                countries_list.append(country_tup(pub_id, address_idx, country))
        else:
            addresses_list.append(address_tup(pub_id, 0, ''))
            affiliations_list.append(affiliation_tup(pub_id, 0, ''))
            countries_list.append(country_tup(pub_id, 0, ''))

    # Building a clean addresses data and accordingly updating the parsing success rate dict
    addresses_df, fails_dic = build_item_df_from_tup(addresses_list, address_cols_list,
                                                     address_col, pub_id_col, fails_dic)

    # Building a clean countries data and accordingly updating the parsing success rate dict
    countries_df, fails_dic = build_item_df_from_tup(countries_list, country_cols_list,
                                                     country_col, pub_id_col, fails_dic)

    # Building a clean affiliations data and accordingly updating the parsing success rate dict
    affiliations_df, fails_dic = build_item_df_from_tup(affiliations_list, affil_cols_list,
                                                        affil_col, pub_id_col, fails_dic)

    if not len(addresses_df)==len(countries_df)==len(affiliations_df):
        warning = ('WARNING: Lengths of "addresses_df", "countries_df" and "affiliations_df" dataframes are not equal'
                   'in "_build_wos_addresses_countries_affiliations" function of "wos_parsing.py" module')
        print(warning)

    return addresses_df, countries_df, affiliations_df


def _build_wos_authors_countries_affiliations(corpus_df, fails_dic, cols_tup,
                                              affil_filter_list=None, affil_params_dic=None):
    """Parses the field of authors with affiliations of the corpus data to build the data of authors 
    with their addresses, country and normalized affiliations per publication of the corpus.

    The parsing success rate data are updated. 
    In addition, the built data may be expanded according to a filtering of affiliations. 
    The parsing is effective only for the format of the following example. Otherwise, the parsing 
    fields are set to UNKNOWN global.

    For example, the 'Authors with affiliations' field string:
       '[Boujjat, Houssame] CEA, LITEN Solar & Thermod Syst Lab L2ST, F-38054 Grenoble, France;
        [Boujjat, Houssame] Univ Grenoble Alpes, F-38000 Grenoble, France;
        [Rodat, Sylvain; Chuayboon, Srirat] CNRS, Proc Mat & Solar Energy Lab,
        PROMES, 7 Rue Four Solaire, F-66120 Font Romeu, France;
        [Abanades, Stephane] CEA, Leti, 17 rue des Martyrs, F-38054 Grenoble, France;
        [Dupont, Sylvain] CEA, Liten, INES. 50 avenue du Lac, F-73370 Le Bourget-du-Lac, France;
        [Durand, Maurice] CEA, INES, DTS, 50 avenue du Lac, F-73370 Le Bourget-du-Lac, France;
        [David, David] Lund University, Department of Phys Geography and Ecosystem Science (INES), Lund, Sweden'

    will be parsed in the "auth_affils_df" dataframe if affiliation filter is not defined (initialization step):

        Pub_id Idx_author                    Address        Country Norm_affiliations           Raw_affiliations
            0      0       CEA, LITEN Solar & Thermod , ... France  CEA Nro;LITEN Rto           F-38054 Grenoble
            0      0       Univ Grenoble Alpes,...          France  UGA Univ                    F-38000 Grenoble
            0      1       CNRS, Proc Mat Lab, PROMES,...   France  CNRS Nro;PROMES CNRS-Lab    7 Rue Four Solaire;...
            0      2       CNRS, Proc Mat Lab, PROMES, ...  France  CNRS Nro;PROMES CNRS-Lab    7 Rue Four Solaire;...
            0      3       CEA, Leti, 17 rue des Martyrs,...France  CEA Nro;LETI Rto            17 rue des Martyrs;...
            0      4       CEA, Liten, INES. 50 avenue...   France  CEA Nro;LITEN Rto;INES Site 50 avenue du Lac;...
            0      5       CEA, INES, DTS, 50 avenue...     France  CEA Nro;INES Site           DTS;...
            0      6       Lund University,...(INES),...    Sweden  Lund Univ                   Department of Phys...

    the authors' identifiers are defined using the ordered list of the authors given by the corpus data.
    The affiliations are identified and normalized using dedicated data that should be specified by the user.

    If affiliation filter is defined based on the following list of normalized affiliations:
        affil_filter_list = ['LITEN Rto', 'INES Campus', 'PROMES CNRS-Lab'), 'Lund Univ'].

    The "auth_affils_df" dataframe will be expended with the following columns (for pub_id = 0):
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
        affil_filter_list (list): The affiliations-filter composed of a list of normalized affiliations (str), \
        optional (default=None).
        affil_params_dic (dict); Optional dict (default=None) keyed by ['affil_types_file_path', \
        'country_affils_file_path', 'country_towns_folder_path', 'country_towns_file'] and valued by the user as \
        the full path to the data per country of raw affiliations per normalized one, the full path to the data of \
        affiliations-types used to normalize the affiliations, the name of the file of the data of towns per country \
        and the full path to the folder where these are available.
    Returns:
        (dataframe): The built data.
    """
    # Setting useful column names
    cols_lists_dic, cols_dic, wos_cols_dic = cols_tup
    auth_affil_cols_list = cols_lists_dic['auth_affil_cols_list']
    cols_keys = ['pub_id_col', 'affil_author_idx_col', 'norm_affils_col']
    (pub_id_col, author_idx_col, norm_affils_col) = [cols_dic[key] for key in cols_keys]
    wos_cols_keys = ['wos_auth_with_aff_col', 'wos_fullnames_col']
    (wos_auth_with_aff_col, wos_fullnames_col) = [wos_cols_dic[key] for key in wos_cols_keys]

    # Setting namedtuples
    addr_country_affils = namedtuple('address', auth_affil_cols_list[:-1] )
    author_address_tup = namedtuple('author_address', 'author address')

    # Building the "addr_country_affils_list" list
    # with one item per publication and per author identifier
    corpus_series_zip = zip(corpus_df[pub_id_col],
                            corpus_df[wos_fullnames_col],
                            corpus_df[wos_auth_with_aff_col])
    pub_nb = len(corpus_df[pub_id_col])
    pub_num = 0
    addr_country_affils_list = []
    for pub_id, authors_str, affiliations_str in corpus_series_zip:
        pub_num += 1
        print("    Publications number:", pub_num, f"/ {pub_nb}", end="\r")
        if '[' in affiliations_str:
            # Proceeding if the field author is present in affiliations.

            # Checking authors in authors list and authors-with-affiliation data
            (authors_ordered_list, affil_authors_list,
             out_authors_list) = _check_authors_list(authors_str, affiliations_str)

            # Building the list of tuples [([Author1, Author2,...], address1),...]
            # from the author-with-affiliations field in the corpus data
            affiliations_list = [x.strip() for x in bp_rg.RE_ADDRESS.findall(affiliations_str)]
            affiliations_list = affiliations_list if affiliations_list else ['']
            tuples_list = tuple(zip(affil_authors_list, affiliations_list))

            # Builds the list of tuples [(author<0>, address<0>),(author<0>, address<1>),...,(author<i>, address<j>)...]
            author_address_tup_list = [author_address_tup(y, x[1]) for x in tuples_list for y in x[0]]

            for tup in author_address_tup_list:
                if tup.author in authors_ordered_list:
                    author_idx = authors_ordered_list.index(tup.author)

                    author_country_raw = tup.address.split(',')[-1].replace(';','').strip()
                    author_country = normalize_country(author_country_raw)

                    author_raw_address = tup.address
                    author_std_address = standardize_address(author_raw_address)

                    author_affiliations_tup = build_addr_affils_tup(author_std_address, affil_params_dic,
                                                                    drop_status=False)
                    addr_country_affils_list.append(addr_country_affils(pub_id, author_idx,
                                                                        author_std_address, author_country,
                                                                        author_affiliations_tup.norm_affils_list,
                                                                        author_affiliations_tup.raw_affils_list,))
            if out_authors_list:
                for out_author in out_authors_list:
                    out_author_idx = authors_ordered_list.index(out_author)
                    out_author_address = set_unknown_address(out_author_idx, add_unknown_country=True)
                    addr_country_affils_list.append(addr_country_affils(pub_id, out_author_idx,
                                                                        out_author_address, bp_pg.UNKNOWN_COUNTRY,
                                                                        bp_ag.EMPTY, bp_ag.EMPTY,))
        else:
            # If the field author is not present in affiliations complete namedtuple with the global UNKNOWN
            addr_country_affils_list.append(addr_country_affils(pub_id, bp_pg.UNKNOWN, bp_pg.UNKNOWN,
                                                                bp_pg.UNKNOWN, bp_pg.UNKNOWN, bp_pg.UNKNOWN,))
    # Building a clean authors-countries-affiliations data and accordingly updating the parsing success rate dict
    auth_affils_df, fails_dic = build_item_df_from_tup(addr_country_affils_list, auth_affil_cols_list[:-1],
                                                       norm_affils_col, pub_id_col, fails_dic)
    auth_affils_df = clean_authors_countries_affils(auth_affils_df)

    if affil_filter_list is not None:
        auth_affils_df = extend_author_affils(auth_affils_df, affil_filter_list)

    # Sorting the values in the built data by two columns
    auth_affils_df.sort_values(by=[pub_id_col, author_idx_col], inplace=True)

    return auth_affils_df


def _build_wos_articles(corpus_df, fails_dic, cols_tup):
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
    # Keeping the number of articles in fails_dic dict
    fails_dic['number of article'] = len(corpus_df)

    # Setting useful column names
    cols_lists_dic, cols_dic, wos_cols_dic = cols_tup
    articles_cols_list = cols_lists_dic['articles_cols_list']
    cols_keys = ['pub_id_col', 'author_col', 'year_col', 'doc_type_col',
                 'title_col', 'norm_journal_col']
    (pub_id_col, author_col, year_col, doc_type_col, title_col,
     norm_journal_col) = [cols_dic[key] for key in cols_keys]

    wos_cols_keys = ['wos_auth_col', 'wos_year_col', 'wos_journal_col',
                     'wos_volume_col', 'wos_page_col', 'wos_doi_col',
                     'wos_doctype_col', 'wos_language_col', 'wos_title_kw_col',
                     'wos_issn_col']
    wos_cols_list = [wos_cols_dic[key] for key in wos_cols_keys]

    articles_wos_cols = wos_cols_list + [norm_journal_col]
    articles_df = corpus_df[articles_wos_cols].astype(str)
    articles_df.rename(columns=dict(zip(articles_wos_cols, articles_cols_list[1:])),
                       inplace=True)

    articles_df[author_col] = articles_df[author_col].apply(treat_author)
    articles_df[year_col] = articles_df[year_col].apply(str_int_convertor)
    articles_df[doc_type_col] = articles_df[doc_type_col].apply(treat_doctype)
    articles_df[title_col] = articles_df[title_col].apply(treat_title)

    articles_df.insert(0, pub_id_col, list(corpus_df[pub_id_col]))
    return articles_df


def wos_parser(rawdata_path, parsing_items_list, affil_filter_list=None, affil_params_dic=None):
    """Builds parsing data from the corpus rawdata.

    The rawdata are parsed using the following internal functions:
    - `_build_wos_articles` which parses the articles' core data from the corpus rawdata
    - `_build_wos_authors` which parses the authors' field of rawdata;
    - `_build_wos_addresses_countries_affiliations` which parses the author-with-affilations \
    field of rawdata by publication;
    - `_build_wos_authors_countries_affiliations` which parses the author-with-affilations \
    field of rawdata by authors;
    - `_build_wos_keywords` which parses the authors' keywords and the indexed keywords fields \
    of rawdata and builds the title keywords from the publication title field of rawdata;
    - `build_wos_subjects_and_sub_subjects` which parses the subjects and the secondary \
    subjects fields of rawdata.
    - `build_wos_references` which parses the references field of rawdata by publication.

    Args:
        rawdata_path (path): The full path to the corpus rawdata.
        parsing_items_list (list): The parsed items (keys of the returned dict which values are the dataframes \
        of the parsing results)
        affil_filter_list (list): The affiliations-filter composed of a list of normalized affiliations (str), \
        optional (default=None).
        affil_params_dic (dict); Optional dict (default=None) keyed by ['affil_types_file_path', \
        'country_affils_file_path', 'country_towns_folder_path', 'country_towns_file'] and valued by the user as \
        the full path to the data per country of raw affiliations per normalized one, the full path to the data of \
        affiliations-types used to normalize the affiliations, the name of the file of the data of towns per country \
        and the full path to the folder where these are available.
    Returns:
        (tup): (The parsed data (dataframes) as values of a dict keyed by parsing items, \
        The parsing success rate data (dict), The data (dataframe) of WoS IDs of publications.
    """
    # Setting columns for wos parsing process
    cols_tup = _set_wos_parsing_cols()

    # Setting items list and values
    items_list = [parsing_items_list[x] for x in range(12)]

    # Reading and checking the raw corpus file
    corpus_df, wos_ids_df = read_wos_rawdata(rawdata_path, wos_ids=True)

    # Initializing the dicts of data resulting from the parsing
    wos_parsing_dict, wos_fails_dic = {}, {}
    empty_df = pd.DataFrame()
    for item in items_list:
        wos_parsing_dict[item] = empty_df

    if not corpus_df.empty:

        # Building the data of articles
        print("  - Publications main data parsing...", end="\r")
        articles_df = _build_wos_articles(corpus_df, wos_fails_dic, cols_tup)
        print("  - Publications main data parsed    ")

        # Building the data of authors
        print("  - Authors parsing...", end="\r")
        authors_df = _build_wos_authors(corpus_df, wos_fails_dic, cols_tup)
        print("  - Authors parsed    ")

        # Building the data of addresses, countries and affiliations
        print("  - Addresses, countries and affiliations parsing...", end="\r")
        addresses_tup = _build_wos_addresses_countries_affiliations(corpus_df, wos_fails_dic, cols_tup)
        addresses_df, countries_df, affiliations_df = addresses_tup
        print("  - Addresses, countries and affiliations parsed    ")

        # Building the data of authors and their affiliations
        print("  - Authors with affiliations parsing...")
        auth_affil_df = _build_wos_authors_countries_affiliations(corpus_df, wos_fails_dic, cols_tup,
                                                                  affil_filter_list=affil_filter_list,
                                                                  affil_params_dic=affil_params_dic)
        print("  - Authors with affiliations parsed                     ")

        # Building the data of keywords
        print("  - Authors' keywords, indexed keywords and title keywords parsing...", end="\r")
        authors_kw_df, index_kw_df, title_kw_df = _build_wos_keywords(corpus_df, wos_fails_dic, cols_tup)
        print("  - Authors' keywords, indexed keywords and title keywords parsed    ")

        # Building the data of subjects and secondary subjects
        print("  - Subjects and secondary subjects parsing...", end="\r")
        subjects_df, sub_subjects_df = build_wos_subjects_and_sub_subjects(corpus_df, wos_fails_dic, cols_tup)
        print("  - Subjects and secondary subjects parsed    ")

        # Building the data of references
        print("  - References parsing...", end="\r")
        references_df = build_wos_references(corpus_df, cols_tup)
        print("  - References parsed    ")

        # Building the wos data dict
        wos_parsing_list = [articles_df, authors_df, addresses_df, countries_df, affiliations_df, auth_affil_df,
                            authors_kw_df, index_kw_df, title_kw_df, subjects_df, sub_subjects_df, references_df]
        wos_parsing_dict = dict(zip(items_list, wos_parsing_list))
    return wos_parsing_dict, wos_fails_dic, wos_ids_df
