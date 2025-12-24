__all__ = ['biblio_parser_scopus','read_database_scopus']


def _build_authors_scopus(corpus_df, dic_failed):
    
    '''Builds the dataframe "df_co_authors" of the co-authors of the article
    referenced with the key publi_id:
    
               pub_id  idx_author   co-author
          0        0      0          Boujjat H.
          1        0      1          Rodat S.

    Args:
        corpus_df (dataframe): the dataframe of the wos/scopus corpus

    Returns:
        The dataframe df_country
    '''

    # Standard library imports
    from collections import namedtuple
    
    # 3rd party library imports
    import pandas as pd
    
    # Local libray imports
    from BiblioParsing.BiblioParsingUtils import build_item_df_from_tup
    from BiblioParsing.BiblioParsingUtils import normalize_name
    
    # Globals imports
    from BiblioParsing.BiblioSpecificGlobals import COL_NAMES
    from BiblioParsing.BiblioSpecificGlobals import COLUMN_LABEL_SCOPUS
    
    # Setting useful aliases
    pub_id_alias        = COL_NAMES['pub_id']
    auth_col_list_alias = COL_NAMES['authors']
    co_authors_alias    = auth_col_list_alias[2]
    scopus_auth_alias   = COLUMN_LABEL_SCOPUS['authors']
    
    # Setting named tuple
    co_author = namedtuple('co_author', auth_col_list_alias)
    
    list_author = []
    for pub_id,x in zip(corpus_df[pub_id_alias],
                        corpus_df[scopus_auth_alias]):
        idx_author = 0
        authors_sep = ',' 
        if ';' in x: authors_sep = ';'             # Change in scopus on 07/2023
        for y in x.split(authors_sep):
            author = normalize_name(y.replace('.',''))
            
            if author not in ['Dr','Pr','Dr ','Pr ']:
                list_author.append(co_author(pub_id,
                                             idx_author,
                                             author))
                idx_author += 1

    # Building a clean co-authors dataframe and accordingly updating the parsing success rate dict
    df_co_authors, dic_failed = build_item_df_from_tup(list_author, auth_col_list_alias, 
                                                       co_authors_alias, pub_id_alias, dic_failed)
    
    return df_co_authors


def _build_keywords_scopus(corpus_df, dic_failed):
    
    '''Builds the dataframe "df_keyword" with three columns:
                pub_id  type  keyword
            0     0      AK    Biomass
            1     0      AK    Gasification
            2     0      AK    Solar energy
    with: 
         type = AK for author keywords 
         type = IK for journal keywords
         type = TK for title keywords 
         
    The author keywords are extracted from the scopus column 'Author Keywords' 
    The journal keywords are extracted from the scopus column 'Index Keywords' 
    The title keywords are builds out of the set TK_corpus of the most cited nouns 
      (at leat N times) in the set of all the articles. The keywords of type TK of an
      article, referenced by the key pub_id, are the elements of the intersection
      between the set TK_corpus and the set of the nouns of the article title.
    
        
    Args:
        corpus_df (dataframe): the dataframe of the wos/scopus corpus

    Returns:
        The dataframe df_keyword
    '''
    # To Do: Check the use of UNKNOWN versus '"null"'
    
    # Standard library imports
    from collections import namedtuple
    from collections import Counter
    from operator import attrgetter
    
    # 3rd party library imports
    import pandas as pd
    
    # Local libary imports
    from BiblioParsing.BiblioParsingUtils import build_item_df_from_tup
    from BiblioParsing.BiblioParsingUtils import build_title_keywords
    
    # Globals imports
    from BiblioParsing.BiblioSpecificGlobals import COL_NAMES
    from BiblioParsing.BiblioSpecificGlobals import COLUMN_LABEL_SCOPUS
    from BiblioParsing.BiblioSpecificGlobals import UNKNOWN
    
    # Setting useful aliases
    pub_id_alias          = COL_NAMES['pub_id']
    kw_col_List_alias     = COL_NAMES['keywords']
    tmp_col_list_alias    = COL_NAMES['temp_col']
    keyword_alias         = kw_col_List_alias[1]
    title_alias           = tmp_col_list_alias[2]
    kept_tokens_alias     = tmp_col_list_alias[4]
    scopus_auth_kw_alias  = COLUMN_LABEL_SCOPUS['author_keywords']
    scopus_idx_kw_alias   = COLUMN_LABEL_SCOPUS['index_keywords']
    scopus_title_kw_alias = COLUMN_LABEL_SCOPUS['title']
    
    # Setting named tuple
    key_word = namedtuple('key_word', kw_col_List_alias)    
    
    
    list_keyword_AK = []
    df_AK = corpus_df[scopus_auth_kw_alias].fillna('')
    for pub_id,keywords_AK in zip(corpus_df[pub_id_alias],df_AK):
        list_keywords_AK = keywords_AK.split(';')      
        for keyword_AK in list_keywords_AK:
            keyword_AK = keyword_AK.strip()
            list_keyword_AK.append(key_word(pub_id,
                                   keyword_AK if keyword_AK != 'null' else '”null”'))

    list_keyword_IK = []
    df_IK = corpus_df[scopus_idx_kw_alias].fillna('')
    for pub_id,keywords_IK in zip(corpus_df[pub_id_alias],df_IK):
        list_keywords_IK = keywords_IK.split(';')
        for keyword_IK in list_keywords_IK:
            keyword_IK = keyword_IK.strip()
            if keyword_IK == 'null': keyword_IK = UNKNOWN # replace 'null' by the keyword UNKNOWN
            list_keyword_IK.append(key_word(pub_id,
                                            keyword_IK if keyword_IK != 'null' else '”null”'))
            
    list_keyword_TK = []
    df_title = pd.DataFrame(corpus_df[scopus_title_kw_alias].fillna(''))
    df_title.columns = [title_alias]  # To be coherent with the convention of function build_title_keywords 
    df_TK,list_of_words_occurrences = build_title_keywords(df_title)
    for pub_id in corpus_df[pub_id_alias]:
        for token in df_TK.loc[pub_id,kept_tokens_alias]:
            token = token.strip()
            list_keyword_TK.append(key_word(pub_id,
                                         token if token != 'null' else '”null”'))
    
    # Building a clean author keywords dataframe and accordingly updating the parsing success rate dict
    df_keyword_AK, dic_failed = build_item_df_from_tup(list_keyword_AK, kw_col_List_alias, 
                                                       keyword_alias, pub_id_alias, dic_failed)
    
    # Building a clean index keywords dataframe and accordingly updating the parsing success rate dict
    df_keyword_IK, dic_failed = build_item_df_from_tup(list_keyword_IK, kw_col_List_alias, 
                                                       keyword_alias, pub_id_alias, dic_failed) 
    
    # Building a clean title keywords dataframe and accordingly updating the parsing success rate dict
    df_keyword_TK, dic_failed = build_item_df_from_tup(list_keyword_TK, kw_col_List_alias, 
                                                       keyword_alias, pub_id_alias, dic_failed)     
    
    return df_keyword_AK,df_keyword_IK, df_keyword_TK


def _build_addresses_countries_institutions_scopus(corpus_df, dic_failed):
    
    '''Builds the dataframe "df_address" from the column "Affiliations" of the scopus corpus:
    
            pub_id  idx_address  address
              0         0         CEA-LITEN Solar and ...
              0         1         Univ. Grenoble Alpes, Grenoble, F-38000, France
              0         2         Processes, Materials and Solar Energy Laboratory,...
        
    where: idx_address is the rank of the address in the list of affiliations
    of the article referenced with the key pub_id.
    We use the column 'Affiliations' formated as :
    
    'CEA-LITEN Solar and Thermodynamic Systems Laboratory (L2ST), Grenoble, F-38054, France;
    Univ. Grenoble Alpes, Grenoble, F-38000, France;
    Processes, Materials and Solar Energy Laboratory, PROMES-CNRS, 
    7 Rue du Four Solaire, Font-Romeu, 66120, France'
    
    Args:
        corpus_df (dataframe): the dataframe of the wos/scopus corpus


    Returns:
        The dataframes df_address, df_country, df_institution.
        
    Notes:
        The globals 'COL_NAMES', 'COLUMN_LABEL_SCOPUS', 'RE_SUB', 'RE_SUB_FIRST' and 'UNKNOWN_COUNTRY' 
        are used from `BiblioSpecificGlobals` module of `BiblioParsing` package.
        The functions `remove_special_symbol` and `normalize_country` are used 
        from `BiblioParsingUtils` of `BiblioAnalysis_utils` package.
         
    '''
    
    # Standard library imports
    import re
    from collections import namedtuple
    
    # 3rd party library imports
    import pandas as pd
    
    # Local library imports
    from BiblioParsing.BiblioParsingUtils import build_item_df_from_tup
    from BiblioParsing.BiblioParsingUtils import normalize_country
    from BiblioParsing.BiblioParsingUtils import remove_special_symbol
    
    # Globals imports
    from BiblioParsing.BiblioRegexpGlobals import RE_SUB
    from BiblioParsing.BiblioRegexpGlobals import RE_SUB_FIRST
    from BiblioParsing.BiblioSpecificGlobals import COL_NAMES
    from BiblioParsing.BiblioSpecificGlobals import COLUMN_LABEL_SCOPUS
    from BiblioParsing.BiblioSpecificGlobals import UNKNOWN_COUNTRY

    # Setting useful aliases
    pub_id_alias           = COL_NAMES['pub_id']
    address_col_List_alias = COL_NAMES['address']
    country_col_list_alias = COL_NAMES['country']
    inst_col_list_alias    = COL_NAMES['institution']
    address_alias          = address_col_List_alias[2]
    country_alias          = country_col_list_alias[2]
    institution_alias      = inst_col_list_alias[2]
    scopus_aff_alias       = COLUMN_LABEL_SCOPUS['affiliations']
    
    # Setting named tuples
    address         = namedtuple('address', address_col_List_alias)
    ref_country     = namedtuple('country', country_col_list_alias)
    ref_institution = namedtuple('ref_institution', inst_col_list_alias)
        
    list_addresses    = []
    list_countries    = []
    list_institutions = []       
    for pub_id, affiliation in zip(corpus_df[pub_id_alias],
                                   corpus_df[scopus_aff_alias]):
        list_affiliation = affiliation.split(';')
        
        if list_affiliation:
            for idx_address, address_pub in enumerate(list_affiliation):
                address_pub = remove_special_symbol(address_pub, only_ascii=True, strip=True)
                list_addresses.append(address(pub_id,
                                              idx_address,
                                              address_pub))

                addresses_split = address_pub.split(',')
                inst_nb = len(addresses_split)
                inst_num = 0
                institution = addresses_split[inst_num]                    
                if not institution and inst_nb:
                    while not institution and inst_num < inst_nb:
                        inst_num += 1
                        institution = address_pub.split(',')[inst_num]
                institution = re.sub(RE_SUB_FIRST,'University' + ', ',institution)
                institution = re.sub(RE_SUB,'University'+' ', institution)
                list_institutions.append(ref_institution(pub_id,
                                                         idx_address,
                                                         institution))
                
                country_raw = address_pub.split(',')[-1].replace(';','').strip()  
                country     = normalize_country(country_raw)
                if country == '':
                    country = UNKNOWN_COUNTRY
                    warning = (f'WARNING: the invalid country name "{country_raw}" '
                               f'in pub_id {pub_id} has been replaced by "{UNKNOWN_COUNTRY}" '
                               f'in "_build_addresses_countries_institutions_scopus" '
                               f'function of "BiblioParsingScopus.py" module.')
                    print(warning)

                list_countries.append(ref_country(pub_id,
                                                  idx_address,
                                                  country))
        else:
            list_addresses.append(address(pub_id, 0, ''))
            list_institutions.append(ref_institution(pub_id, 0, ''))
            list_countries.append(country(pub_id, 0, ''))

    # Building a clean addresses dataframe and accordingly updating the parsing success rate dict
    df_address, dic_failed = build_item_df_from_tup(list_addresses, address_col_List_alias, 
                                                    address_alias, pub_id_alias, dic_failed)

    # Building a clean countries dataframe and accordingly updating the parsing success rate dict
    df_country, dic_failed = build_item_df_from_tup(list_countries, country_col_list_alias, 
                                                    country_alias, pub_id_alias, dic_failed)
    
    # Building a clean institutions dataframe and accordingly updating the parsing success rate dict
    df_institution, dic_failed = build_item_df_from_tup(list_institutions, inst_col_list_alias, 
                                                        institution_alias, pub_id_alias, dic_failed)
    
    if not(len(df_address) == len(df_country) == len(df_institution)):
        warning = (f'\nWARNING: Lengths of "df_address", "df_country" and "df_institution" dataframes are not equal '
                   f'in "_build_addresses_countries_institutions_scopus" function of "BiblioParsingScopus.py" module')
        print(warning)
    
    return df_address, df_country, df_institution


def _build_authors_countries_institutions_scopus(corpus_df, dic_failed, inst_filter_list = None,
                                                 country_affiliations_file_path = None,
                                                 inst_types_file_path = None,
                                                 country_towns_file = None,
                                                 country_towns_folder_path = None):
    """The `_build_authors_countries_institutions_scopus' function parses the fields 'Affiliations' 
       and 'Authors with affiliations' of a scopus database to retrieve the article authors 
       with their addresses, affiliations and country. 
       In addition, a secondary affiliations list may be added according to a filtering of affiliations.
       
       The parsing is effective only for the format of the following example.
       Otherwise, the parsing fields are set to empty strings.
       
       For example, the 'Authors with affiliations' field string:

       'Boujjat, H., CEA, LITEN Solar & Thermodynam Syst Lab L2ST, F-38054 Grenoble, France,
        Univ Grenoble Alpes, F-38000 Grenoble, France; 
        Rodat, S., CNRS, Proc Mat & Solar Energy Lab, PROMES, 7 Rue Four Solaire, F-66120 Font Romeu, France;
        Chuayboon, S., CNRS, Proc Mat & Solar Energy Lab, PROMES, 7 Rue Four Solaire, F-66120 Font Romeu, France;
        Abanades, S., CEA, Leti, 17 rue des Martyrs, F-38054 Grenoble, France;
        Dupont, S., CEA, Liten, INES. 50 avenue du Lac Leman, F-73370 Le Bourget-du-Lac, France;
        Durand, M., CEA, INES, DTS, 50 avenue du Lac Leman, F-73370 Le Bourget-du-Lac, France;
        David, D., Lund University, Department of Physical Geography and Ecosystem Science (INES), Lund, Sweden'

        will be parsed in the "df_addr_country_inst" dataframe if affiliation filter is not defined (initialization step):
   
        pub_id  idx_author                     address               country    institutions                   
            0       0        CEA, LITEN Solar & Thermodynam , ...    France     CEA_France;LITEN_France        
            0       0        Univ Grenoble Alpes,...                 France     UGA_France;Universities_France      
            0       1        CNRS, Proc Mat Lab, PROMES,...          France     CNRS_France;PROMES_France                                  
            0       2        CNRS, Proc Mat Lab, PROMES, ...         France     CNRS_France;PROMES_France           
            0       3        CEA, Leti, 17 rue des Martyrs,...       France     CEA_France;LETI_France                        
            0       4        CEA, Liten, INES. 50 avenue...          France     CEA_France;LITEN_France;INES_France               
            0       5        CEA, INES, DTS, 50 avenue...            France     CEA_France;INES_France             
            0       6        Lund University,...(INES),...           Sweden     Lund Univ_Sweden;Universities_Sweden 
        
        given that the 'Affiliations' field string is:
        
        'CEA, LITEN Solar & Thermodynam Syst Lab L2ST, F-38054 Grenoble, France; 
        Univ Grenoble Alpes, F-38000 Grenoble, France; 
        CNRS, Proc Mat & Solar Energy Lab, PROMES, 7 Rue Four Solaire, F-66120 Font Romeu, France;
        CEA, Leti, 17 rue des Martyrs, F-38054 Grenoble, France;
        CEA, Liten, INES. 50 avenue du Lac Leman, F-73370 Le Bourget-du-Lac, France;
        CEA, INES, DTS, 50 avenue du Lac Leman, F-73370 Le Bourget-du-Lac, France;
        Lund University, Department of Physical Geography and Ecosystem Science (INES), Lund, Sweden'
        
        The institutions are identified and normalized using "inst_dic" dict which should be specified by the user.
        
        If affiliation filter is defined based on the following list of tuples (institution, country), 
        inst_filter_list = [('LITEN','France'),('INES','France'),('PROMES','France'), (Lund University, Sweden)]. 
        
        The "df_addr_country_inst" dataframe will be expended with the following columns (for pub_id = 0):
            LITEN_France  INES_France  PROMES_France  Lund University_Sweden                   
                 1            0              0                  0             
                 0            0              0                  0                              
                 0            0              1                  0                     
                 0            0              1                  0             
                 0            0              0                  0             
                 1            1              0                  0                              
                 0            1              0                  0                              
                 0            0              0                  1                      

    Args:
        corpus_df (dataframe): the dataframe of the scopus corpus.
        inst_filter_list (list): the affiliation filter list of tuples (institution, country)

    Returns:
        The dataframe df_addr_country_inst.
        
    Notes:
        The globals 'COL_NAMES', 'COLUMN_LABEL_SCOPUS', 'RE_SUB', 'RE_SUB_FIRST' and 'SYMBOL' are used 
        from `BiblioSpecificGlobals` module of `BiblioParsing` package.
        The functions `remove_special_symbol`, and `normalize_country` are imported 
        from `BiblioParsingUtils` module of `BiblioParsing` package.
        The functions  `address_inst_full_list`, `build_norm_raw_affiliations_dict`, 
        `read_inst_types` and `extend_author_institutions` are imported 
        from `BiblioParsingInstitutions` module of `BiblioParsing` package.
    """

    # Standard library imports
    import re
    from collections import namedtuple

    # Local library imports
    from BiblioParsing.BiblioParsingInstitutions import address_inst_full_list
    from BiblioParsing.BiblioParsingInstitutions import build_norm_raw_affiliations_dict
    from BiblioParsing.BiblioParsingInstitutions import extend_author_institutions
    from BiblioParsing.BiblioParsingInstitutions import read_inst_types
    from BiblioParsing.BiblioParsingInstitutions import read_towns_per_country
    from BiblioParsing.BiblioParsingInstitutions import standardize_address
    from BiblioParsing.BiblioParsingUtils import build_item_df_from_tup
    from BiblioParsing.BiblioParsingUtils import clean_authors_countries_institutions 
    from BiblioParsing.BiblioParsingUtils import normalize_country
    from BiblioParsing.BiblioParsingUtils import remove_special_symbol    

    # Globals imports
    from BiblioParsing.BiblioRegexpGlobals import RE_SUB
    from BiblioParsing.BiblioRegexpGlobals import RE_SUB_FIRST
    from BiblioParsing.BiblioSpecificGlobals import COL_NAMES
    from BiblioParsing.BiblioSpecificGlobals import COLUMN_LABEL_SCOPUS     

    # Setting useful aliases
    pub_id_alias               = COL_NAMES['pub_id'] 
    auth_inst_col_list_alias   = COL_NAMES['auth_inst']
    pub_idx_author_alias       = auth_inst_col_list_alias[1]
    norm_institution_alias     = auth_inst_col_list_alias[4] 
    scopus_aff_alias           = COLUMN_LABEL_SCOPUS['affiliations']
    scopus_auth_with_aff_alias = COLUMN_LABEL_SCOPUS['authors_with_affiliations']

    # Setting named tuples
    addr_country_inst  = namedtuple('address', auth_inst_col_list_alias[:-1])

    # Building the inst_dic dict
    norm_raw_aff_dict = build_norm_raw_affiliations_dict(country_affiliations_file_path = country_affiliations_file_path,
                                                         verbose = False)
    aff_type_dict = read_inst_types(inst_types_file_path = inst_types_file_path, inst_types_usecols = None)
    towns_dict = read_towns_per_country(country_towns_file = country_towns_file,
                                        country_towns_folder_path = country_towns_folder_path)

    addr_country_inst_list = []    
    for pub_id, affiliations, authors_affiliations in zip(corpus_df[pub_id_alias],
                                                          corpus_df[scopus_aff_alias],
                                                          corpus_df[scopus_auth_with_aff_alias]):
        # Initializing the authors and addresses counters
        idx_author, last_author = -1, '' 

        affiliations_list = affiliations.split(';')
        authors_affiliations_list = authors_affiliations.split(';')

        for author_affiliations in authors_affiliations_list:
            auth_item_nbr = 2
            author_affiliations_list = author_affiliations.split(',')
            if "." in author_affiliations_list[0]: # Change in scopus on 07/2023
                auth_item_nbr = 1
            author = (','.join(author_affiliations_list[0:auth_item_nbr])).strip()

            if last_author != author:
                idx_author += 1
            last_author = author

            author_addresses_str = ','.join(author_affiliations_list[auth_item_nbr:])
            author_addresses_list = []
            for raw_affiliation in affiliations_list:
                if raw_affiliation in author_addresses_str:
                    raw_affiliation = remove_special_symbol(raw_affiliation, only_ascii=True, strip=True)
                    std_affiliation = standardize_address(raw_affiliation)
                    author_addresses_list.append(std_affiliation) 
                    for address in author_addresses_list:
                        author_country_raw = address.split(',')[-1].strip()
                        author_country = normalize_country(author_country_raw)
                        author_institutions_tup = address_inst_full_list(address,
                                                                         norm_raw_aff_dict,
                                                                         aff_type_dict,
                                                                         towns_dict,
                                                                         drop_status = False)

                    addr_country_inst_list.append(addr_country_inst(pub_id,
                                                                    idx_author,
                                                                    address,
                                                                    author_country,
                                                                    author_institutions_tup.norm_inst_list,
                                                                    author_institutions_tup.raw_inst_list,))

    # Building a clean addresses-country-inst dataframe and accordingly updating the parsing success rate dict
    df_addr_country_inst, dic_failed = build_item_df_from_tup(addr_country_inst_list, auth_inst_col_list_alias[:-1], 
                                                              norm_institution_alias, pub_id_alias, dic_failed)    
    df_addr_country_inst = clean_authors_countries_institutions(df_addr_country_inst)

    if inst_filter_list is not None:
        df_addr_country_inst = extend_author_institutions(df_addr_country_inst, inst_filter_list)

    # Sorting the values in the dataframe returned by two columns
    df_addr_country_inst.sort_values(by = [pub_id_alias, pub_idx_author_alias], inplace = True)

    return df_addr_country_inst


def _build_subjects_scopus(corpus_df,
                           path_scopus_cat_codes,
                           path_scopus_journals_issn_cat,
                           dic_failed):
    
    '''Builds the dataframe "df_gross_subject" with two columns 'publi_id' 
    and 'ASJC_description'
    
    ex:       pub_id   ASJC_description
        0       0      Mathematics
        1       0      Engineering
         
    the AJS_description is the generic name of category given every multiple of 1OO in the 
    scopus_cat_codes.txt file. For exemple an extract of the scopus_cat_codes.txt file:
    
    General Medicine                 2700  mutiple of 100 General category
    Medicine (miscellaneous)         2701  subcategory
    Anatomy                          2702
    Anesthesiology and Pain Medicine 2703
    Biochemistry, medical            2704
    
    The codes attached to a journal are given in the tcv file scopus_journals_issn_cat:
    
    21st Century Music \t 15343219 \t 1210; 
    2D Materials \t \t     2210; 2211; 3104; 2500; 1600; 
    3 Biotech \t 2190572X \t1101; 2301; 1305;
    
    Args:
        corpus_df (dataframe): the dataframe of the wos/scopus corpus
        path_scopus_cat_codes=None,
        path_scopus_journals_issn_cat=None):

    Returns:
        The dataframe df_gross_subject
    '''
    
    # Standard library imports
    from pathlib import Path
    
    # 3rd party library imports
    import pandas as pd
    
    # Globals imports
    from BiblioParsing.BiblioSpecificGlobals import COL_NAMES
    from BiblioParsing.BiblioSpecificGlobals import COLUMN_LABEL_SCOPUS
    
    # Setting useful aliases
    pub_id_alias  = COL_NAMES['pub_id']
    subject_alias = COL_NAMES['subject'][1]
    scopus_journal_alias = COLUMN_LABEL_SCOPUS['journal']
    scopus_issn_alias    = COLUMN_LABEL_SCOPUS['issn']

    # Builds the dict "code_cat" {ASJC classification codes:description} out 
    # of the file "scopus_cat_codes.txt"
    # ex: {1000: 'Multidisciplinary', 1100: 'General Agricultural',...}
    # -----------------------------------------------------------------------
    df_scopus_cat_codes = pd.read_csv(path_scopus_cat_codes,
                                      sep='\t',
                                      header=None)
    code_cat = dict(zip(df_scopus_cat_codes[1].fillna(0.0).astype(int),
                        df_scopus_cat_codes[0]))

    # Builds the dataframe "df_scopus_journals_issn_cat" out of the file
    # "scopus_journals_issn_cat.txt"
    # "df_scopus_journals_issn_cat" has three columns:
    #       "journal": scopus journal name
    #       "issn": journal issn
    #       "keyword_id": list of keywords id asociated to the journal or the issn
    # -----------------------------------------------------------------------------
    df_scopus_journals_issn_cat = pd.read_csv(path_scopus_journals_issn_cat,
                                              sep='\t',
                                              header=None).fillna(0) 
    df_scopus_journals_issn_cat[2] = df_scopus_journals_issn_cat[2].str.split(';')
    df_scopus_journals_issn_cat.columns = ['journal','issn','keyword_id']

    # Builds the list "res" of tuples [(publi_id,scopus category),...]
    # ex: [(0, 'Applied Mathematics'), (0, 'Materials Chemistry'),...]
    # ----------------------------------------------------------------
    res = [] 
    for pub_id,journal, issn in zip(corpus_df[pub_id_alias],
                                    corpus_df[scopus_journal_alias],
                                    corpus_df[scopus_issn_alias] ):
        keywords = df_scopus_journals_issn_cat.query('journal==@journal ')['keyword_id']

        if len(keywords):                # Checks if journal found in scopus journal list
            try:                         # Checks if keywords id not empty (nan)
                keywords = keywords.tolist()
                for keyword in keywords: # append keyword dont take care of duplicates
                                         # takes index multiple of 100 to select "generic" keyword
                    res.extend([(pub_id,code_cat[int(i.strip()[0:2] + "00")].replace("General",""))
                                for i in keyword[:-1]])
            except:
                res.extend([(pub_id,'')])

        else:                            # check if journal found in scopus issn list
            keywords = df_scopus_journals_issn_cat.query('issn==@issn')['keyword_id']

            if len(keywords):
                try:
                    keywords = keywords.tolist()
                    for keyword in keywords: # append keyword dont take care of duplicates
                                             # takes index multiple of 100 to select "generic" keyword
                        res.extend([(pub_id,code_cat[int(i.strip()[0:2] + "00")].\
                                     replace("General",""))
                                     for i in keyword[:-1]])
                except:
                    res.extend([(pub_id,'')])

    # Builds the dataframe "df_keyword" out of tuples [(publi_id,scopus category),...]
    # "df_keyword" has two columns "pub_id" and "scopus_keywords". 
    # The duplicated rows are supressed.
    # ----------------------------------------------------------------            
    list_pub_id,list_keywords = zip(*res)
    
    df_subject = pd.DataFrame.from_dict({pub_id_alias:list_pub_id,
                                         subject_alias:list_keywords})
 
    list_id = df_subject[df_subject[subject_alias] == ''][pub_id_alias].values
    dic_failed[subject_alias] = {'success (%)':100*(1-len(list_id)/len(corpus_df)),
                                 pub_id_alias:[int(x) for x in list(list_id)]}

    df_subject.drop_duplicates(inplace=True)
    df_subject = df_subject[df_subject[subject_alias] != '']
    
    return df_subject


def _build_sub_subjects_scopus(corpus_df,
                               path_scopus_cat_codes,
                               path_scopus_journals_issn_cat,
                               dic_failed):
    
    '''Builds the dataframe "df_sub_subjects" with two columns 'publi_id' and 'ASJC_description'
    ex:       pub_id   ASJC_description
        0       0      Applied Mathematics
        1       0      Industrial and Manufacturing Engineering

    Args:
        corpus_df (dataframe): the dataframe of the wos/scopus corpus
        path_scopus_cat_codes (path): full path of the file path_scopus_cat_codes.txt
        path_scopus_journals_issn_cat (path): full path of the file scopus_journals_issn_cat.txt

    Returns:
        The dataframe sub_subjects
    '''
    
    # Standard library imports
    from pathlib import Path
    
    # 3rd party library imports
    import pandas as pd
    
    # Globals imports
    from BiblioParsing.BiblioSpecificGlobals import COL_NAMES
    from BiblioParsing.BiblioSpecificGlobals import COLUMN_LABEL_SCOPUS
    
    # Setting useful aliases
    pub_id_alias         = COL_NAMES['pub_id']
    sub_subject_alias    = COL_NAMES['sub_subject'][1]
    scopus_journal_alias = COLUMN_LABEL_SCOPUS['journal']
    scopus_issn_alias    = COLUMN_LABEL_SCOPUS['issn']

    # Builds the dict "code_cat" {ASJC classification codes:description} out of the file "scopus_cat_codes.txt"
    # ex: {1000: 'Multidisciplinary', 1100: 'General Agricultural and Biological Sciences',...}
    # -------------------------------------------------------------------------------------------------
    df_scopus_cat_codes = pd.read_csv(path_scopus_cat_codes,
                                      sep='\t',
                                      header=None)
    code_cat = dict(zip(df_scopus_cat_codes[1].fillna(0.0).astype(int),df_scopus_cat_codes[0]))

    # Builds the dataframe "df_scopus_journals_issn_cat" out of the file "scopus_journals_issn_cat.txt"
    # "df_scopus_journals_issn_cat" has three columns:
    #       "journal": scopus journal name
    #       "issn": journal issn
    #       "keyword_id": list of keywords id asoociated to the journal or the issn
    # -----------------------------------------------------------------------------
    df_scopus_journals_issn_cat = pd.read_csv(path_scopus_journals_issn_cat,
                                              sep='\t',
                                              header=None).fillna(0) 
    df_scopus_journals_issn_cat[2] = df_scopus_journals_issn_cat[2].str.split(';')
    df_scopus_journals_issn_cat.columns = ['journal','issn','keyword_id']


    # Builds the list "res" of tuples [(publi_id,scopus category),...]
    # ex: [(0, 'Applied Mathematics'), (0, 'Materials Chemistry'),...]
    # ----------------------------------------------------------------
    res = [] 
    for pub_id,journal, issn in zip(corpus_df[pub_id_alias],
                                    corpus_df[scopus_journal_alias],
                                    corpus_df[scopus_issn_alias] ):
        keywords = df_scopus_journals_issn_cat.query('journal==@journal ')['keyword_id']

        if len(keywords):                # Checks if journal found in scopus journal list
            try:                         # Checks if keywords id not empty (nan)
                keywords = keywords.tolist()
                for keyword in keywords: # append keyword dont take care of duplicates
                    res.extend([(pub_id,code_cat[int(i)]) for i in keyword[:-1]])
            except:
                res.extend([(pub_id,'')])

        else:                            # check if journal found in scopus issn list
            keywords = df_scopus_journals_issn_cat.query('issn==@issn')['keyword_id']

            if len(keywords):
                try:
                    keywords = keywords.tolist()
                    for keyword in keywords: # append keyword dont take care of duplicates
                        res.extend([(pub_id,code_cat[int(i)]) for i in keyword[:-1]])
                except:
                    res.extend([(pub_id,'')])

    # Builds the dataframe "df_keyword" out of tuples [(publ_id,scopus category),...]
    # "df_keyword" has two columns "pub_id" and "scopus_keywords". 
    # The duplicated rows are supressed.
    # ----------------------------------------------------------------            
    list_pub_id,list_keywords = zip(*res)
    df_sub_subject = pd.DataFrame.from_dict({pub_id_alias:list_pub_id,
                                              sub_subject_alias:list_keywords})    
    
    list_id = df_sub_subject[df_sub_subject[sub_subject_alias] == ''][pub_id_alias].values
    dic_failed[sub_subject_alias] = {'success (%)':100*(1-len(list_id)/len(corpus_df)),
                                     pub_id_alias:[int(x) for x in list(list_id)]}
    
    df_sub_subject.drop_duplicates(inplace=True)
    
    df_sub_subject = df_sub_subject[df_sub_subject[sub_subject_alias] != '']
    
    return df_sub_subject


def _build_articles_scopus(corpus_df):
 
    '''Builds the dataframe "df_article" with three columns:
   
    Authors|Year|Source title|Volume|Page start|DOI|Document Type|
    Language of Original Document|Title|ISSN
 
    Args:
        corpus_df (dataframe): the dataframe of the wos/scopus corpus
 
 
    Returns:
        The dataframe df_institution
        
    '''
    #To Do: Doc string update
    
    # Standard library imports
    import re
    
    # Local library imports
    from BiblioParsing.BiblioParsingUtils import normalize_name

    # Globals imports
    from BiblioParsing.BiblioGeneralGlobals import DASHES_CHANGE
    from BiblioParsing.BiblioGeneralGlobals import LANG_CHAR_CHANGE
    from BiblioParsing.BiblioGeneralGlobals import PONCT_CHANGE    
    from BiblioParsing.BiblioSpecificGlobals import COL_NAMES
    from BiblioParsing.BiblioSpecificGlobals import COLUMN_LABEL_SCOPUS
    from BiblioParsing.BiblioSpecificGlobals import NORM_JOURNAL_COLUMN_LABEL         
    from BiblioParsing.BiblioSpecificGlobals import DIC_DOCTYPE
    from BiblioParsing.BiblioSpecificGlobals import UNKNOWN

    re_issn = re.compile(r'^[0-9]{8}|[0-9]{4}|[0-9]{3}X') # Used to normalize the ISSN to the
                                                          # form dddd-dddd or dddd-dddX used by wos
    
    def _convert_issn(text):        
        y = ''.join(re.findall(re_issn,  text))
        if len(y) != 0:
            return y[0:4] + "-" + y[4:]
        else:
            return UNKNOWN
   
    def _str_int_convertor(x):
        try:
            return(int(float(x)))
        except:
            return 0
        
    def _treat_author(list_authors):
        authors_sep = ',' 
        if ';' in list_authors: authors_sep = ';'             # Change in scopus on 07/2023
        raw_first_author = list_authors.split(authors_sep)[0] # we pick the first author
        first_author = normalize_name(raw_first_author) 
        return first_author
    
    def _treat_doctype(doctype):
        for doctype_key,doctype_list in DIC_DOCTYPE.items():
            if doctype in doctype_list: doctype = doctype_key
        return doctype 
    
    def _treat_title(title):
        title = title.translate(DASHES_CHANGE)
        title = title.translate(LANG_CHAR_CHANGE)
        title = title.translate(PONCT_CHANGE)
        return title
    
    # Setting useful aliases
    pub_id_alias            = COL_NAMES['pub_id']
    articles_col_list_alias = COL_NAMES['articles']
    author_alias            = articles_col_list_alias[1]
    year_alias              = articles_col_list_alias[2]
    doc_type_alias          = articles_col_list_alias[7] 
    title_alias             = articles_col_list_alias[9]
    issn_alias              = articles_col_list_alias[-1]
    
    
    scopus_columns = [COLUMN_LABEL_SCOPUS['authors'],
                      COLUMN_LABEL_SCOPUS['year'],
                      COLUMN_LABEL_SCOPUS['journal'],
                      COLUMN_LABEL_SCOPUS['volume'],
                      COLUMN_LABEL_SCOPUS['page_start'],
                      COLUMN_LABEL_SCOPUS['doi'],
                      COLUMN_LABEL_SCOPUS['document_type'],
                      COLUMN_LABEL_SCOPUS['language'],
                      COLUMN_LABEL_SCOPUS['title'],
                      COLUMN_LABEL_SCOPUS['issn'],
                      NORM_JOURNAL_COLUMN_LABEL]                                 

    df_article = corpus_df[scopus_columns].astype(str)

    df_article.rename (columns = dict(zip(scopus_columns, articles_col_list_alias[1:])),
                       inplace = True)                      
   
    df_article[author_alias]   = df_article[author_alias].apply(_treat_author)
    df_article[year_alias]     = df_article[year_alias].apply(_str_int_convertor)
    df_article[doc_type_alias] = df_article[doc_type_alias].apply(_treat_doctype)
    df_article[title_alias]    = df_article[title_alias].apply(_treat_title)
    df_article[issn_alias]     = df_article[issn_alias].apply(_convert_issn)
    
    df_article.insert(0, pub_id_alias, list(corpus_df[pub_id_alias]))

    return df_article


def _build_references_scopus(corpus_df):
    
    '''Builds the dataframe "df_references" of cited references by the article
    referenced with the key publi_id:
    
            pub_id  author     year         journal          volume  page
        0    0    Bellouard Q  2017   Int. J. Hydrog. Energy   42    13486
        1    0    Bellouard Q  2017   Energy Fuels             31    10933
        2    0    Bellouard Q  2018   Int. J. Hydrog. Energy   44    19193

    Args:
        corpus_df (dataframe): the dataframe of the wos/scopus corpus


    Returns:
        A dataframe df_keyword
    '''
    #To Do: Doc string update
    
    # Standard library imports
    import re
    from collections import namedtuple
    
    # 3rd party library imports
    import pandas as pd
    
    # Local library imports
    from BiblioParsing.BiblioParsingUtils import normalize_name
    
    # Globals imports
    from BiblioParsing.BiblioRegexpGlobals import RE_DETECT_SCOPUS_NEW
    from BiblioParsing.BiblioRegexpGlobals import RE_REF_AUTHOR_SCOPUS
    from BiblioParsing.BiblioRegexpGlobals import RE_REF_AUTHOR_SCOPUS_NEW
    from BiblioParsing.BiblioRegexpGlobals import RE_REF_JOURNAL_SCOPUS
    from BiblioParsing.BiblioRegexpGlobals import RE_REF_JOURNAL_SCOPUS_NEW
    from BiblioParsing.BiblioRegexpGlobals import RE_REF_PAGE_SCOPUS
    from BiblioParsing.BiblioRegexpGlobals import RE_REF_PAGE_SCOPUS_NEW
    from BiblioParsing.BiblioRegexpGlobals import RE_REF_PROC_SCOPUS
    from BiblioParsing.BiblioRegexpGlobals import RE_REF_VOL_SCOPUS
    from BiblioParsing.BiblioRegexpGlobals import RE_REF_YEAR_SCOPUS
    from BiblioParsing.BiblioSpecificGlobals import COL_NAMES
    from BiblioParsing.BiblioSpecificGlobals import COLUMN_LABEL_SCOPUS
    from BiblioParsing.BiblioSpecificGlobals import PARTIAL
    from BiblioParsing.BiblioSpecificGlobals import UNKNOWN
    
    # Setting useful alias
    pub_id_alias       = COL_NAMES['pub_id']
    ref_col_list_alias = COL_NAMES['references']
    scopus_ref_alias   = COLUMN_LABEL_SCOPUS['references']
    
    # Setting named tuple
    ref_article = namedtuple('ref_article', ref_col_list_alias)
    
    list_ref_article =[]
    dic_ref = {}               
    for pub_id, row in zip(list(corpus_df[pub_id_alias]),
                                corpus_df[scopus_ref_alias]):

        if isinstance(row, str): # if the reference field is not empty and not an URL
            for field in row.split(";"):
                if RE_DETECT_SCOPUS_NEW.search(field):  # detect new SCOPUS coding 2023

                    year = re.findall(RE_REF_YEAR_SCOPUS, field)
                    if len(year):
                        year = year[0]
                    else:
                        year = 0

                    author = re.findall(RE_REF_AUTHOR_SCOPUS_NEW, field)
                    if len(author):
                        author = normalize_name(author[0])
                    else:
                        author = UNKNOWN            

                    page = re.findall(RE_REF_PAGE_SCOPUS_NEW, field)
                    if len(page) == 0:
                        page = 0
                    else:
                        page = page[0].split('p.')[1]

                    journal_vol =  re.findall(RE_REF_JOURNAL_SCOPUS_NEW , field)
                    if journal_vol:
                        journal_split = journal_vol[0].split(',')
                        journal = journal_split[0]
                        vol  = journal_split[1]
                    else:
                        journal = UNKNOWN
                        vol = 0

                else: # use old parsing

                    year = re.findall(RE_REF_YEAR_SCOPUS, field)
                    if len(year):
                        year = year[0]
                    else:
                        year = 0
                        
                    author = re.findall(RE_REF_AUTHOR_SCOPUS, field)
                    if len(author):
                        author = normalize_name(author[0])
                    else:
                        author = UNKNOWN

                    proceeding = re.findall(RE_REF_PROC_SCOPUS, field)
                    if not proceeding:
                        journal = re.findall(RE_REF_JOURNAL_SCOPUS, field)
                        if journal:
                            if ',' in journal[0] :
                                journal = journal[0][6:-1]
                            else:
                                journal = journal[0][6:]
                        else:
                            journal = UNKNOWN
                    else:
                        journal = proceeding

                    vol = re.findall(RE_REF_VOL_SCOPUS, field)
                    if len(vol):
                        if ',' in vol[0]:
                            vol = re.findall(r'\d{1,6}',vol[0])[0]
                        else:
                            vol = vol[0].strip()
                    else:
                        vol = 0

                    page = re.findall(RE_REF_PAGE_SCOPUS, field)
                    if len(page) == 0:
                        page = 0
                    else:
                        page = page[0].split('p.')[1]

            if author == UNKNOWN or journal == UNKNOWN: author = PARTIAL 
            list_ref_article.append(ref_article(pub_id,author,year,journal,vol,page))
    
    df_references = pd.DataFrame.from_dict({label:[s[idx] for s in list_ref_article] 
                                            for idx,label in enumerate(ref_col_list_alias)})    
    return df_references


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
        (dataframe): The corrected full rawdata of the corpus.
    """
    pub_id_col, authors_col, affil_col, auth_affil_col = check_cols
    corrected_pub_ids = []
    new_corpus_df = corpus_df.copy()
    for row_idx, row in corpus_df.iterrows():
        pub_id = row[pub_id_col]
        std_sep = "; "
        authors_list = row[authors_col].split(std_sep)        
        affil_list = row[affil_col].split(std_sep)
        auth_affil_list = row[auth_affil_col].split(std_sep)

        check_sep = ";"
        check_auth_affil_list = row[auth_affil_col].split(check_sep)
        authors_nb = len(authors_list)
        auth_affil_nb = len(check_auth_affil_list)
        if authors_nb!=auth_affil_nb:
            false_sep = ";, "
            correct_sep = ", "
            if any(lambda s: false_sep in s for s in affil_list):
                affil_list = [x.replace(false_sep, correct_sep) for x in affil_list]
            if any(lambda s: false_sep in s for s in auth_affil_list):
                auth_affil_list = [x.replace(false_sep, correct_sep) for x in auth_affil_list]
            corrected_pub_ids.append(pub_id)
        new_corpus_df.loc[row_idx, affil_col] = std_sep.join(affil_list)
        new_corpus_df.loc[row_idx, auth_affil_col] = std_sep.join(auth_affil_list)
    if verbose:
        print("  - Corrected lists of affilations and author-with-affiliations "
              f"for publication IDs: {corrected_pub_ids}")        
    return new_corpus_df


def _correct_firstname_initials(author, fullname):
    if "(" in fullname:
        fullname = fullname.split(" (")[0]
    lastname, firstname = fullname.split(", ")
    firstname = firstname.replace('-',' ').strip(' ')
    firstname_list = firstname.split(' ')
    firstname_list = sum([x.split('.') for x in firstname.split(' ')], [])
    initials_list = [x[0]+"." for x in firstname_list if x]
    initials = ''.join(initials_list)
    new_author = ' '.join([lastname, initials])
    return new_author


def _correct_auth_data(author, auth_tup):
    fullname, auth_affil = auth_tup
    new_author = _correct_firstname_initials(author, fullname)
    
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
        (dataframe): The corrected full rawdata of the corpus.
    """
    # Local library imports
    from BiblioParsing.BiblioParsingUtils import remove_special_symbol
    pub_id_col, authors_col, fullname_col, auth_affil_col = check_cols
    corrected_pub_ids = []
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
                corrected_pub_ids.append(pub_id)
            new_authors_list.append(new_author)
            new_auth_affils_list.append(new_auth_affil)

        # Updating the corpus data with the corrected lists
        new_corpus_df.loc[row_idx, authors_col] = "; ".join(new_authors_list)
        new_corpus_df.loc[row_idx, auth_affil_col] = "; ".join(new_auth_affils_list)
    if verbose:
        print(f"  - Corrected lists of authors for publication IDs: {corrected_pub_ids}")        
    return new_corpus_df
            
        
def _correct_full_rawdata(corpus_df):
    """Corrects firstname initials and affiliations of authors 
    in the full rawdata of the corpus.

    Args:
        corpus_df (dataframe): The full rawdata of the corpus.
    Returns:
        (dataframe): The corrected full rawdata of the corpus.
    """
    
    # Globals imports
    from BiblioParsing.BiblioSpecificGlobals import COL_NAMES
    from BiblioParsing.BiblioSpecificGlobals import COLUMN_LABEL_SCOPUS
    from BiblioParsing.BiblioSpecificGlobals import COLUMN_LABEL_SCOPUS_PLUS
    
    # Setting useful aliases    
    pub_id_col = COL_NAMES['pub_id']
    authors_col = COLUMN_LABEL_SCOPUS['authors']
    affil_col = COLUMN_LABEL_SCOPUS['affiliations']
    auth_affil_col = COLUMN_LABEL_SCOPUS['authors_with_affiliations']
    fullnames_col = COLUMN_LABEL_SCOPUS_PLUS['auth_fullnames']

    affil_check_cols = [pub_id_col, authors_col, affil_col, auth_affil_col]
    auth_check_cols = [pub_id_col, authors_col, fullnames_col, auth_affil_col]

    # Setting the pub_id in df index
    corpus_df.index = range(len(corpus_df))

    # Setting the pub-id as a column
    corpus_df = corpus_df.rename_axis(pub_id_col).reset_index()

    # Correcting corpus data
    new_corpus_df = _check_authors_with_affiliations(corpus_df, affil_check_cols,
                                                     verbose=True)
    new_corpus_df = _check_authors(new_corpus_df, auth_check_cols,
                                   verbose=True)
    # Droping pub_id_col column
    new_corpus_df.drop(columns=[pub_id_col])
    return new_corpus_df


def _check_affiliation_column_scopus(df):
    
    '''The `_check_affiliation_column_scopus` function checks the correcteness of the column affiliation of a df 
    read from a csv scopus file.
    A cell of the column affiliation should read:
    address<0>, country<0>;...; address<i>, country<i>;...
    
    Some cells can be misformatted with an uncorrect country field. The function eliminates, for each
    cell of the column, those items address<i>, country<i> uncorrectly formatted. When such an item is detected
    a warning message is printed.    
    '''
    #To Do: Doc string update
    
    # Local local imports
    from BiblioParsing.BiblioParsingUtils import normalize_country
    
    # Globals imports
    from BiblioParsing.BiblioSpecificGlobals import COLUMN_LABEL_SCOPUS
    
    # Setting useful aliases
    scopus_aff_alias = COLUMN_LABEL_SCOPUS['affiliations']
        
    def _valid_affiliation(row):
        nonlocal idx
        idx += 1
        valid_affiliation_list = []
        for affiliation in row[scopus_aff_alias].split(';'):
            raw_country = affiliation.split(',')[-1].strip()
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
            return ';'.join(valid_affiliation_list)
        else:
            return 'unkown'
    
    idx = -1
    df[scopus_aff_alias] = df.apply(_valid_affiliation,axis=1) 
    
    return df


def read_database_scopus(rawdata_path, scopus_ids=False):
    """Gets the Scopus rawdata and the Scopus-Ids of the publications.

    First, it corrects the firsname initials and the affiliations 
    of the authors when required using the `_correct_full_rawdata` 
    internal function. 
    Then, :
    - It checks columns and drops unuseful columns using the \
    `check_and_drop_columns` function imported from `BiblioParsingUtils` module.
    - It checks the affilation column content using the `_check_affiliation_column_scopus` \
       internal function. 
    - It replaces the unavailable items values by a string set in the global UNKNOWN.
    - It normalizes the journal names using the `normalize_journal_names` function \
    imported from the `BiblioParsingUtils` module.

    Args:
        rawdata_path (path): The full path to the Scopus-rawdata file.
        scopus_ids (bool): Optional for building the data of Scopus IDs of publications \
        (dafault=False).
    Returns:
        (tup): (The cleaned corpus data (dataframe), The optional Scopus-IDs data). 
    """
    
    # 3rd party library imports
    import numpy as np
    import pandas as pd
    
    # Local library imports
    from BiblioParsing.BiblioParsingUtils import build_pub_db_ids
    from BiblioParsing.BiblioParsingUtils import check_and_drop_columns
    from BiblioParsing.BiblioParsingUtils import check_and_get_rawdata_file_path
    from BiblioParsing.BiblioParsingUtils import normalize_journal_names
    
    # Globals imports
    from BiblioParsing.BiblioSpecificGlobals import COL_NAMES
    from BiblioParsing.BiblioSpecificGlobals import COLUMN_LABEL_SCOPUS_PLUS
    from BiblioParsing.BiblioSpecificGlobals import COLUMN_TYPE_SCOPUS
    from BiblioParsing.BiblioSpecificGlobals import SCOPUS
    from BiblioParsing.BiblioSpecificGlobals import SCOPUS_RAWDATA_EXTENT
    from BiblioParsing.BiblioSpecificGlobals import UNKNOWN

    # Setting useful aliases
    init_scopus_id_col = COLUMN_LABEL_SCOPUS_PLUS['scopus_id']
    scopus_id_col = COL_NAMES['scopus_id'][0]
    
    # Check if rawdata file is available and get its full path if it is 
    rawdata_file_path = check_and_get_rawdata_file_path(rawdata_path, SCOPUS_RAWDATA_EXTENT)

    return_tup = None
    if rawdata_file_path:    
        full_scopus_rawdata_df = pd.read_csv(rawdata_file_path, dtype=COLUMN_TYPE_SCOPUS)

        if len(full_scopus_rawdata_df):
            full_scopus_rawdata_df = _correct_full_rawdata(full_scopus_rawdata_df)
            
            # Selecting useful rawdata for parsing
            scopus_rawdata_df = check_and_drop_columns(SCOPUS, full_scopus_rawdata_df)
            scopus_rawdata_df = _check_affiliation_column_scopus(scopus_rawdata_df)
            scopus_rawdata_df = scopus_rawdata_df.replace(np.nan, UNKNOWN, regex=True)
            scopus_rawdata_df = normalize_journal_names(SCOPUS, scopus_rawdata_df)
            return_tup = (scopus_rawdata_df)

            if scopus_ids:
                # Building the Scopus-IDs data
                scopus_ids_df = build_pub_db_ids(full_scopus_rawdata_df, init_scopus_id_col, scopus_id_col)
                return_tup = (scopus_rawdata_df, scopus_ids_df)               
    return return_tup 


def biblio_parser_scopus(rawdata_path, inst_filter_list=None, country_affiliations_file_path=None,
                         inst_types_file_path=None, country_towns_file=None,
                         country_towns_folder_path=None):
    
    '''The function `biblio_parser_scopus` generates parsing dataframes from the csv file stored in the rawdata folder.    
    The columns of the csv file are read and parsed using the functions:
        _build_references_scopus which parses the column 'References',
        _build_authors_scopus which parses the column 'Authors'
        _build_keywords_scopus which parses the column 'Author Keywords' (for author keywords AK),
                                        the column 'Index Keywords' (for journal keywords IK),
                                        the column 'title' (for title keywords IK)
        _build_addresses_countries_institutions_scopus which parses the column 'Affiliations'
        _build_authors_countries_institutions_scopus which parses the column 'Authors with affiliations'
        _build_subjects_scopus which parses the column 'Source title', 'ISSN'
        _build_sub_subjects_scopus which parses the column 'Source title', 'ISSN'
        _build_articles_scopus which parses the columns 'Authors','Year','Source title','Volume',
            'Page start','DOI','Document Type','Language of Original Document','Title','ISSN'.
            
    Args:
    
    
    Returns:
    
    
    Note:
    

    '''
    
    # Standard library imports
    import json
    from pathlib import Path
    
    # 3rd party imports
    import pandas as pd
    
    # Local imports
    from BiblioParsing.BiblioGeneralGlobals import REP_UTILS
    from BiblioParsing.BiblioSpecificGlobals import COL_NAMES
    from BiblioParsing.BiblioSpecificGlobals import PARSING_ITEMS_LIST
    from BiblioParsing.BiblioSpecificGlobals import SCOPUS
    from BiblioParsing.BiblioSpecificGlobals import SCOPUS_CAT_CODES
    from BiblioParsing.BiblioSpecificGlobals import SCOPUS_JOURNALS_ISSN_CAT
    from BiblioParsing.BiblioParsingScopus import read_database_scopus
    
    # Internal functions    
    def _keeping_item_parsing_results(item, item_df):
        scopus_parsing_dict[item] = item_df
    
    # Setting useful aliases
    articles_alias     = PARSING_ITEMS_LIST[0]
    authors_alias      = PARSING_ITEMS_LIST[1]
    addresses_alias    = PARSING_ITEMS_LIST[2]
    countries_alias    = PARSING_ITEMS_LIST[3]
    institutions_alias = PARSING_ITEMS_LIST[4]
    auth_inst_alias    = PARSING_ITEMS_LIST[5]
    authors_kw_alias   = PARSING_ITEMS_LIST[6]
    index_kw_alias     = PARSING_ITEMS_LIST[7]
    title_kw_alias     = PARSING_ITEMS_LIST[8]    
    subjects_alias     = PARSING_ITEMS_LIST[9]
    sub_subjects_alias = PARSING_ITEMS_LIST[10]
    references_alias   = PARSING_ITEMS_LIST[11]
    items_aliases_list = [PARSING_ITEMS_LIST[x] for x in range(12)]
    
    # Setting the specific file paths for subjects ans sub-subjects assignement for scopus corpuses    
    path_scopus_cat_codes = Path(__file__).parent / Path(REP_UTILS) / Path(SCOPUS_CAT_CODES)
    path_scopus_journals_issn_cat = Path(__file__).parent / Path(REP_UTILS) / Path(SCOPUS_JOURNALS_ISSN_CAT)   

    # Reading and checking the corpus file
    corpus_df, scopus_ids_df = read_database_scopus(rawdata_path, scopus_ids=True)
    
    # Initializing the scopus_dic_failed dict for the parsing control
    scopus_dic_failed = {}
    
    # Initializing the dict of dataframes resulting from the parsing
    scopus_parsing_dict = {}
    
    if corpus_df is not None:                      
        # Keeping the number of articles in scopus_dic_failed dict
        scopus_dic_failed['number of article'] = len(corpus_df)
        if len(corpus_df):
            # Building the dataframe of articles
            print(f"  - {articles_alias} parsing...", end="\r")
            articles_df = _build_articles_scopus(corpus_df)
            _keeping_item_parsing_results(articles_alias, articles_df)
            print(f"  - {articles_alias} parsed    ")
            
            # Building the dataframe of authors
            print(f"  - {authors_alias} parsing...", end="\r")
            authors_df = _build_authors_scopus(corpus_df, scopus_dic_failed)
            _keeping_item_parsing_results(authors_alias, authors_df)
            print(f"  - {authors_alias} parsed    ")
            
            # Building the dataframe of addresses, countries and institutions
            print(f"  - {addresses_alias}, {countries_alias} and {institutions_alias} parsing...", end="\r")
            addresses_tup = _build_addresses_countries_institutions_scopus(corpus_df, scopus_dic_failed)
            addresses_df, countries_df, institutions_df = addresses_tup
            _keeping_item_parsing_results(addresses_alias, addresses_df)
            _keeping_item_parsing_results(countries_alias, countries_df)
            _keeping_item_parsing_results(institutions_alias, institutions_df)
            print(f"  - {addresses_alias}, {countries_alias} and {institutions_alias} parsed    ")
            
            # Building the dataframe of authors and their institutions
            print(f"  - {auth_inst_alias} parsing...", end="\r")
            auth_inst_df = _build_authors_countries_institutions_scopus(corpus_df, scopus_dic_failed, 
                                                                        inst_filter_list = inst_filter_list ,
                                                                        country_affiliations_file_path = country_affiliations_file_path,
                                                                        inst_types_file_path = inst_types_file_path,
                                                                        country_towns_file = country_towns_file,
                                                                        country_towns_folder_path = country_towns_folder_path)
            _keeping_item_parsing_results(auth_inst_alias, auth_inst_df)
            print(f"  - {auth_inst_alias} parsed    ")
            
            # Building the dataframes of keywords
            print(f"  - {authors_kw_alias}, {index_kw_alias} and {title_kw_alias} parsing...", end="\r")
            keywords_tup = _build_keywords_scopus(corpus_df, scopus_dic_failed) 
            AK_keywords_df, IK_keywords_df, TK_keywords_df = keywords_tup
            _keeping_item_parsing_results(authors_kw_alias, AK_keywords_df)
            _keeping_item_parsing_results(index_kw_alias, IK_keywords_df)
            _keeping_item_parsing_results(title_kw_alias, TK_keywords_df)
            print(f"  - {authors_kw_alias}, {index_kw_alias} and {title_kw_alias} parsed    ")
            
            # Building the dataframe of subjects
            print(f"  - {subjects_alias} parsing...", end="\r")
            subjects_df = _build_subjects_scopus(corpus_df,
                                                 path_scopus_cat_codes,
                                                 path_scopus_journals_issn_cat,
                                                 scopus_dic_failed)
            _keeping_item_parsing_results(subjects_alias, subjects_df)
            print(f"  - {subjects_alias} parsed    ")
           
            # Building the dataframe of sub-subjects
            print(f"  - {sub_subjects_alias} parsing...", end="\r")
            sub_subjects_df = _build_sub_subjects_scopus(corpus_df,
                                                         path_scopus_cat_codes,
                                                         path_scopus_journals_issn_cat,
                                                         scopus_dic_failed)
            _keeping_item_parsing_results(sub_subjects_alias, sub_subjects_df)
            print(f"  - {sub_subjects_alias} parsed    ")
            
            # Building the dataframe of references
            print(f"  - {references_alias} parsing...", end="\r")
            references_df = _build_references_scopus(corpus_df)
            _keeping_item_parsing_results(references_alias, references_df)
            print(f"  - {references_alias} parsed    ")
    
        else:
            empty_df = pd.DataFrame()
            for item in items_aliases_list:
                _keeping_item_parsing_results(item, empty_df)
            
    return scopus_parsing_dict, scopus_dic_failed, scopus_ids_df      
        