__all__ = ['biblio_parser_scopus','read_database_scopus']


def _build_authors_scopus(df_corpus, dic_failed):
    
    '''Builds the dataframe "df_co_authors" of the co-authors of the article
    referenced with the key publi_id:
    
               pub_id  idx_author   co-author
          0        0      0          Boujjat H.
          1        0      1          Rodat S.

    Args:
        df_corpus (dataframe): the dataframe of the wos/scopus corpus

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
    for pub_id,x in zip(df_corpus[pub_id_alias],
                        df_corpus[scopus_auth_alias]):
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


def _build_keywords_scopus(df_corpus, dic_failed):
    
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
        df_corpus (dataframe): the dataframe of the wos/scopus corpus

    Returns:
        The dataframe df_keyword
    '''
    # To Do: Check the use of UNKNOWN versus '"null"'
    
    # Standard library imports
    from collections import namedtuple
    from collections import Counter
    from operator import attrgetter
    
    # 3rd party library imports
    import nltk
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
    df_AK = df_corpus[scopus_auth_kw_alias].fillna('')
    for pub_id,keywords_AK in zip(df_corpus[pub_id_alias],df_AK):
        list_keywords_AK = keywords_AK.split(';')      
        for keyword_AK in list_keywords_AK:
            keyword_AK = keyword_AK.strip()
            list_keyword_AK.append(key_word(pub_id,
                                   keyword_AK if keyword_AK != 'null' else '”null”'))

    list_keyword_IK = []
    df_IK = df_corpus[scopus_idx_kw_alias].fillna('')
    for pub_id,keywords_IK in zip(df_corpus[pub_id_alias],df_IK):
        list_keywords_IK = keywords_IK.split(';')
        for keyword_IK in list_keywords_IK:
            keyword_IK = keyword_IK.strip()
            if keyword_IK == 'null': keyword_IK = UNKNOWN # replace 'null' by the keyword UNKNOWN
            list_keyword_IK.append(key_word(pub_id,
                                            keyword_IK if keyword_IK != 'null' else '”null”'))
            
    list_keyword_TK = []
    df_title = pd.DataFrame(df_corpus[scopus_title_kw_alias].fillna(''))
    df_title.columns = [title_alias]  # To be coherent with the convention of function build_title_keywords 
    df_TK,list_of_words_occurrences = build_title_keywords(df_title)
    for pub_id in df_corpus[pub_id_alias]:
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


def _build_addresses_countries_institutions_scopus(df_corpus, dic_failed):
    
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
        df_corpus (dataframe): the dataframe of the wos/scopus corpus


    Returns:
        The dataframes df_address, df_country, df_institution.
        
    Notes:
        The globals 'COL_NAMES', 'COLUMN_LABEL_SCOPUS', 'RE_SUB', 'RE_SUB_FIRST' and 'UNKNOWN' 
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
    from BiblioParsing.BiblioSpecificGlobals import UNKNOWN

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
    for pub_id, affiliation in zip(df_corpus[pub_id_alias],
                                   df_corpus[scopus_aff_alias]):
        list_affiliation = affiliation.split(';')
        
        if list_affiliation:
            for idx_address, address_pub in enumerate(list_affiliation):
                
                address_pub = remove_special_symbol(address_pub, only_ascii=True, strip=True)
                list_addresses.append(address(pub_id,
                                              idx_address,
                                              address_pub))

                institution = address_pub.split(',')[0]
                institution = re.sub(RE_SUB_FIRST,'University' + ', ',institution)
                institution = re.sub(RE_SUB,'University'+' ', institution)
                list_institutions.append(ref_institution(pub_id,
                                                         idx_address,
                                                         institution))
                country_raw = address_pub.split(',')[-1].replace(';','').strip()  
                country     = normalize_country(country_raw)
                if country == '':
                    country = UNKNOWN
                    warning = (f'WARNING: the invalid country name "{country_raw}" '
                               f'in pub_id {pub_id} has been replaced by "{UNKNOWN}"'
                               f'in "_build_addresses_countries_institutions_scopus" function of "BiblioParsingScopus.py" module')
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
        warning = (f'WARNING: Lengths of "df_address", "df_country" and "df_institution" dataframes are not equal'
                   f'in "_build_addresses_countries_institutions_scopus" function of "BiblioParsingScopus.py" module')
        print(warning)
    
    return df_address, df_country, df_institution


def _build_authors_countries_institutions_scopus(df_corpus, dic_failed, inst_filter_list):
    
    '''The `_build_authors_countries_institutions_scopus' function parses the fields 'Affiliations' 
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
        df_corpus (dataframe): the dataframe of the scopus corpus.
        inst_filter_list (list): the affiliation filter list of tuples (institution, country)

    Returns:
        The dataframe df_addr_country_inst.
        
    Notes:
        The globals 'COL_NAMES', 'COLUMN_LABEL_SCOPUS', 'RE_SUB', 'RE_SUB_FIRST' and 'SYMBOL' are used 
        from `BiblioSpecificGlobals` module of `BiblioParsing` package.
        The functions `remove_special_symbol`, and `normalize_country` are imported 
        from `BiblioParsingUtils` module of `BiblioAnalysis_utils` package.
        The functions  `address_inst_full_list` and `build_institutions_dic` are imported 
        from `BiblioParsingInstitutions` module of `BiblioAnalysis_utils` package.
             
    '''
    
    # Standard library imports
    import re
    from collections import namedtuple
    
    # 3rd party library imports
    import pandas as pd
    
    # Local library imports
    from BiblioParsing.BiblioParsingInstitutions import address_inst_full_list
    from BiblioParsing.BiblioParsingInstitutions import build_institutions_dic
    from BiblioParsing.BiblioParsingInstitutions import extend_author_institutions
    from BiblioParsing.BiblioParsingUtils import build_item_df_from_tup 
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
    inst_dic = build_institutions_dic(rep_utils = None, dic_inst_filename = None)
    
    list_addr_country_inst = []    
    for pub_id, affiliations, authors_affiliations in zip(df_corpus[pub_id_alias],
                                                          df_corpus[scopus_aff_alias],
                                                          df_corpus[scopus_auth_with_aff_alias]):
        
        idx_author, last_author = -1, '' # Initialization for the author and address counter
        
        list_affiliations = affiliations.split(';')
        list_authors_affiliations = authors_affiliations.split(';')
        
        for x in list_authors_affiliations:
            auth_item_nbr = 2
            if "." in x.split(',')[0]: auth_item_nbr = 1              # Change in scopus on 07/2023
            author = (','.join(x.split(',')[0:auth_item_nbr])).strip()

            if last_author != author:
                idx_author += 1
            last_author = author

            author_list_addresses = ','.join(x.split(',')[auth_item_nbr:])

            author_address_list_raw = []
            for affiliation_raw in list_affiliations:
                if affiliation_raw in author_list_addresses:
                    affiliation_raw = remove_special_symbol(affiliation_raw, only_ascii=True, strip=True)
                    affiliation_raw = re.sub(RE_SUB_FIRST,'University' + ', ',affiliation_raw)
                    affiliation     = re.sub(RE_SUB,'University' + ' ',affiliation_raw)
                    author_address_list_raw.append(affiliation) 

                    for address in author_address_list_raw:
                        author_country_raw      = address.split(',')[-1].strip()
                        author_country          = normalize_country(author_country_raw)
                        author_institutions_tup = address_inst_full_list(address, inst_dic)

                    list_addr_country_inst.append(addr_country_inst(pub_id,
                                                                    idx_author,
                                                                    address,
                                                                    author_country,
                                                                    author_institutions_tup.norm_inst_list,
                                                                    author_institutions_tup.raw_inst_list,))
                
    # Building a clean addresses-country-inst dataframe and accordingly updating the parsing success rate dict
    df_addr_country_inst, dic_failed = build_item_df_from_tup(list_addr_country_inst, auth_inst_col_list_alias[:-1], 
                                                              norm_institution_alias, pub_id_alias, dic_failed)
    
    if inst_filter_list is not None:
        df_addr_country_inst = extend_author_institutions(df_addr_country_inst, inst_filter_list)
        
    # Sorting the values in the dataframe returned by two columns
    df_addr_country_inst.sort_values(by = [pub_id_alias, pub_idx_author_alias], inplace = True)
    
    return df_addr_country_inst


def _build_subjects_scopus(df_corpus,
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
        df_corpus (dataframe): the dataframe of the wos/scopus corpus
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
    for pub_id,journal, issn in zip(df_corpus[pub_id_alias],
                                    df_corpus[scopus_journal_alias],
                                    df_corpus[scopus_issn_alias] ):
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
    dic_failed[subject_alias] = {'success (%)':100*(1-len(list_id)/len(df_corpus)),
                                 pub_id_alias:[int(x) for x in list(list_id)]}

    df_subject.drop_duplicates(inplace=True)
    df_subject = df_subject[df_subject[subject_alias] != '']
    
    return df_subject


def _build_sub_subjects_scopus(df_corpus,
                               path_scopus_cat_codes,
                               path_scopus_journals_issn_cat,
                               dic_failed):
    
    '''Builds the dataframe "df_sub_subjects" with two columns 'publi_id' and 'ASJC_description'
    ex:       pub_id   ASJC_description
        0       0      Applied Mathematics
        1       0      Industrial and Manufacturing Engineering

    Args:
        df_corpus (dataframe): the dataframe of the wos/scopus corpus
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
    for pub_id,journal, issn in zip(df_corpus[pub_id_alias],
                                    df_corpus[scopus_journal_alias],
                                    df_corpus[scopus_issn_alias] ):
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
    dic_failed[sub_subject_alias] = {'success (%)':100*(1-len(list_id)/len(df_corpus)),
                                     pub_id_alias:[int(x) for x in list(list_id)]}
    
    df_sub_subject.drop_duplicates(inplace=True)
    
    df_sub_subject = df_sub_subject[df_sub_subject[sub_subject_alias] != '']
    
    return df_sub_subject


def _build_articles_scopus(df_corpus):
 
    '''Builds the dataframe "df_article" with three columns:
   
    Authors|Year|Source title|Volume|Page start|DOI|Document Type|
    Language of Original Document|Title|ISSN
 
    Args:
        df_corpus (dataframe): the dataframe of the wos/scopus corpus
 
 
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

    df_article = df_corpus[scopus_columns].astype(str)

    df_article.rename (columns = dict(zip(scopus_columns, articles_col_list_alias[1:])),
                       inplace = True)                      
   
    df_article[author_alias]   = df_article[author_alias].apply(_treat_author)
    df_article[year_alias]     = df_article[year_alias].apply(_str_int_convertor)
    df_article[doc_type_alias] = df_article[doc_type_alias].apply(_treat_doctype)
    df_article[title_alias]    = df_article[title_alias].apply(_treat_title)
    df_article[issn_alias]     = df_article[issn_alias].apply(_convert_issn)
    
    df_article.insert(0, pub_id_alias, list(df_corpus[pub_id_alias]))

    return df_article


def _build_references_scopus(df_corpus):
    
    '''Builds the dataframe "df_references" of cited references by the article
    referenced with the key publi_id:
    
            pub_id  author     year         journal          volume  page
        0    0    Bellouard Q  2017   Int. J. Hydrog. Energy   42    13486
        1    0    Bellouard Q  2017   Energy Fuels             31    10933
        2    0    Bellouard Q  2018   Int. J. Hydrog. Energy   44    19193

    Args:
        df_corpus (dataframe): the dataframe of the wos/scopus corpus


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
    for pub_id, row in zip(list(df_corpus[pub_id_alias]),
                                df_corpus[scopus_ref_alias]):

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


def read_database_scopus(rawdata_path):
    
    '''The `read_database_scopus`function reads the raw scopus-database file `filename`,
       checks columns and drops unuseful columns using the `check_and_drop_columns` function.
       It checks the affilation column content using the `_check_affiliation_column_scopus` 
       internal function. 
       It replaces the unavailable items values by a string set in the global UNKNOW.
       It normalizes the journal names using the `normalize_journal_names` function.
       
    Args:
        filename (str): the full path of the scopus-database file. 
        
    Returns:
        (dataframe): the cleaned corpus dataframe.
        
    Note:
        The functions 'check_and_drop_columns' and 'normalize_journal_names' from `BiblioParsingUtils` module 
        of `BiblioParsing`module are used.
        The globals 'SCOPUS' and 'UNKNOWN' from `BiblioSpecificGlobals` module 
        of `BiblioParsing`module are used.
        
    '''
    
    # 3rd party library imports
    import numpy as np
    import pandas as pd
    
    # Local library imports
    from BiblioParsing.BiblioParsingUtils import check_and_drop_columns
    from BiblioParsing.BiblioParsingUtils import check_and_get_rawdata_file_path
    from BiblioParsing.BiblioParsingUtils import normalize_journal_names
    
    # Globals imports
    from BiblioParsing.BiblioSpecificGlobals import COLUMN_TYPE_SCOPUS
    from BiblioParsing.BiblioSpecificGlobals import SCOPUS
    from BiblioParsing.BiblioSpecificGlobals import SCOPUS_RAWDATA_EXTENT
    from BiblioParsing.BiblioSpecificGlobals import UNKNOWN
    
    # Check if rawdata file is available and get its full path if is 
    rawdata_file_path = check_and_get_rawdata_file_path(rawdata_path, SCOPUS_RAWDATA_EXTENT)
    
    if rawdata_file_path:    
        df = pd.read_csv(rawdata_file_path, dtype = COLUMN_TYPE_SCOPUS)    
        df = check_and_drop_columns(SCOPUS, df)    
        df = _check_affiliation_column_scopus(df)
        df = df.replace(np.nan, UNKNOWN, regex = True)
        df = normalize_journal_names(SCOPUS, df)
    else:
        df = None        
    return df


def biblio_parser_scopus(rawdata_path, inst_filter_list = None):
    
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
    
    # Globals imports
    from BiblioParsing.BiblioSpecificGlobals import COL_NAMES
    from BiblioParsing.BiblioSpecificGlobals import PARSING_ITEMS_LIST
    from BiblioParsing.BiblioSpecificGlobals import REP_UTILS
    from BiblioParsing.BiblioSpecificGlobals import SCOPUS
    from BiblioParsing.BiblioSpecificGlobals import SCOPUS_CAT_CODES
    from BiblioParsing.BiblioSpecificGlobals import SCOPUS_JOURNALS_ISSN_CAT
    
    # Internal functions    
    def _keeping_item_parsing_results(item, item_df):
        scopus_parsing_dict[item] = item_df
    
    # Setting useful aliases
    articles_item_alias     = PARSING_ITEMS_LIST[0]
    authors_item_alias      = PARSING_ITEMS_LIST[1]
    addresses_item_alias    = PARSING_ITEMS_LIST[2]
    countries_item_alias    = PARSING_ITEMS_LIST[3]
    institutions_item_alias = PARSING_ITEMS_LIST[4]
    auth_inst_item_alias    = PARSING_ITEMS_LIST[5]
    raw_inst_item_alias     = PARSING_ITEMS_LIST[6]
    authors_kw_item_alias   = PARSING_ITEMS_LIST[7]
    index_kw_item_alias     = PARSING_ITEMS_LIST[8]
    title_kw_item_alias     = PARSING_ITEMS_LIST[9]    
    subjects_item_alias     = PARSING_ITEMS_LIST[10]
    sub_subjects_item_alias = PARSING_ITEMS_LIST[11]
    references_item_alias   = PARSING_ITEMS_LIST[12]  
    
    # Setting the specific file paths for subjects ans sub-subjects assignement for scopus corpuses    
    path_scopus_cat_codes = Path(__file__).parent / Path(REP_UTILS) / Path(SCOPUS_CAT_CODES)
    path_scopus_journals_issn_cat = Path(__file__).parent / Path(REP_UTILS) / Path(SCOPUS_JOURNALS_ISSN_CAT)

    # Reading and checking the corpus file
    df_corpus = read_database_scopus(rawdata_path)
    
    # Initializing the scopus_dic_failed dict for the parsing control
    scopus_dic_failed = {}
    
    # Initializing the dict of dataframes resulting from the parsing
    scopus_parsing_dict = {}
    
    if df_corpus is not None:
        
        # Keeping the number of articles in scopus_dic_failed dict
        scopus_dic_failed['number of article'] = len(df_corpus)

        # Building the dataframe of articles
        articles_df = _build_articles_scopus(df_corpus)
        _keeping_item_parsing_results(articles_item_alias, articles_df)

        # Building the dataframe of authors
        authors_df = _build_authors_scopus(df_corpus, scopus_dic_failed)
        _keeping_item_parsing_results(authors_item_alias, authors_df)

        # Building the dataframe of addresses, countries and institutions
        addresses_df, countries_df, institutions_df = _build_addresses_countries_institutions_scopus(df_corpus,
                                                                                                     scopus_dic_failed)
          # Keeping addresses df
        _keeping_item_parsing_results(addresses_item_alias, addresses_df)
          # Keeping countries df 
        _keeping_item_parsing_results(countries_item_alias, countries_df)
          # Keeping institutions df
        _keeping_item_parsing_results(institutions_item_alias, institutions_df)

        # Building the dataframe of authors and their institutions
        authors_institutions_df = _build_authors_countries_institutions_scopus(df_corpus, scopus_dic_failed, inst_filter_list)
        _keeping_item_parsing_results(auth_inst_item_alias, authors_institutions_df)
            # Building raw institutions file for further expending normalized institutions list               
        #raw_institutions_df = build_raw_institutions(authors_institutions_df)               # Not yet used for Scopus
        #_keeping_item_parsing_results(raw_inst_item_alias, raw_institutions_df)    

        # Building the dataframes of keywords
        AK_keywords_df, IK_keywords_df, TK_keywords_df = _build_keywords_scopus(df_corpus, scopus_dic_failed)   
          # Keeping author keywords df 
        _keeping_item_parsing_results(authors_kw_item_alias, AK_keywords_df)
          # Keeping journal (indexed) keywords df
        _keeping_item_parsing_results(index_kw_item_alias, IK_keywords_df)
          # Keeping title keywords df 
        _keeping_item_parsing_results(title_kw_item_alias, TK_keywords_df)

        # Building the dataframe of subjects
        subjects_df = _build_subjects_scopus(df_corpus,
                                             path_scopus_cat_codes,
                                             path_scopus_journals_issn_cat,
                                             scopus_dic_failed)
        _keeping_item_parsing_results(subjects_item_alias, subjects_df)

        # Building the dataframe of sub-subjects
        sub_subjects_df = _build_sub_subjects_scopus(df_corpus,
                                                     path_scopus_cat_codes,
                                                     path_scopus_journals_issn_cat,
                                                     scopus_dic_failed)
        _keeping_item_parsing_results(sub_subjects_item_alias, sub_subjects_df)

        # Building the dataframe of references
        references_df = _build_references_scopus(df_corpus)
        _keeping_item_parsing_results(references_item_alias, references_df)        
    
    return scopus_parsing_dict, scopus_dic_failed
        
        