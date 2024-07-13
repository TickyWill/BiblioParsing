__all__ = ['biblio_parser_wos','read_database_wos']


def _build_authors_wos(df_corpus, dic_failed):
    
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
    
    # Local library imports
    from BiblioParsing.BiblioParsingUtils import build_item_df_from_tup
    from BiblioParsing.BiblioParsingUtils import normalize_name
    
    # Globals imports
    from BiblioParsing.BiblioSpecificGlobals import COL_NAMES
    from BiblioParsing.BiblioSpecificGlobals import COLUMN_LABEL_WOS
        
    # Setting useful aliases
    pub_id_alias        = COL_NAMES['pub_id']
    auth_col_list_alias = COL_NAMES['authors']
    co_authors_alias       = auth_col_list_alias[2]
    wos_auth_alias      = COLUMN_LABEL_WOS['authors']
    
    # Setting named tuple
    co_author = namedtuple('co_author', auth_col_list_alias )    
    
    list_author = []
    for pub_id,x in zip(df_corpus[pub_id_alias],
                        df_corpus[wos_auth_alias]):
        idx_author = 0
        for y in x.split(';'):
            author = normalize_name(y.replace('.','').replace(',',''))  # <----- to be checked
            if author not in ['Dr','Pr','Dr ','Pr ']:
                list_author.append(co_author(pub_id,
                                             idx_author,
                                             author))
                idx_author += 1
    
    # Building a clean co-authors dataframe and accordingly updating the parsing success rate dict
    df_co_authors, dic_failed = build_item_df_from_tup(list_author, auth_col_list_alias, 
                                                       co_authors_alias, pub_id_alias, dic_failed)
    
    return df_co_authors


def _build_keywords_wos(df_corpus, dic_failed):
    
    '''Builds the dataframe "df_keyword" with three columns:
                pub_id  type  keyword
            0     0      AK    Biomass
            1     0      AK    Gasification
            2     0      AK    Solar energy
    with: 
         type = AK for author keywords 
         type = IK for indexed keywords
         type = TK for title keywords
         
    The title keywords are builds out of the set TK_corpus of the most cited nouns 
    (at leat N times) in the set of all the articles. The keywords of type TK of an
    article, referenced by the key pub_id, are the elements of the intersection
    between the set TK_corpus and the set of the nouns of the article title.
    
        
    Args:
        df_corpus (dataframe): the dataframe of the wos/scopus corpus

    Returns:
        df_keyword (dataframe): pub_id | type | keyword 
        dic_failed (dict): dic_failed[type] = {"success (%)":rate of success,
                            "pub_id":list of orphan article}
    '''
    
    # Standard library imports
    from collections import namedtuple
    
    # 3rd party library imports
    import pandas as pd
    
    # Local library imports
    from BiblioParsing.BiblioParsingUtils import build_item_df_from_tup
    from BiblioParsing.BiblioParsingUtils import build_title_keywords 
    
    # Globals imports
    from BiblioParsing.BiblioSpecificGlobals import COL_NAMES
    from BiblioParsing.BiblioSpecificGlobals import COLUMN_LABEL_WOS
    from BiblioParsing.BiblioSpecificGlobals import UNKNOWN
    
    # Setting useful aliases
    pub_id_alias       = COL_NAMES['pub_id']
    kw_col_List_alias  = COL_NAMES['keywords']
    tmp_col_list_alias = COL_NAMES['temp_col']
    keyword_alias      = kw_col_List_alias[1]
    title_alias        = tmp_col_list_alias[2]
    kept_tokens_alias  = tmp_col_list_alias[4]
    wos_auth_kw_alias  = COLUMN_LABEL_WOS['author_keywords']
    wos_idx_kw_alias   = COLUMN_LABEL_WOS['index_keywords']
    wos_title_kw_alias = COLUMN_LABEL_WOS['title']
    
    # Setting named tuple
    key_word = namedtuple('key_word', kw_col_List_alias)    
    
    list_keyword_AK = [] 
    df_AK = df_corpus[wos_auth_kw_alias].fillna('')
    for pub_id,keywords_AK in zip(df_corpus[pub_id_alias],df_AK):
        list_keywords_AK = keywords_AK.split(';')      
        for keyword_AK in list_keywords_AK:
            keyword_AK = keyword_AK.lower().strip()
            list_keyword_AK.append(key_word(pub_id,
                                   keyword_AK if keyword_AK != 'null' else '”null”'))

    list_keyword_IK = []
    df_IK = df_corpus[wos_idx_kw_alias].fillna('')
    for pub_id,keywords_IK in zip(df_corpus[pub_id_alias],df_IK):
        list_keywords_IK = keywords_IK.split(';')
        for keyword_IK in list_keywords_IK:
            keyword_IK = keyword_IK.lower().strip()
            if keyword_IK == 'null': keyword_IK = UNKNOWN # replace 'null' by the keyword UNKNOWN
            list_keyword_IK.append(key_word(pub_id,
                                            keyword_IK if keyword_IK != 'null' else '”null”'))
            
    list_keyword_TK = []
    df_title = pd.DataFrame(df_corpus[wos_title_kw_alias].fillna('')) # Tranform a data list into dataframe
    df_title.columns = [title_alias]  # To be coherent with the convention of function build_title_keywords 
    df_TK,list_of_words_occurrences = build_title_keywords(df_title)
    for pub_id in df_corpus[pub_id_alias]:
        for token in df_TK.loc[pub_id,kept_tokens_alias]:
            token = token.lower().strip()
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
    
    return df_keyword_AK, df_keyword_IK, df_keyword_TK


def _build_addresses_countries_institutions_wos(df_corpus, dic_failed):
    
    '''Parse the field 'C1' of wos database to retrieve the article author address (without duplicates),
       the author country and affiliation. Beware, multiple formats may exist for the 'C1' field. 
       We take care for two different formats in this implementation.
       
    For example the string:

    '[Boujjat, Houssame] CEA, LITEN Solar & Thermodynam Syst Lab L2ST, F-38054 Grenoble, France;
     [Boujjat, Houssame] Univ Grenoble Alpes, F-38000 Grenoble, France; 
     [Rodat, Sylvain; Chuayboon, Srirat; Abanades, Stephane] CNRS, Proc Mat & Solar Energy Lab,
     PROMES, 7 Rue Four Solaire, F-66120 Font Romeu, France'

       will be parsed in:
    
    df_address:
    
    pub_id  idx_address                     address   
        0       0        CEA, LITEN Solar & Thermodynam Syst Lab L2ST, ...
        0       1        Univ    Grenoble Alpes, F-38000 Grenoble, France;
        0       2        CNRS,   Proc Mat & Solar Energy Lab, PROMES, 7 ...
    
    df_country:
        
    pub_id  idx_author   country
      0        0         France
      0        1         France
      0        2         France
      0        3         France
      0        4         France
      
   df_institution: 
   
   pub_id  idx_author   institution
     0       0          CEA
     0       1          University Grenoble Alpes
     0       2          CNRS
     0       3          CNRS
     0       4          CNRS

    Args:
        df_corpus (dataframe): the dataframe of the wos/scopus corpus.

    Returns:
        The dataframes df_address, df_country, df_institution.
        
    Notes:
        The globals 'COL_NAMES', 'COLUMN_LABEL_WOS', 'RE_ADDRESS', 'RE_AUTHOR', 'RE_SUB', 'RE_SUB_FIRST'
        and 'UNKNOWN' are imported from `BiblioSpecificGlobals` module of `BiblioParsing` package.
        The functions `remove_special_symbol`, `address_inst_full_list` and `normalize_country` 
        are imported from `BiblioParsingUtils` of `BiblioAnalysis_utils` package.
        
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
    from BiblioParsing.BiblioRegexpGlobals import RE_ADDRESS
    from BiblioParsing.BiblioRegexpGlobals import RE_AUTHOR
    from BiblioParsing.BiblioRegexpGlobals import RE_SUB
    from BiblioParsing.BiblioRegexpGlobals import RE_SUB_FIRST    
    from BiblioParsing.BiblioSpecificGlobals import COL_NAMES
    from BiblioParsing.BiblioSpecificGlobals import COLUMN_LABEL_WOS
    from BiblioParsing.BiblioSpecificGlobals import UNKNOWN
    
    # Setting useful aliases
    pub_id_alias             = COL_NAMES['pub_id']
    address_col_List_alias   = COL_NAMES['address']
    country_col_list_alias   = COL_NAMES['country']
    inst_col_list_alias      = COL_NAMES['institution']
    address_alias            = address_col_List_alias[2]
    country_alias            = country_col_list_alias[2]
    institution_alias        = inst_col_list_alias[2]     
    wos_auth_with_aff_alias  = COLUMN_LABEL_WOS['authors_with_affiliations']
    
    # Setting named tuples 
    address     = namedtuple('address', address_col_List_alias )
    country     = namedtuple('country', country_col_list_alias )
    institution = namedtuple('institution', inst_col_list_alias )    
    
    list_addresses    = []
    list_countries    = []
    list_institutions = []
    for pub_id, affiliation in zip(df_corpus[pub_id_alias],
                                   df_corpus[wos_auth_with_aff_alias]):       
        try:
            if '[' in affiliation:                           # ex: '[Author1] address1;[Author1, Author2] address2...'
                #authors = RE_AUTHOR.findall(affiliation)    # for future use
                addresses = RE_ADDRESS.findall(affiliation)
            else:                                            # ex: 'address1;address2...'
                addresses = affiliation.split(';')   
        except:
            print(pub_id, affiliation)
        
        if addresses:
            for idx, author_address in enumerate(addresses):
                
                author_address = remove_special_symbol(author_address, only_ascii = True, strip = True)
                list_addresses.append(address(pub_id,
                                              idx,
                                              author_address))

                author_institution_raw = author_address.split(',')[0]
                author_institution_raw = re.sub(RE_SUB_FIRST,'University' + ', ', author_institution_raw)                 
                author_institution     = re.sub(RE_SUB,'University' + ' ', author_institution_raw)
                list_institutions.append(institution(pub_id, idx, author_institution))

                author_country_raw = author_address.split(',')[-1].replace(';','').strip()
                author_country     = normalize_country(author_country_raw)
                if author_country == '':
                    author_country = UNKNOWN
                    warning = (f'WARNING: the invalid country name "{author_country_raw}" '
                               f'in pub_id {pub_id} has been replaced by "{UNKNOWN}" '
                               f'in "_build_addresses_countries_institutions_wos" function of "BiblioParsingWos.py" module')
                    print(warning)

                list_countries.append(country(pub_id, idx, author_country))
        else:
            list_addresses.append(address(pub_id, 0, ''))
            list_institutions.append(institution(pub_id, 0, ''))
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
    
    if not(len(df_address)==len(df_country)==len(df_institution)):
        warning = (f'WARNING: Lengths of "df_address", "df_country" and "df_institution" dataframes are not equal'
                   f'in "_build_addresses_countries_institutions_wos" function of "BiblioParsingWos.py" module')
        print(warning)
    
    return df_address, df_country, df_institution


def _build_authors_countries_institutions_wos(df_corpus, dic_failed, inst_filter_list = None,
                                              country_affiliations_file_path = None,
                                              inst_types_file_path = None):
    """The `_build_authors_countries_institutions_wos' function parses the fields 'C1' 
       of wos database to retrieve the article authors with their addresses, affiliations and country. 
       In addition, a secondary affiliations list may be added according to a filtering of affiliations.
       
       Beware, multiple formats may exist for the 'C1' field. 
       The parsing is effective only for the format of the following example.
       Otherwise, the parsing fields are set to empty strings.
       
       For example, the 'C1' field string:
       '[Boujjat, Houssame] CEA, LITEN Solar & Thermodynam Syst Lab L2ST, F-38054 Grenoble, France;
       [Boujjat, Houssame] Univ Grenoble Alpes, F-38000 Grenoble, France; 
       [Rodat, Sylvain; Chuayboon, Srirat] CNRS, Proc Mat & Solar Energy Lab,
       PROMES, 7 Rue Four Solaire, F-66120 Font Romeu, France;
       [Abanades, Stephane] CEA, Leti, 17 rue des Martyrs, F-38054 Grenoble, France;
       [Dupont, Sylvain] CEA, Liten, INES. 50 avenue du Lac Leman, F-73370 Le Bourget-du-Lac, France;
       [Durand, Maurice] CEA, INES, DTS, 50 avenue du Lac Leman, F-73370 Le Bourget-du-Lac, France;
       [David, David] Lund University, Department of Physical Geography and Ecosystem Science (INES), Lund, Sweden'

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
        df_corpus (dataframe): the dataframe of the wos corpus.
        dic_failed (dict): the dict to be extended with key "'authors_inst'" and value (%) of the parsing success.
        inst_filter_list (list): the affiliation filter list of tuples (institution, country) 
                                 which value is 'None' at initialization of the parsing.
                            
    Returns:
        (dataframe):  the dataframe of addresses, countrie and institutions for each author of a publication.
        
    Notes:
        The globals 'COL_NAMES', 'COLUMN_LABEL_WOS', 'RE_ADDRESS', 'RE_AUTHOR', 'RE_SUB', 'RE_SUB_FIRST',
        'SYMBOL' and 'UNKNOWN' are imported from `BiblioSpecificGlobals` module of `BiblioParsing` package.
        The functions `remove_special_symbol` and `normalize_country` are imported from `BiblioParsingUtils` 
        of `BiblioAnalysis_utils` package.
        The functions `address_inst_full_list` is imported 
        from `BiblioParsingInstitutions` module of `BiblioAnalysis_utils` package.
        
    """
    
    # Standard library imports
    import re
    from collections import namedtuple
    
    # Local library imports 
    from BiblioParsing.BiblioParsingInstitutions import address_inst_full_list
    from BiblioParsing.BiblioParsingInstitutions import build_norm_raw_affiliations_dict
    from BiblioParsing.BiblioParsingInstitutions import extend_author_institutions
    from BiblioParsing.BiblioParsingInstitutions import read_inst_types
    from BiblioParsing.BiblioParsingUtils import build_item_df_from_tup
    from BiblioParsing.BiblioParsingUtils import normalize_country
    from BiblioParsing.BiblioParsingUtils import remove_special_symbol
    
    # Globals imports
    from BiblioParsing.BiblioRegexpGlobals import RE_ADDRESS
    from BiblioParsing.BiblioRegexpGlobals import RE_AUTHOR
    from BiblioParsing.BiblioRegexpGlobals import RE_SUB
    from BiblioParsing.BiblioRegexpGlobals import RE_SUB_FIRST
    from BiblioParsing.BiblioSpecificGlobals import COL_NAMES
    from BiblioParsing.BiblioSpecificGlobals import COLUMN_LABEL_WOS
    from BiblioParsing.BiblioSpecificGlobals import UNKNOWN

    # Setting useful aliases
    pub_id_alias             = COL_NAMES['pub_id']
    auth_inst_col_list_alias = COL_NAMES['auth_inst']
    pub_idx_author_alias     = auth_inst_col_list_alias[1]
    norm_institution_alias   = auth_inst_col_list_alias[4]    
    wos_auth_with_aff_alias  = COLUMN_LABEL_WOS['authors_with_affiliations']
    wos_auth_fullnames_alias = COLUMN_LABEL_WOS['authors_fullnames']
    
    # Setting namedtuples
    addr_country_inst  = namedtuple('address',auth_inst_col_list_alias[:-1] )
    author_address_tup = namedtuple('author_address','author address')
    
    # Building the inst_dic dict
    norm_raw_aff_dict = build_norm_raw_affiliations_dict(country_affiliations_file_path = country_affiliations_file_path,
                                                         verbose = False)
    aff_type_dict = read_inst_types(inst_types_file_path = inst_types_file_path, inst_types_usecols = None)
    
    list_addr_country_inst = []
    for pub_id, affiliation in zip(df_corpus[pub_id_alias],
                                   df_corpus[wos_auth_with_aff_alias]):
        if '[' in affiliation:  # Proceed if the field author is present in affiliation.
           
            # From the wos column C1 builds the list of tuples [([Author1, Author2,...], address1),...].
            list_authors = [[x.strip() for x in authors.split(';')] for authors in RE_AUTHOR.findall(affiliation)]
            list_affiliation = [x.strip() for x in RE_ADDRESS.findall(affiliation)]
            list_affiliation = list_affiliation if list_affiliation else ['']
            list_tuples = tuple(zip(list_authors, list_affiliation)) 

            # Builds the list of tuples [(Author<0>, address<0>),(Author<0>, address<1>),...,(Author<i>, address<j>)...]
            list_author_address_tup = [author_address_tup(y,x[1]) for x in list_tuples for y in x[0]]            
            
            # Build the list of ordered authors full names
            authors_list_ordered = df_corpus.loc[pub_id, wos_auth_fullnames_alias].split(';')
            authors_list_ordered = [author_ordered.strip() for author_ordered in authors_list_ordered]
                
            for tup in list_author_address_tup:
                # check the case of an author in list_author_address_tup not an effective author name (ex: a team name or anonymous)
                if tup.author in authors_list_ordered: 
                    idx_author = authors_list_ordered.index(tup.author)

                    author_country_raw = tup.address.split(',')[-1].replace(';','').strip()
                    author_country = normalize_country(author_country_raw)
                    if author_country == '':
                        author_country = UNKNOWN
                        warning = (f'WARNING: the invalid country name "{author_country_raw}" '
                                   f'in pub_id {pub_id} has been replaced by "{UNKNOWN}" '
                                   f'in "_build_addresses_countries_institutions_wos" function of "BiblioParsingWos.py" module')
                        print(warning)

                    author_address_raw = tup.address
                    author_address_raw = remove_special_symbol(author_address_raw, only_ascii=True, strip=True)
                    author_address = re.sub(RE_SUB_FIRST,'University' + ', ', author_address_raw) 
                    author_address = re.sub(RE_SUB,'University' + ' ', author_address_raw)
                    author_institutions_tup = address_inst_full_list(author_address,
                                                                     norm_raw_aff_dict,
                                                                     aff_type_dict,
                                                                     drop_status = False)

                    list_addr_country_inst.append(addr_country_inst(pub_id,
                                                                    idx_author,
                                                                    tup.address,
                                                                    author_country,
                                                                    author_institutions_tup.norm_inst_list,
                                                                    author_institutions_tup.raw_inst_list,))
                else:
                    pass
                
        else:  # If the field author is not present in affiliation complete namedtuple with the global UNKNOWN
            list_addr_country_inst.append(addr_country_inst(pub_id, UNKNOWN, UNKNOWN, 
                                                            UNKNOWN, UNKNOWN, UNKNOWN,))

    # Building a clean addresses-country-inst dataframe and accordingly updating the parsing success rate dict
    df_addr_country_inst, dic_failed = build_item_df_from_tup(list_addr_country_inst, auth_inst_col_list_alias[:-1], 
                                                              norm_institution_alias, pub_id_alias, dic_failed)
    
    if inst_filter_list is not None:
        df_addr_country_inst = extend_author_institutions(df_addr_country_inst, inst_filter_list)
        
    # Sorting the values in the dataframe returned by two columns
    df_addr_country_inst.sort_values(by = [pub_id_alias, pub_idx_author_alias], inplace = True)

    return df_addr_country_inst


def _build_subjects_wos(df_corpus,dic_failed):
    
    '''Builds the dataframe "df_subject" using the column "SC":
    
            pub_id  subject
               0    Neurosciences & Neurology
               1    Psychology
               1    Environmental Sciences & Ecology
               2    Engineering
               2    Physics
               3    Philosophy
    
    
    Args:
        df_corpus (dataframe): the dataframe of the wos/scopus corpus

    Returns:
        The dataframe df_subject
    '''    
    
    # Standard library imports
    from collections import namedtuple
    
    # 3rd party library imports
    import pandas as pd
    
    # Local library imports
    from BiblioParsing.BiblioParsingUtils import build_item_df_from_tup
    
    # Globals imports
    from BiblioParsing.BiblioSpecificGlobals import COL_NAMES
    from BiblioParsing.BiblioSpecificGlobals import COLUMN_LABEL_WOS
    
    # Setting useful aliases
    pub_id_alias           = COL_NAMES['pub_id']
    subject_col_list_alias = COL_NAMES['subject']
    subject_alias          = subject_col_list_alias[1]
    wos_subjects_alias     = COLUMN_LABEL_WOS['subjects']
    
    # Setting named tuple
    subject = namedtuple('subject', subject_col_list_alias )    
    
    list_subject = []
    for pub_id,scs in zip(df_corpus[pub_id_alias], df_corpus[wos_subjects_alias]):
        for sc in scs.split(';'):
            list_subject.append(subject(pub_id,
                                        sc.strip()))
     
    # Building a clean subjects dataframe and accordingly updating the parsing success rate dict
    df_subject, dic_failed = build_item_df_from_tup(list_subject, subject_col_list_alias, 
                                                    subject_alias, pub_id_alias, dic_failed)    
    
    #df_subject = pd.DataFrame.from_dict({label:[s[idx] for s in list_subject] 
    #                                     for idx,label in enumerate(subject_col_list_alias)})
    #
    #list_id = df_subject[df_subject[subject_alias] == ''][pub_id_alias].values
    #list_id = list(set(list_id))
    #dic_failed[subject_alias] = {'success (%)':100*(1-len(list_id)/len(df_corpus)),
    #                             pub_id_alias:[int(x) for x in list(list_id)]}
    #
    #df_subject = df_subject[df_subject[subject_alias] != '']

    return df_subject


def _build_sub_subjects_wos(df_corpus,dic_failed):
    
    '''Builds the dataframe "df_wos_category" using the column "WC":
    
            pub_id  wos_category
               0    Engineering
               1    Materials Science
               1    Physics
               2    Materials Science
               2    Physics
               3    Chemistry
        
    Args:
        df_corpus (dataframe): the dataframe of the wos/scopus corpus

    Returns:
        The dataframe df_gross_subject
    '''
    
    # Standard library imports
    from collections import namedtuple
    
    # 3rd party library imports
    import pandas as pd

    # Local library imports
    from BiblioParsing.BiblioParsingUtils import build_item_df_from_tup
    
    # Globals imports
    from BiblioParsing.BiblioSpecificGlobals import COL_NAMES
    from BiblioParsing.BiblioSpecificGlobals import COLUMN_LABEL_WOS
    
    # Setting useful aliases
    pub_id_alias               = COL_NAMES['pub_id']
    sub_subject_col_list_alias = COL_NAMES['sub_subject']
    sub_subject_alias          = sub_subject_col_list_alias[1]
    wos_sub_subjects_alias     = COLUMN_LABEL_WOS['sub_subjects']
    
    # Setting named tuple
    sub_subject = namedtuple('sub_subject', sub_subject_col_list_alias ) 

    list_sub_subject = []
    for pub_id, sub_scs in zip(df_corpus[pub_id_alias], df_corpus[wos_sub_subjects_alias]):
        if isinstance(sub_scs,str):
            for sub_sc in sub_scs.split(';'):
                list_sub_subject.append(sub_subject(pub_id,
                                                    sub_sc.strip()))
    
    # Building a clean sub_subjects dataframe and accordingly updating the parsing success rate dict
    df_sub_subject, dic_failed = build_item_df_from_tup(list_sub_subject, sub_subject_col_list_alias, 
                                                        sub_subject_alias, pub_id_alias, dic_failed)
    
    return df_sub_subject


def _build_articles_wos(df_corpus):
 
    '''Builds the dataframe "df_article" with ten columns:
   
    Authors|Year|Source title|Volume|Page start|DOI|Document Type|
    Language of Original Document|Title|EID
 
    Args:
        df_corpus (dataframe): the dataframe of the wos corpus
 
 
    Returns:
        The dataframe df_institution
        
    '''
    
    # Local library imports
    from BiblioParsing.BiblioParsingUtils import normalize_name
    
    # Globals imports    
    from BiblioParsing.BiblioGeneralGlobals import DASHES_CHANGE
    from BiblioParsing.BiblioGeneralGlobals import LANG_CHAR_CHANGE
    from BiblioParsing.BiblioGeneralGlobals import PONCT_CHANGE    
    from BiblioParsing.BiblioSpecificGlobals import COL_NAMES
    from BiblioParsing.BiblioSpecificGlobals import COLUMN_LABEL_WOS
    from BiblioParsing.BiblioSpecificGlobals import NORM_JOURNAL_COLUMN_LABEL 
    from BiblioParsing.BiblioSpecificGlobals import DIC_DOCTYPE

    def _str_int_convertor(x):
        if x:
            return(str(x))
        else:
            return '0'
    
    def _treat_author(list_authors):
        first_author = list_authors.split(';')[0] # we pick the first author
        return  normalize_name(first_author)
    
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

    wos_columns = [COLUMN_LABEL_WOS['authors'],
                   COLUMN_LABEL_WOS['year'],
                   COLUMN_LABEL_WOS['journal'], 
                   COLUMN_LABEL_WOS['volume'],
                   COLUMN_LABEL_WOS['page_start'],
                   COLUMN_LABEL_WOS['doi'],
                   COLUMN_LABEL_WOS['document_type'],
                   COLUMN_LABEL_WOS['language'],
                   COLUMN_LABEL_WOS['title'],
                   COLUMN_LABEL_WOS['issn'],
                   NORM_JOURNAL_COLUMN_LABEL]
                   
    df_article = df_corpus.loc[:, wos_columns].astype(str)

    df_article.rename (columns = dict(zip(wos_columns, articles_col_list_alias[1:])),
                       inplace = True)    
                                                                                                
    df_article[author_alias]   = df_article[author_alias].apply(_treat_author)    
    df_article[year_alias]     = df_article[year_alias].apply(_str_int_convertor)
    df_article[doc_type_alias] = df_article[doc_type_alias].apply(_treat_doctype)
    df_article[title_alias]    = df_article[title_alias].apply(_treat_title)
    
    df_article.insert(0, pub_id_alias, list(df_corpus[pub_id_alias]))
   
    return df_article


def _build_references_wos(df_corpus):
   
    '''Builds the dataframe "df_references" of cited references by the article
    referenced with the key publi_id:
   
            pub_id  author     year         journal          volume  page
        0    0    Bellouard Q  2017   INT. J. HYDROG. ENERGY   42    13486
        1    0    Bellouard Q  2017   ENERGY FUELS             31    10933
        2    0    Bellouard Q  2018   INT. J. HYDROG. ENERGY   44    19193
 
    Args:
        df_corpus (dataframe): the dataframe of the wos/scopus corpus
 
 
    Returns:
        A dataframe df_keyword
    '''
 
    # Standard library imports
    import re
    from collections import namedtuple
 
    # 3rd party library imports
    import pandas as pd
    
    # Local library imports
    from BiblioParsing.BiblioParsingUtils import normalize_name
    
    # Globals imports
    from BiblioParsing.BiblioRegexpGlobals import RE_REF_AUTHOR_WOS
    from BiblioParsing.BiblioRegexpGlobals import RE_REF_JOURNAL_WOS
    from BiblioParsing.BiblioRegexpGlobals import RE_REF_PAGE_WOS
    from BiblioParsing.BiblioRegexpGlobals import RE_REF_VOL_WOS
    from BiblioParsing.BiblioRegexpGlobals import RE_REF_YEAR_WOS 
    from BiblioParsing.BiblioSpecificGlobals import COL_NAMES
    from BiblioParsing.BiblioSpecificGlobals import COLUMN_LABEL_WOS
    from BiblioParsing.BiblioSpecificGlobals import UNKNOWN
    
    # Setting useful alias
    pub_id_alias       = COL_NAMES['pub_id']
    ref_col_list_alias = COL_NAMES['references']
    wos_ref_alias      = COLUMN_LABEL_WOS['references']
 
    # Setting named tuple
    ref_article = namedtuple('ref_article', ref_col_list_alias)
    
    list_ref_article =[]
    for pub_id, row in zip(list(df_corpus[pub_id_alias]),
                                df_corpus[wos_ref_alias]):
 
        if isinstance(row, str): # if the reference field is not empty and not an URL
 
                for field in row.split(";"):
 
                    year = re.findall(RE_REF_YEAR_WOS, field) 
                    if len(year):
                        year = year[0][1:-1]
                    else:
                        year = 0
 
                    vol = re.findall(RE_REF_VOL_WOS, field)
                    if len(vol):
                        vol = vol[0][3:]
                    else:
                        vol = 0
 
                    page = re.findall(RE_REF_PAGE_WOS, field)
                    if len(page):
                        page = page[0][3:]
                    else:
                        page = 0
 
                    journal = re.findall(RE_REF_JOURNAL_WOS, field)
                    if len(journal):
                        journal = journal[0].strip()
                    else:
                        journal = UNKNOWN
 
                    author = re.findall(RE_REF_AUTHOR_WOS, field)
                    if len(author):
                        author = normalize_name(author[0][:-1])
                    else:
                        author = UNKNOWN
 
                    if (author != UNKNOWN) and (journal != UNKNOWN):
                        list_ref_article.append(ref_article(pub_id, author, year, journal, vol,page))
 
                    if (vol==0) & (page==0) & (author != UNKNOWN):
                        pass
    
    df_references = pd.DataFrame.from_dict({label:[s[idx] for s in list_ref_article] 
                                            for idx,label in enumerate(ref_col_list_alias)})
    
    return df_references


def read_database_wos(rawdata_path):
    
    '''The `read_database_wos` function allows to circumvent the error ParserError: '	' 
       expected after '"' generated by the method `pd.read_csv` when reading the raw wos-database file `filename`.
       Then, it checks columns and drops unuseful columns using the `check_and_drop_columns` function.
       It adds an index column. 
       It replaces the unavailable items values by a string set in the global UNKNOWN.
       It normalizes the journal names using the `normalize_journal_names` function.
       
    Args:
        filename (str): the full path of the wos-database file. 
        
    Returns:
        (dataframe): the cleaned corpus dataframe. 
       
    Note:
        The functions 'check_and_drop_columns' and 'normalize_journal_names' are imported from the `BiblioParsingUtils`module 
        of the `BiblioParsing` package.
        The globals 'ENCODING', 'FIELD_SIZE_LIMIT', 'UNKNOWN' and 'WOS' are imported from the `BiblioSpecificGlobals` module 
        of the `BiblioParsing` package.
        
    '''
    # Standard library imports
    import csv
    import numpy as np
    
    # 3rd party library imports
    import pandas as pd
    
    # Local library imports
    from BiblioParsing.BiblioParsingUtils import check_and_drop_columns
    from BiblioParsing.BiblioParsingUtils import check_and_get_rawdata_file_path
    from BiblioParsing.BiblioParsingUtils import normalize_journal_names
    
    # Globals imports
    from BiblioParsing.BiblioSpecificGlobals import ENCODING
    from BiblioParsing.BiblioSpecificGlobals import FIELD_SIZE_LIMIT
    from BiblioParsing.BiblioSpecificGlobals import UNKNOWN
    from BiblioParsing.BiblioSpecificGlobals import WOS
    from BiblioParsing.BiblioSpecificGlobals import WOS_RAWDATA_EXTENT
    
    # Check if rawdata file is available and get its full path if it is
    rawdata_file_path = check_and_get_rawdata_file_path(rawdata_path, WOS_RAWDATA_EXTENT)

    if rawdata_file_path: 
        # Extending the field size limit for reading .txt files
        csv.field_size_limit(FIELD_SIZE_LIMIT)

        with open(rawdata_file_path ,'rt',encoding = ENCODING) as csv_file: 
            csv_reader = csv.reader(csv_file, delimiter = '\t')
            csv_list = []
            for row in csv_reader:
                csv_list.append(row)

        df = pd.DataFrame(csv_list)
        df.columns = df.iloc[0]                  # Sets columns name to raw 0
        df = df.drop(0)                          # Drops the raw 0 from df 
        df = check_and_drop_columns(WOS, df)
        df = df.replace(np.nan, UNKNOWN, regex = True)
        df = normalize_journal_names(WOS, df)
    else:
        df = None          
    return df


def biblio_parser_wos(rawdata_path, inst_filter_list = None,
                      country_affiliations_file_path = None,
                      inst_types_file_path = None):
    
    '''The function `biblio_parser_wos` generates parsing dataframes from the csv file stored in the rawdata folder.    
    The columns USECOLS_WOS of the tsv file xxxx.txt are read and parsed using the functions:
        _build_references_wos which parses the column 'CR'
        _build_authors_wos which parses the column 'AU'
        _build_keywords_wos which parses the column 'ID' (for author keywords AK),
                                        the column 'DE' (for journal keywords IK),
                                        the column 'TI' (for title keywords IK)
        _build_addresses_countries_institutions_wos which parses the column 'C1' by pub_id
        _build_authors_countries_institutions_wos which parses the column 'C1' by authors
        _build_subjects_wos which parses the column 'SC'
        _build_sub_subjects_wos which parses the column 'WC'
        _build_articles_wos which parses the column 'AU', 'PY', 'SO', 'VL', 'BP',
                                                   'DI', 'DT', 'LA', 'TI', 'SN'.
                                                   
    List of parsed items (keys of returned dict which values are the dataframes of the parsing results): 
        - "articles", "authors", "addresses", "countries", 
        - "institutions", "authors_institutions", "raw_institutions"
        - "authors_keywords", "indexed_keywords", "title_keywords", 
        - "subjects", "sub_subjects", "references".
    
    Args:
    
    
    Returns:
    
    
    Note:
    '''
    
    # Standard library imports
    import os
    import json
    from pathlib import Path 
    
    # Local globals imports
    from BiblioParsing.BiblioSpecificGlobals import COL_NAMES
    from BiblioParsing.BiblioSpecificGlobals import PARSING_ITEMS_LIST
    
    # Internal functions    
    def _keeping_item_parsing_results(item, item_df):
        wos_parsing_dict[item] = item_df
    
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
    
    # Reading and checking the raw corpus file
    df_corpus = read_database_wos(rawdata_path)
    
    # Initializing the dic_failed dict for the parsing control
    wos_dic_failed = {}    
    
    # Initializing the dict of dataframes resulting from the parsing
    wos_parsing_dict = {}
    
    if df_corpus is not None:
        
        # Keeping the number of articles in wos_dic_failed dict
        wos_dic_failed['number of article'] = len(df_corpus)
    
        # Building the dataframe of articles
        articles_df = _build_articles_wos(df_corpus)
        _keeping_item_parsing_results(articles_alias, articles_df)    

        # Building the dataframe of authors
        authors_df = _build_authors_wos(df_corpus, wos_dic_failed)
        _keeping_item_parsing_results(authors_alias, authors_df)

        # Building the dataframe of addresses, countries and institutions
        addresses_df, countries_df, institutions_df = _build_addresses_countries_institutions_wos(df_corpus,
                                                                                                  wos_dic_failed)
          # Keeping addresses df
        _keeping_item_parsing_results(addresses_alias, addresses_df)
          # Keeping countries df
        _keeping_item_parsing_results(countries_alias, countries_df)
          # Keeping institutions df
        _keeping_item_parsing_results(institutions_alias, institutions_df)

        # Building the dataframe of authors and their institutions
        auth_inst_df = _build_authors_countries_institutions_wos(df_corpus, wos_dic_failed, 
                                                                 inst_filter_list = inst_filter_list ,
                                                                 country_affiliations_file_path = country_affiliations_file_path,
                                                                 inst_types_file_path = inst_types_file_path)
        _keeping_item_parsing_results(auth_inst_alias, auth_inst_df)

        # Building the dataframes of keywords
        AK_keywords_df, IK_keywords_df, TK_keywords_df = _build_keywords_wos(df_corpus, wos_dic_failed)   
          # Keeping author keywords df
        _keeping_item_parsing_results(authors_kw_alias, AK_keywords_df)
          # Keeping journal (indexed) keywords df
        _keeping_item_parsing_results(index_kw_alias, IK_keywords_df)
          # Keeping title keywords df
        _keeping_item_parsing_results(title_kw_alias, TK_keywords_df)

        # Building the dataframe of subjects
        subjects_df = _build_subjects_wos(df_corpus, wos_dic_failed)
        _keeping_item_parsing_results(subjects_alias, subjects_df)

        # Building the dataframe of sub-subjects
        sub_subjects_df = _build_sub_subjects_wos(df_corpus, wos_dic_failed)
        _keeping_item_parsing_results(sub_subjects_alias, sub_subjects_df)

        # Building the dataframe of references 
        references_df = _build_references_wos(df_corpus)
        _keeping_item_parsing_results(references_alias, references_df)
        
    return wos_parsing_dict, wos_dic_failed