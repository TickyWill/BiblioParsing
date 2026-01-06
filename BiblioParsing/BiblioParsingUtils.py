__all__ = ['build_item_df_from_tup',
           'build_pub_db_ids',
           'build_title_keywords',
           'check_and_drop_columns',
           'check_and_get_rawdata_file_path',
           'clean_authors_countries_institutions',
           'dict_print',
           'normalize_country',
           'normalize_journal_names',
           'normalize_name',
           'rationalize_town_names',
           'remove_special_symbol',
           'set_rawdata_error',
           'set_unknown_address',
           'standardize_address',
           'upgrade_col_names',
           ]


# Standard library imports
import functools
import numpy as np
import operator
import os
import re
import sys
import unicodedata
from collections import Counter
from pathlib import Path


# 3rd party imports
import pandas as pd
import nltk
from pandas.core.groupby.groupby import DataError

# Local library imports
import BiblioParsing.BiblioGeneralGlobals as bp_gg
import BiblioParsing.BiblioRegexpGlobals as bp_rg
import BiblioParsing.BiblioSpecificGlobals as bp_sg


def dict_print(dic):
    for k,v in dic.items():
        print("            ", k, ":", v)


def set_unknown_address(author_idx, add_unknown_country=False):
    """Builds unknown address for an author wich address is unknown.

    Args:
        author_idx (int): Index of the author in the publication's authors list.
        add_unknown_country (bool): If True (default: False), unknown-country key \
        is added to the unknown address.
    Returns:
        (str): The built unknown address.
    """
    if add_unknown_country:
        author_address = f'{author_idx}_{bp_sg.UNKNOWN}, {bp_sg.UNKNOWN_COUNTRY}'
    else:
        author_address = f'{author_idx}_{bp_sg.UNKNOWN}'
    return author_address
        

def check_and_get_rawdata_file_path(rawdata_path, raw_extent):
    """
    """
    # Listing the available files with 'raw_extent' extension
    # ToDo: Management of multiple files to merge with 'merge_database' function
    rawdata_list = []
    for path, _, files in os.walk(rawdata_path):
        rawdata_list.extend(Path(path) / Path(file) for file in files 
                              if file.endswith(raw_extent))                
    if rawdata_list:
        # Selecting the most recent file with raw_extent extension
        rawdata_list.sort(key = lambda x: os.path.getmtime(x), reverse=True)
        rawdata_file_path = rawdata_list[0]
    else:
        rawdata_file_path = None
    return rawdata_file_path


def set_rawdata_error(database, rawdata_path, raw_extent):
    error_text  = f"\n   !!! No {database} raw-data file available !!! \n"
    error_text += f"\nBefore new launch of the cell, "
    error_text += f"please make available a {database} raw-data file "
    error_text += f"with {raw_extent} extension in:\n   {rawdata_path}."
    return error_text


def build_item_df_from_tup(item_list, item_col_names, item_col, pub_id_col, fails_dict=None):
    """Building a clean item dataframe from a tuple 
    and accordingly updating the parsing success rate dict."""
    
    item_df = pd.DataFrame.from_dict({label:[s[idx] for s in item_list] 
                                      for idx,label in enumerate(item_col_names)})
    pub_ids_list = item_df[item_df[item_col]==''][pub_id_col].values
    pub_ids_list = list(set(pub_ids_list))
    if fails_dict:
        corpus_size = fails_dict['number of article']
        fails_dict[item_col] = {'success (%)':100 * ( 1 - len(pub_ids_list) / corpus_size),
                                pub_id_col:[int(x) for x in pub_ids_list]}    
    item_df = item_df[item_df[item_col]!='']
    return item_df, fails_dict


def clean_authors_countries_institutions(auth_addr_country_inst_df):
    """Gathers author's attributes in a single line for each publication.
    """
    # Setting useful column names
    columns_list = auth_addr_country_inst_df.columns
    (pub_id_col, author_col, address_col, country_col,
     norm_aff_col, raw_aff_col) = columns_list[0:6]
    
    new_auth_addr_country_inst_df = pd.DataFrame(columns=columns_list)
    for pub_id, pub_id_dg in auth_addr_country_inst_df.groupby(pub_id_col):
        new_pub_id_dg = pd.DataFrame(columns=columns_list)
        for author_id, author_dg in pub_id_dg.groupby(author_col):
            new_author_dg = author_dg.copy()
            if len(author_dg)>1:
                country_list = list(set(author_dg[country_col].to_list()))
                new_author_dg[country_col] = "; ".join(country_list)
                
                address_list = author_dg[address_col].to_list()
                new_author_dg[address_col] = "; ".join(address_list)
                
                norm_aff_list = list(set(author_dg[norm_aff_col].to_list()) - {bp_sg.EMPTY})
                new_author_dg[norm_aff_col] = "; ".join(norm_aff_list)
                
                raw_aff_list = list(set(author_dg[raw_aff_col].to_list()) - {bp_sg.EMPTY})
                new_author_dg[raw_aff_col] = "; ".join(raw_aff_list)
                
                new_author_dg.drop_duplicates(subset=[pub_id_col, author_col], inplace=True)
                new_pub_id_dg = pd.concat([new_pub_id_dg, new_author_dg])
            else:
                new_pub_id_dg = pd.concat([new_pub_id_dg, author_dg])
        new_auth_addr_country_inst_df = pd.concat([new_auth_addr_country_inst_df, new_pub_id_dg])
    new_auth_addr_country_inst_df.fillna(bp_sg.EMPTY, inplace=True)
    new_auth_addr_country_inst_df.replace("", bp_sg.EMPTY, inplace=True)
    return new_auth_addr_country_inst_df


def build_title_keywords(df):
    """Given the dataframe 'df' with one column 'title':
    
                    Title
            0  Experimental and CFD investigation of inert be...
            1  Impact of Silicon/Graphite Composite Electrode...
            
    the function 'build_title_keywords':
    
       1- Builds the set "keywords_TK" of the tokens appearing at least NOUN_MINIMUM_OCCURRENCE times 
    in all the article titles of the corpus. The tokens are the words of the title with nltk tags 
    belonging to the global list 'NLTK_VALID_TAG_LIST'.
       2- Adds two columns 'token' and 'pub_token' to the dataframe 'df'. The column 'token' contains
    the set of the tokenized and lemmelized (using the nltk WordNetLemmatizer) title. The column
    'pub_token' contains the list of words common to the set "keywords_TK" and to the column 'kept_tokens'
       3- Builds the list of tuples 'list_of_words_occurrences.sort'
    [(token_1,# occurrences token_1), (token_2,# occurrences token_2),...] ordered by decreasing values
    of # occurrences token_i.
       4- Suppress words pertening to BLACKLISTED_WORDS to the list from the bag of words
    
    Args:
       df (dataframe): Data of publication title per publication identifier.

    Returns:
       (tup): tuple (df, bag_of_words_occurrences) with df a dataframe 
       which columns are [pub_id, title_tokens_alias, kept_tokens_alias]  
       where title_tokens_alias contains the list of tokens of the title 
       and kept_tokens_alias the list of tokens with an occurrence frequency 
       >= NOUN_MINIMUM_OCCURRENCES, and bag_of_words_occurrences a list of tuples
       where tuple i is (word_i,# occurrence_i). 
    """
    # To Do: update docstring
    
    def tokenizer(text):
        """Tokenizes, lemmelizes the string 'text'. Only the words with nltk tags in the global
        NLTK_VALID_TAG_LIST are kept.
        
        ex 'Thermal stability of Mg2Si0.55Sn0.45 for thermoelectric applications' 
        gives the list : ['thermal', 'stability', 'mg2si0.55sn0.45', 'thermoelectric', 'application']
        
        Args:
            text (string): String to tokenize
        Returns
            (list) : The tokenized and lemmatized words.
        """
        tokenized = nltk.word_tokenize(text.lower())
        valid_words = [word for (word, pos) in nltk.pos_tag(tokenized) 
                       if pos in bp_sg.NLTK_VALID_TAG_LIST] 

        stemmer = nltk.stem.WordNetLemmatizer()
        valid_words_lemmatized = [stemmer.lemmatize(valid_word) for valid_word in valid_words]    
        return valid_words_lemmatized        

    title_alias = bp_sg.COL_NAMES['temp_col'][2]
    title_tokens_alias = bp_sg.COL_NAMES['temp_col'][3]
    kept_tokens_alias = bp_sg.COL_NAMES['temp_col'][4]
    
    df[title_tokens_alias] = df[title_alias].apply(tokenizer)

    # Removing the blacklisted words from the bag of words
    bag_of_words = np.array(df[title_tokens_alias].sum()) 
    for remove in bp_sg.BLACKLISTED_WORDS:
        bag_of_words = bag_of_words[bag_of_words!=remove] 

    bag_of_words_occurrences = list(Counter(bag_of_words).items())
    bag_of_words_occurrences.sort(key=operator.itemgetter(1), reverse=True)

    title_keywords = set([x for x, y in bag_of_words_occurrences if y>=bp_sg.NOUN_MINIMUM_OCCURRENCES])    
    df[kept_tokens_alias] = df[title_tokens_alias].apply(lambda x :list(title_keywords.intersection(set(x))))
   
    return (df, bag_of_words_occurrences)


def normalize_country(country):
    """Normalizes the country name for coherence seeking between 
    wos and scopus corpuses.
    """
    # To Do: update docstring

    country_clean = country
    if country not in bp_gg.COUNTRIES:
        if country in  bp_gg.ALIAS_UK:
            country_clean = 'United Kingdom'
        elif 'USA' in country:
            country_clean = 'United States'
        elif ('china' in country) or ('China' in country):
            country_clean = 'China'
        elif country=='Russia':    
            country_clean = 'Russian Federation'
        elif country=='U Arab Emirates':    
            country_clean = 'United Arab Emirates'
        elif country=='Vietnam':   
            country_clean = 'Viet Nam'
        elif country=='Palestine':
            country_clean = 'Palestinian Territory'
        elif country in bp_gg.ALIAS_FR:
            country_clean = 'France'
        elif country in bp_gg.ALIAS_BLR:
            country_clean = 'Belarus'
        else:
            country_clean = bp_sg.UNKNOWN_COUNTRY
    return country_clean


def normalize_name(text, drop_ponct=True, lastname_only=False, firstname_only=False):
    """Normalizes the author name spelling according the three debatable rules:
            - replacing none ascii letters by ascii ones,
            - capitalizing first name, 
            - capitalizing surnames,
            - removing comma and dot.
       It uses the internal funtion `remove_special_symbol`of the same module.    
       ex: normalize_name(" GrÔŁ-biçà-vèLU D'aillön, E-kj. ")
        >>> "Grol-Bica-Velu D'Aillon E-KJ".
        
    Args:
        text (str): The name to normalize.    
    Returns
        (str) : The normalized text.        
    Notes:
        The globals 'DASHES_CHANGE', 'LANG_CHAR_CHANGE' and 'PONCT_CHANGE' 
        from `BiblioGeneralGlobals` module are used.
    """
    if "." not in text:
        text_split = text.split(" ")
        text = " ".join([x.capitalize() for x in text_split])
    
    # Translate special character 
    text = text.translate(bp_gg.DASHES_CHANGE)
    text = text.translate(bp_gg.LANG_CHAR_CHANGE)
    if drop_ponct:
        text = text.translate(bp_gg.PONCT_CHANGE)
    
    # Removing accentuated characters
    text = remove_special_symbol(text, only_ascii=True, strip=True)

    # capturing "cCc-cC-ccc-CCc"       
    re_minus = re.compile('(-[a-zA-Z]+)')       # Captures: "cCc-cC-ccc-CCc"
    for text_minus_texts in re.findall(re_minus, text):
        text = text.replace(text_minus_texts, '-' + text_minus_texts[1:].capitalize())

    # capturing "cCc'cC'ccc'cc'CCc"        
    re_apostrophe = re.compile("('[a-zA-Z]+)")
    for text_minus_texts in re.findall(re_apostrophe, text):
        text = text.replace(text_minus_texts, "'" + text_minus_texts[1:].capitalize())

    # capturing "cCc-"        
    re_minus = re.compile('([a-zA-Z]+-)')
    for text_minus_texts in re.findall(re_minus, text):
        text = text.replace(text_minus_texts, text_minus_texts[:-1].capitalize() + '-')

    # capturing "cCc'"        
    re_apostrophe = re.compile("([a-zA-Z]+')")
    for text_minus_texts in re.findall(re_apostrophe, text):
        text = text.replace(text_minus_texts, text_minus_texts[:-1].capitalize() + "'")

    # capturing "cCccC "
    re_surname = re.compile("[a-zA-Z]+\s")
    for text_minus_texts in re.findall(re_surname, text):
        text = text.replace(text_minus_texts, text_minus_texts.capitalize())

    if not lastname_only:
        # Capturing " cCc-cC" in the first name
        re_minus_first_name = re.compile('\s[a-zA-Z]+-[a-zA-Z]+$')
        for x in  re.findall(re_minus_first_name, text):
            text = text.replace(x, x.upper())
        if firstname_only:
            # Capturing "cCc-cC " or " cCccC." in the first name
            re_minus_first_name = re.compile('[a-zA-Z]+-[a-zA-Z]+\.$|\s[a-zA-Z]+\.$')
            for x in  re.findall(re_minus_first_name, text):
                text = text.replace(x, x.upper())

    # Capturing "Mc" in name
    re_mac = re.compile('^Mc[a-zA-Z]')
    for text_mac_texts in re.findall(re_mac, text):
        new_text_mac_texts = "Mc" + text_mac_texts[2:].capitalize()
        text = text.replace(text_mac_texts, new_text_mac_texts)
      
    return text


def normalize_journal_names(database, corpus_df):
    """Adds the column `normalize_journal_names` to the corpus. 

    The journal normalized names are expurgated from unecessary
	pieces of information such as: small words defined in a global 
    dict (`DIC_LOW_WORDS`), year, conference edition... 
    These normalized and simplified journal names are mainly used 
    when concatenating two corpus (wos, scopus, ...) using slightly
    different name for the same journal.
   
    Args:
        database (string): Type of data among the ones defined \
        by SCOPUS and WOS globals.
        corpus_df (dataframe): corpus dataframe to be normalized \
        in terms of journal names.
       
    Returns:
        (dataframe): The data with an additional column containing \
        the normalized journal names.
   """
    def _journal_normalizer(journal):
        # Adding a lazzy trick to simplefy the regexp
        journal = ' ' + journal + ' '
        journal = journal.lower()
        journal = re.sub(bp_rg.RE_YEAR_JOURNAL, ' ', journal)
        journal = re.sub(bp_rg.RE_NUM_CONF, ' ', journal)
        for old_str, new_str in bp_sg.DIC_LOW_WORDS.items():
            journal = journal.replace(old_str, new_str)
        journal = re.sub('\s+', ' ', journal)
        journal = journal.strip()
        return journal

    if database==bp_sg.WOS:
        journal_alias = bp_sg.COLUMN_LABEL_WOS['journal']
    elif database==bp_sg.SCOPUS:
        journal_alias = bp_sg.COLUMN_LABEL_SCOPUS['journal']
    else:
        raise Exception(f"Sorry, unrecognized database {database}: "
                        f"should be {bp_sg.WOS} or {bp_sg.SCOPUS} ")
    
    norm_journal_alias = bp_sg.NORM_JOURNAL_COLUMN_LABEL
    corpus_df[norm_journal_alias] = corpus_df[journal_alias].apply(_journal_normalizer)
    
    return corpus_df


def build_pub_db_ids(rawdata_df, init_db_id_col, db_id_col):

    # Setting useful aliases
    pub_id_col_alias = bp_sg.COL_NAMES['pub_id']
    
    # Setting the pub_id in rawdata_df index
    rawdata_df.index = range(len(rawdata_df))

    # Setting the pub-id as a column
    rawdata_df = rawdata_df.rename_axis(pub_id_col_alias).reset_index()

    # Building the final data
    init_db_ids_df = rawdata_df[[init_db_id_col, pub_id_col_alias]]
    db_ids_df = init_db_ids_df.rename(columns={init_db_id_col: db_id_col})
    return db_ids_df

        
def check_and_drop_columns(database, init_df):
    
    df = init_df.copy()
    
    # Setting useful aliases
    pub_id_col_alias    = bp_sg.COL_NAMES["pub_id"]
    wos_col_issn_alias  = bp_sg.COLUMN_LABEL_WOS["issn"]
    wos_col_eissn_alias = bp_sg.COLUMN_LABEL_WOS_PLUS["e_issn"] 

    # Check for missing mandatory columns
    if database==bp_sg.WOS:
        cols_mandatory = set([val for val in bp_sg.COLUMN_LABEL_WOS.values() if val] + [wos_col_eissn_alias])
    elif database==bp_sg.SCOPUS:
        cols_mandatory = set([val for val in bp_sg.COLUMN_LABEL_SCOPUS.values() if val])    
    else:
        raise Exception(f"Sorry, unrecognized database {database} : should be {bp_sg.WOS} or {bp_sg.SCOPUS} ")
        
    cols_available = set(df.columns)
    missing_columns = cols_mandatory.difference(cols_available)
    if missing_columns:
        error_text  = f'The mandarory columns: {",".join(missing_columns)} are missing '
        error_text += f'in rawdata extracted from {database}.\nPlease correct before proceeding.'
        raise Exception(error_text)
    
    # Setting issn to e_issn if issn not available for wos
    if database==bp_sg.WOS:
        df = df.replace('',np.nan,regex=True) # To allow the use of combine_first
        df[wos_col_issn_alias] = df[wos_col_issn_alias].combine_first(df[wos_col_eissn_alias])
        df = df.dropna(axis = 0, how = 'all')
        cols_mandatory = set([val for val in bp_sg.COLUMN_LABEL_WOS.values() if val])
        
        
    # Droping unused columns    
    cols_to_drop = list(cols_available.difference(cols_mandatory))
    df.drop(cols_to_drop, axis=1, inplace=True)

    # Setting publication identifier in a column of the data
    df.index = range(len(df))
    df = df.rename_axis(pub_id_col_alias).reset_index()
    return df

                    
def upgrade_col_names(corpus_folder):
    """Add names to the colummn of the parsing and filter_<i> files to take into account the
    upgrage of BiblioParsing package.
    
    Args:
        corpus_folder (str): folder of the corpus to be adapted
    """
    # Beware: the new file authorsinst.dat is not present in the old parsing folders
    dict_filename_conversion  = {'addresses.dat'      : 'address',
                                 'articles.dat'       : 'articles',
                                 'authors.dat'        : 'authors',
                                 'authorsinst.dat'    : 'auth_inst',
                                 'authorskeywords.dat': 'keywords',
                                 'countries.dat'      : 'country',
                                 'institutions.dat'   : 'institution',
                                 'journalkeywords.dat': 'keywords',
                                 'references.dat'     : 'references',
                                 'subjects.dat'       : 'subject',
                                 'subjects2.dat'      : 'sub_subject',
                                 'titlekeywords.dat'  : 'keywords'}

    for dirpath, dirs, files in os.walk(corpus_folder):  
        if ('parsing' in   dirpath) |  ('filter_' in  dirpath):
            for file in  [file for file in files
                          if (file.split('.')[1]=='dat') 
                          and (file!='database.dat')      # Unused this file is no longer generated
                          and (file!='keywords.dat') ]:   # Unused this file is no longer generated
                try:
                    df = pd.read_csv(os.path.join(dirpath, file), sep='\t', header=None)
                    
                    if df.loc[0].tolist()==bp_sg.COL_NAMES[dict_filename_conversion[file]]:
                        print(f'The file {os.path.join(dirpath,file)} is up to date')
                    else:
                        df.columns = bp_sg.COL_NAMES[dict_filename_conversion[file]]
                        df.to_csv(os.path.join(dirpath,file), sep='\t', index=False)
                        print(f'*** The file {os.path.join(dirpath,file)} has been upgraded ***')
                except  pd.errors.EmptyDataError:
                    df = pd.DataFrame(columns=bp_sg.COL_NAMES[dict_filename_conversion[file]])
                    df.to_csv(os.path.join(dirpath, file), sep='\t', index=False)
                    print(f'*** The EMPTY file {os.path.join(dirpath,file)} has been upgraded ***')
                except:
                    print(f'Warning: File {os.path.join(dirpath,file)} not recognized as a parsing file')


def rationalize_town_names(text, dic_town_symbols=None, dic_town_words=None):
    """Replaces in the string 'text' symbols and words defined by the keys 
    of the dictionaries 'DIC_TOWN_SYMBOLS' and 'DIC_TOWN_WORDS' by their 
    corresponding values in these dictionaries.

    Args:
        text (str): The string where changes will be done.
    Returns:
        (str): The modified string.
    """    
    if dic_town_symbols==None:   
        dic_town_symbols = bp_sg.DIC_TOWN_SYMBOLS
    if dic_town_words==None:
        dic_town_words = bp_sg.DIC_TOWN_WORDS
    
    # Uniformizing symbols in town names using the dict 'DIC_TOWN_SYMBOLS'
    for town_symb in dic_town_symbols.keys():
        text = text.replace(town_symb, dic_town_symbols[town_symb])

    # Uniformizing words in town names using the dict 'DIC_TOWN_WORDS'
    for town_word in dic_town_words.keys():
        text = text.replace(town_word, dic_town_words[town_word])    
    return text


def remove_special_symbol(text, only_ascii=True, strip=True):
    """The function `remove_special_symbol` removes accentuated characters in the string 'text'
    and ignore non-ascii characters if 'only_ascii' is true.
    
    Finally, spaces at the ends of 'text' are removed if strip is true.
    
    Args:
        text (str): The text where to remove special symbols.
        only_ascii (boolean): If True, non-ascii characters are removed from 'text' (default: True).
        strip (boolean): If True, spaces at the ends of 'text' are removed (default: True).
    Returns:
        (str): The modified string 'text'.
    """
    if only_ascii:
        nfc = functools.partial(unicodedata.normalize,'NFD')
        text = nfc(text). \
                   encode('ascii', 'ignore'). \
                   decode('utf-8')
    else:
        nfkd_form = unicodedata.normalize('NFKD',text)
        text = ''.join([c for c in nfkd_form if not unicodedata.combining(c)])

    if strip:
        text = text.strip()
    return text


def standardize_address(raw_address, add_unknown_country=True):
    """Standardizes the string 'raw_address' by replacing all aliases of a word, 
    such as 'University', 'Institute', 'Center' and' Department', by a standardized 
    version.

    The aliases of a given word are captured using a specific regex which is case sensitive defined 
    by the global 'DIC_WORD_RE_PATTERN' imported from the `BiblioParsing` package imported as "bp". 
    The aliases may contain symbols from a given list of any language including accentuated ones. 
    The length of the aliases is limited to a maximum according to the longest alias known.
        ex: The longest alias known for the word 'University' is 'Universidade'. 
            Thus, 'University' aliases are limited to 12 symbols beginning with the base 'Univ' 
            with possibly before one symbol among a to z and after up to 8 symbols from the list 
            '[aàäcdeéirstyz]' and possibly finishing with a dot. 
    Then, dashes are replaced by a hyphen-minus using 'DASHES_CHANGE' global and apostrophes are replaced 
    by the standard cote using 'APOSTROPHE_CHANGE' global. 
    The globals are imported from the `BiblioParsing` package imported as "bp". 
    Finally, the country is normalized through the `normalize_country` function of the same module.

    Args:
        raw_address (str): The full address to be standardized.
        add_unknown_country (bool): If False (default: True), unknown-country key is not added \
        to the standardized address.
    Returns:
        (str): The full standardized address.
    """
    # Uniformizing words
    standard_address = remove_special_symbol(raw_address)
    for word_to_substitute, re_pattern in bp_sg.DIC_WORD_RE_PATTERN.items():
        if word_to_substitute=='University':
            re_pattern = re.compile(r'\b[a-z]?Univ[aàäcdeéirstyz]{0,8}\b\.?')
        standard_address = re.sub(re_pattern, word_to_substitute + ' ', standard_address)
    standard_address = re.sub(r'\s+', ' ', standard_address)
    standard_address = re.sub(r'\s,', ',', standard_address)

    # Uniformizing dashes
    standard_address = standard_address.translate(bp_gg.DASHES_CHANGE)

    # Uniformizing apostrophes
    standard_address = standard_address.translate(bp_gg.APOSTROPHE_CHANGE)

    # Dropping symbols
    standard_address = standard_address.translate(bp_gg.SYMB_DROP)

    # Uniformizing countries
    country_pos = -1
    first_raw_affiliations_list = standard_address.split(',')
    # This split below is just for country finding even if affiliation may be separated by dashes
    raw_affiliations_list = sum([x.split(' - ') for x in first_raw_affiliations_list], [])
    country = normalize_country(raw_affiliations_list[country_pos].strip())
    country_chunck = " " + country
    if country==bp_sg.UNKNOWN_COUNTRY:
        if add_unknown_country:   
            standard_address = ','.join(first_raw_affiliations_list + [country_chunck])
        else:
            standard_address = ','.join(first_raw_affiliations_list)
    else:
        standard_address = ','.join(first_raw_affiliations_list[:-1] + [country_chunck])
    
    return standard_address
