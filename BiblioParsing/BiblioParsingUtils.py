__all__ = ['accent_remove',                 # To remove after calls check in modules
           'biblio_parser',
           'build_title_keywords',
           'check_and_drop_columns',
           'check_and_get_rawdata_file_path',
           'country_normalization',         # To remove after replace by 'normalize_country' when called in modules
           'merge_database',
           'name_normalizer',
           'normalize_country',
           'normalize_journal_names',
           'rationalize_town_names',
           'read_towns_per_country',
           'remove_special_symbol',
           'set_rawdata_error',
           'special_symbol_remove',         # To remove after replace by 'remove_special_symbol' when called in modules (done in this module BiblioParsingWos and BiblioParsingScopus)
           'town_names_uniformization',     # To remove after replace  by 'rationalize_town_names' when called in modules
           'upgrade_col_names',
           ]


# Globals used from BiblioParsing.BiblioGeneralGlobals:  ALIAS_UK, COUNTRIES, 
#                                                               DASHES_CHANGE, LANG_CHAR_CHANGE, PONCT_CHANGE, SYMB_CHANGE  
# Globals used from BiblioParsing.BiblioSpecificGlobals: BLACKLISTED_WORDS, COL_NAMES,
#                                                               DIC_LOW_WORDS,
#                                                               DIC_TOWN_SYMBOLS, DIC_TOWN_WORDS,
#                                                               INST_FILTER_LIST, REP_UTILS, 
#                                                               NLTK_VALID_TAG_LIST, NOUN_MINIMUM_OCCURRENCES,
#                                                               RE_NUM_CONF,RE_YEAR_JOURNAL,
#                                                               SCOPUS, USECOLS_SCOPUS, UNKNOWN, WOS

# Functions used from BiblioParsing.BiblioParsingScopus: biblio_parser_scopus
# Functions used from BiblioParsing.BiblioParsingWos: biblio_parser_wos


def check_and_get_rawdata_file_path(rawdata_path, raw_extent):
    '''
    '''
    # Standard library imports
    import os
    from pathlib import Path
    
    # Listing the available files with raw_extent extension
    # ToDo: Management of multiple files to merge with 'merge_database' function
    list_data_base = []
    for path, _, files in os.walk(rawdata_path):
        list_data_base.extend(Path(path) / Path(file) for file in files 
                              if file.endswith(raw_extent))
    if list_data_base:
        # Selecting the first file with raw_extent extension
        rawdata_file_path = list_data_base[0]
    else:
        rawdata_file_path = None
    return rawdata_file_path


def set_rawdata_error(database, rawdata_path, raw_extent):
    error_text  = f"\n   !!! No {database} raw-data file available !!! \n"
    error_text += f"\nBefore new launch of the cell, "
    error_text += f"please make available a {database} raw-data file "
    error_text += f"with {raw_extent} extension in:\n   {rawdata_path}."
    return error_text

def build_title_keywords(df):
    
    '''Given the dataframe 'df' with one column 'title':
    
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
       3- Buids the list of tuples 'list_of_words_occurrences.sort'
    [(token_1,# occurrences token_1), (token_2,# occurrences token_2),...] ordered by decreasing values
    of # occurrences token_i.
       4- Suppress words pertening to BLACKLISTED_WORDS to the list  from the bag of words
    
    Args:
       df (dataframe): pub_id | title_alias 

    Returns:
       df (dataframe): pub_id | title_tokens_alias | kept_tokens_alias where title_tokens is the list of tokens of the title
         and kept_tokens the list of tokens with a frequency occurrence >= NOUN_MINIMUM_OCCURRENCES
       bag_of_words_occurrences (list of tuples): [(word_1,# occurrence_1), (word_2,# occurrence_2), ...]
        
    '''
    # To Do: update docstring
    
    # Standard library imports
    import operator
    from collections import Counter
       
    # 3rd party library imports
    import nltk
    import numpy as np
    
    # Globals imports
    from BiblioParsing.BiblioSpecificGlobals import BLACKLISTED_WORDS
    from BiblioParsing.BiblioSpecificGlobals import COL_NAMES
    from BiblioParsing.BiblioSpecificGlobals import NLTK_VALID_TAG_LIST
    from BiblioParsing.BiblioSpecificGlobals import NOUN_MINIMUM_OCCURRENCES
    
    def tokenizer(text):
        
        '''
        Tokenizes, lemmelizes the string 'text'. Only the words with nltk tags in the global
        NLTK_VALID_TAG_LIST are kept.
        
        ex 'Thermal stability of Mg2Si0.55Sn0.45 for thermoelectric applications' 
        gives the list : ['thermal', 'stability', 'mg2si0.55sn0.45', 'thermoelectric', 'application']
        
        Args:
            text (string): string to tokenize
            
        Returns
            The list valid_words_lemmatized 
        '''
            
        tokenized = nltk.word_tokenize(text.lower())
        valid_words = [word for (word, pos) in nltk.pos_tag(tokenized) 
                       if pos in NLTK_VALID_TAG_LIST] 

        stemmer = nltk.stem.WordNetLemmatizer()
        valid_words_lemmatized = [stemmer.lemmatize(valid_word) for valid_word in valid_words]
    
        return valid_words_lemmatized        

    title_alias = COL_NAMES['temp_col'][2]
    title_tokens_alias = COL_NAMES['temp_col'][3]
    kept_tokens_alias = COL_NAMES['temp_col'][4]
    
    df[title_tokens_alias] = df[title_alias].apply(tokenizer)

    bag_of_words = np.array(df[title_tokens_alias].sum()) # remove the blacklisted words from the bag of words
    for remove in BLACKLISTED_WORDS:
        bag_of_words = bag_of_words[bag_of_words != remove] 

    bag_of_words_occurrences = list(Counter(bag_of_words).items())
    bag_of_words_occurrences.sort(key = operator.itemgetter(1),reverse=True)

    keywords_TK = set([x for x,y in bag_of_words_occurrences if y>=NOUN_MINIMUM_OCCURRENCES])
    
    df[kept_tokens_alias] = df[title_tokens_alias].apply(lambda x :list(keywords_TK.intersection(set(x))))
   
    return df,bag_of_words_occurrences


def normalize_country(country):
    
    '''
    Normalize the country name for coherence seeking between wos and scopus corpuses.
    '''
    # To Do: update docstring
    
    # Globals imports
    from BiblioParsing.BiblioGeneralGlobals import ALIAS_UK
    from BiblioParsing.BiblioGeneralGlobals import COUNTRIES
    from BiblioParsing.BiblioSpecificGlobals import UNKNOWN
    
    country_clean = country
    if country not in COUNTRIES:
        if country in  ALIAS_UK:
            country_clean = 'United Kingdom'
        elif 'USA' in country:
            country_clean = 'United States'
        elif ('china' in country) or ('China' in country):
            country_clean = 'China'
        elif country == 'Russia':    
            country_clean = 'Russian Federation'
        elif country == 'U Arab Emirates':    
            country_clean = 'United Arab Emirates'
        elif country == 'Vietnam':   
            country_clean = 'Viet Nam'
        else:
            country_clean = UNKNOWN

    return country_clean


def merge_database(database,filename,in_dir,out_dir):
    
    '''The `merge_database` function merges several corpus of same database type in one corpus.
    
    Args:
        database (str): database type (scopus or wos).
        filename (str): name of the merged database.
        in_dir (str): name of the folder where the corpuses are saved.
        out_dir (str): name of the folder where the merged corpuses will be saved.
    
    Notes:
        The globals 'SCOPUS' and 'WOS' from `BiblioSpecificGlobals`module 
        of `BiblioParsing`package are used.
        The functions 'read_database_scopus' and 'read_database_wos' 
        from, respectively, `BiblioParsingScopus`module and `BiblioParsingWos` 
        of `BiblioParsing`package are used. 
        
    '''
    # Standard library imports
    import os
    import sys
    from pathlib import Path   

    # 3rd party library imports
    import pandas as pd
    
    # Local library imports
    from BiblioParsing.BiblioParsingScopus import read_database_scopus
    from BiblioParsing.BiblioParsingWos import read_database_wos
    
    # Globals imports
    from BiblioParsing.BiblioSpecificGlobals import SCOPUS
    #from BiblioParsing.BiblioSpecificGlobals import USECOLS_SCOPUS       !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    from BiblioParsing.BiblioSpecificGlobals import WOS

    list_data_base = []
    list_df = []
    if database == WOS:
        for path, _, files in os.walk(in_dir):
            list_data_base.extend(Path(path) / Path(file) for file in files
                                                          if file.endswith(".txt"))
        for file in list_data_base:
            list_df.append(read_database_wos(file))

    elif database == SCOPUS:
        for path, _, files in os.walk(in_dir):
            list_data_base.extend(Path(path) / Path(file) for file in files
                                                          if file.endswith(".csv"))
        for file in list_data_base:
            #df = pd.read_csv(file,usecols=USECOLS_SCOPUS) # reads the database     !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            #list_df.append(df)                                                     !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! 
            list_df.append(read_database_scopus(file))
    else:
        raise Exception(f"Sorry, unrecognized database {database} : should be {WOS} or {SCOPUS} ")
        
    result = pd.concat(list_df,ignore_index=True)
    result.to_csv(out_dir / Path(filename),sep='\t')

def name_normalizer(text):
    
    '''The `name_normalizer` function normalizes the author name spelling according the three debatable rules:
            - replacing none ascii letters by ascii ones,
            - capitalizing first name, 
            - capitalizing surnames,
            - supressing comma and dot.
       It uses the internal funtion `remove_special_symbol`of this module "BiblioParsingUtils".    
       ex: name_normalizer(" GrÔŁ-biçà-vèLU D'aillön, E-kj. ")
        >>> "Grol-Bica-Velu D'Aillon E-KJ".
        
    Args:
        text (str): the text to normalize.
    
    Returns
        (str) : The normalized text.
        
    Notes:
        The globals 'DASHES_CHANGE', 'LANG_CHAR_CHANGE' and 'PONCT_CHANGE' 
        from `BiblioGeneralGlobals` module of `BiblioParsing` package are used.
        
    '''

    # Standard library imports
    import re
    
    # Globals imports
    from BiblioParsing.BiblioGeneralGlobals import DASHES_CHANGE    
    from BiblioParsing.BiblioGeneralGlobals import LANG_CHAR_CHANGE
    from BiblioParsing.BiblioGeneralGlobals import PONCT_CHANGE
    
    # Translate special character 
    text = text.translate(DASHES_CHANGE)
    text = text.translate(LANG_CHAR_CHANGE)
    text = text.translate(PONCT_CHANGE)
    
    # Removing accentuated characters
    text = remove_special_symbol(text, only_ascii=True, strip=True)
    
    re_minus = re.compile('(-[a-zA-Z]+)')       # Captures: "cCc-cC-ccc-CCc"
    for text_minus_texts in re.findall(re_minus,text):
        text = text.replace(text_minus_texts,'-' + text_minus_texts[1:].capitalize() )
    
    re_apostrophe = re.compile("('[a-zA-Z]+)")  # Captures: "cCc'cC'ccc'cc'CCc"
    for text_minus_texts in re.findall(re_apostrophe,text):
        text = text.replace(text_minus_texts,"'" + text_minus_texts[1:].capitalize() )
        
    re_minus = re.compile('([a-zA-Z]+-)')       # Captures: "cCc-" 
    for text_minus_texts in re.findall(re_minus,text):
        text = text.replace(text_minus_texts,text_minus_texts[:-1].capitalize() + '-')
        
    re_apostrophe = re.compile("([a-zA-Z]+')")  # Captures: "cCc'"
    for text_minus_texts in re.findall(re_apostrophe,text):
        text = text.replace(text_minus_texts,text_minus_texts[:-1].capitalize() + "'")
        
    re_surname = "[a-zA-Z]+\s"                  # Captures: "cCccC "
    for text_minus_texts in re.findall(re_surname,text):
        text = text.replace(text_minus_texts,text_minus_texts.capitalize())
        
    re_minus_first_name = '\s[a-zA-Z]+-[a-zA-Z]+$'     # Captures: "cCc-cC" in the first name
    for x in  re.findall(re_minus_first_name,text):
        text = text.replace(x,x.upper())
           
    return text


def normalize_journal_names(database,corpus_df):
    '''Tadds the column `normalize_journal_names` to the corpus `corpus_df`. The journal normalized names are expurgated from unecessary
	pieces of information such as : small words defined in a global dict (`DIC_LOW_WORDS`), year, conference edition... These normalized
	and simplified journal names are mainly uses when concatenating two corpus (wos, scpus, ...) using slightly different name for the same
	journal.
    globals.
   
    Args:
        database (string): type of database among the ones defined by SCOPUS and WOS globals.
        corpus_df (dataframe): corpus dataframe to be normalized in terms of journal names.
       
    Returns:
        (dataframe): the dataframe with an additional columns containing the normalized journal names.
       
    Note:
        The globals 'COLUMN_LABEL_WOS', 'COLUMN_LABEL_SCOPUS','DIC_LOW_WORDS', 'RE_YEAR_JOURNAL', 'SCOPUS' and 'WOS' are used.
   
    '''
   
    # Globals imports
    from BiblioParsing.BiblioRegexpGlobals import RE_NUM_CONF
    from BiblioParsing.BiblioRegexpGlobals import RE_YEAR_JOURNAL
    from BiblioParsing.BiblioSpecificGlobals import COLUMN_LABEL_WOS
    from BiblioParsing.BiblioSpecificGlobals import COLUMN_LABEL_SCOPUS
    from BiblioParsing.BiblioSpecificGlobals import DIC_LOW_WORDS
    from BiblioParsing.BiblioSpecificGlobals import NORM_JOURNAL_COLUMN_LABEL    
    from BiblioParsing.BiblioSpecificGlobals import SCOPUS
    from BiblioParsing.BiblioSpecificGlobals import WOS
   
   
    import re
    def _journal_normalizer(journal):
        journal = ' ' + journal + ' ' # a lazzy trick to simplefy the regexp
        journal = journal.lower()
        journal = re.sub(RE_YEAR_JOURNAL, ' ', journal)
        journal = re.sub(RE_NUM_CONF, ' ', journal)
        for old_str, new_str in DIC_LOW_WORDS.items():
            journal = journal.replace(old_str, new_str)
        journal = re.sub('\s+',' ',journal)
        journal = journal.strip()
        return journal

    if database == WOS:
        journal_alias = COLUMN_LABEL_WOS['journal']
    elif database == SCOPUS:
        journal_alias = COLUMN_LABEL_SCOPUS['journal']
    else:
        raise Exception(f"Sorry, unrecognized database {database}: should be {WOS} or {SCOPUS} ")
    
    norm_journal_alias = NORM_JOURNAL_COLUMN_LABEL
    corpus_df[norm_journal_alias] = corpus_df[journal_alias].apply(_journal_normalizer)
    
    return corpus_df


def biblio_parser(rawdata_path, database, inst_filter_list = None):
    
    '''The `biblio_parser` function parse wos or scopus databases using the appropriate parser.
    
    Args:
    
    
    Returns:
        (dict): The dict of dataframes resulting from the parsing.
    '''
    
    # Local library imports
    from BiblioParsing.BiblioParsingScopus import biblio_parser_scopus
    from BiblioParsing.BiblioParsingWos import biblio_parser_wos
    
    # Globals imports
    from BiblioParsing.BiblioSpecificGlobals import SCOPUS
    from BiblioParsing.BiblioSpecificGlobals import WOS
    
    if database == WOS:
        parsing_dict, dic_failed = biblio_parser_wos(rawdata_path, inst_filter_list)
    elif database == SCOPUS:
        parsing_dict, dic_failed = biblio_parser_scopus(rawdata_path, inst_filter_list)
    else:
        raise Exception(f"Sorry, unrecognized database {database} : should be wos or scopus ")
        
    return parsing_dict, dic_failed

        
def check_and_drop_columns(database, df):
    # Standard libraries import
    import numpy as np
    
    # Local imports
    from BiblioParsing.BiblioSpecificGlobals import COL_NAMES
    from BiblioParsing.BiblioSpecificGlobals import COLUMN_LABEL_SCOPUS
    from BiblioParsing.BiblioSpecificGlobals import COLUMN_LABEL_WOS 
    from BiblioParsing.BiblioSpecificGlobals import COLUMN_LABEL_WOS_PLUS    
    from BiblioParsing.BiblioSpecificGlobals import SCOPUS
    from BiblioParsing.BiblioSpecificGlobals import WOS
    
    
    # Setting useful aliases
    wos_col_issn_alias  = COLUMN_LABEL_WOS["issn"]
    wos_col_eissn_alias = COLUMN_LABEL_WOS_PLUS["e_issn"]
    pub_id_col_alias    = COL_NAMES["pub_id"] 

    # Check for missing mandatory columns
    if database == WOS:
        cols_mandatory = set([val for val in COLUMN_LABEL_WOS.values() if val] + [COLUMN_LABEL_WOS_PLUS["e_issn"]])
    elif database == SCOPUS:
        cols_mandatory = set([val for val in COLUMN_LABEL_SCOPUS.values() if val])    
    else:
        raise Exception(f"Sorry, unrecognized database {database} : should be {WOS} or {SCOPUS} ")
        
    cols_available = set(df.columns)
    missing_columns = cols_mandatory.difference(cols_available)
    if missing_columns:
        error_text  = f'The mandarory columns: {",".join(missing_columns)} are missing '
        error_text += f'in rawdata extracted from {database}.\nPlease correct before proceeding.'
        raise Exception(error_text)
    
    # Setting issn to e_issn if issn not available for wos
    if database == WOS:
        df = df.replace('',np.nan,regex=True) # To allow the use of combine_first
        df[wos_col_issn_alias] = df[wos_col_issn_alias].combine_first(df[wos_col_eissn_alias])
        df = df.dropna(axis = 0, how = 'all')
        cols_mandatory = set([val for val in COLUMN_LABEL_WOS.values() if val])
        
        
    # Columns selection and dataframe reformatting    
    cols_to_drop = list(cols_available.difference(cols_mandatory))
    df.drop(cols_to_drop,
            axis=1,
            inplace=True)                    # Drops unused columns    
    df.index = range(len(df))                # Sets the pub_id in df index
    df = df.rename_axis(pub_id_col_alias).reset_index() # Sets the pub-id as a column
    return df

                    
def upgrade_col_names(corpus_folder):
    
    '''Add names to the colummn of the parsing and filter_<i> files to take into account the
    upgrage of BiblioParsing.
    
    Args:
        corpus_folder (str): folder of the corpus to be adapted
        
    Notes:
        The global 'COL_NAMES' from `BiblioSpecificGlobals` module 
        of `BiblioParsing` package are used.
    
    '''
    # Standard library imports
    import os
    
    # 3rd party library imports
    import pandas as pd
    from pandas.core.groupby.groupby import DataError
    
    # Local imports
    from BiblioParsing.BiblioSpecificGlobals import COL_NAMES
    
    # Beware: the new file authorsinst.dat is not present in the old parsing folders
    dict_filename_conversion  = {'addresses.dat':'address',
                                'articles.dat': 'articles',
                                'authors.dat':'authors',
                                'authorsinst.dat':'auth_inst',
                                'authorskeywords.dat':'keywords',
                                'countries.dat':'country',
                                'institutions.dat':'institution',
                                'journalkeywords.dat':'keywords',
                                'references.dat':'references',
                                'subjects.dat': 'subject',
                                'subjects2.dat':'sub_subject',
                                'titlekeywords.dat':'keywords'}

    for dirpath, dirs, files in os.walk(corpus_folder):  
        if ('parsing' in   dirpath) |  ('filter_' in  dirpath):
            for file in  [file for file in files
                          if (file.split('.')[1]=='dat') 
                          and (file!='database.dat')      # Not used this file is no longer generated
                          and (file!='keywords.dat') ]:   # Not used this file is no longer generated
                try:
                    df = pd.read_csv(os.path.join(dirpath,file),sep='\t',header=None)
                    
                    if df.loc[0].tolist() == COL_NAMES[dict_filename_conversion[file]]:
                        print(f'The file {os.path.join(dirpath,file)} is up to date')
                    else:
                        df.columns = COL_NAMES[dict_filename_conversion[file]]
                        df.to_csv(os.path.join(dirpath,file),sep='\t',index=False)
                        print(f'*** The file {os.path.join(dirpath,file)} has been upgraded ***')
                except  pd.errors.EmptyDataError:
                    df = pd.DataFrame(columns=COL_NAMES[dict_filename_conversion[file]])
                    df.to_csv(os.path.join(dirpath,file),sep='\t',index=False)
                    print(f'*** The EMPTY file {os.path.join(dirpath,file)} has been upgraded ***')
                except:
                    print(f'Warning: File {os.path.join(dirpath,file)} not recognized as a parsing file')


def read_towns_per_country(country_towns_file, rep_utils, dic_town_symbols, dic_town_words):
    
    '''The function `read_towns_per_country` builds a list of towns per country.
    It calls the functions `rationalize_town_names`and `remove_special_symbol`
    defined in the same module 'BiblioParsingUtils' of the package 'BiblioParsing'.
    
    Args:
        country_towns_file (str): File name of the list of towns per country.
        rep_utils (str): Folder of the file named 'country_towns_file'
        
    Returns:
        (list): list of towns.
        
    Notes:
        The data are extracted from the excel file 'country_towns' which is located in the folder
        `rep_utils` of the package `BiblioParsing`.
        
    '''
    # Standard library imports
    from pathlib import Path

    # 3rd party library imports
    import openpyxl
    import pandas as pd

    # Local imports
    import BiblioParsing as bau

    file = Path(bau.__file__).parent / Path(rep_utils) / Path(country_towns_file)
    wb = openpyxl.load_workbook(file)

    dict_df = pd.read_excel(file, 
                            sheet_name=wb.sheetnames)
    towns_dic = {x[0]:x[1]['Town name'].tolist() for x in dict_df.items()}

    for country in towns_dic.keys():
        
        list_towns = []
        for town in towns_dic[country]:
            town = town.lower()
            town = rationalize_town_names(town, dic_town_symbols=dic_town_symbols, dic_town_words= dic_town_words)
            town = remove_special_symbol(town, only_ascii = False, strip = False)
            town = town.strip()
            list_towns.append(town)

        towns_dic[country]= list_towns
    
    return towns_dic

                    
################################# Functions replacing deprecated ones ##########################################
# To Do: 2022-12-08
#    - Update All with functions replacing deprecated ones
#    - Search for calls of deprecated functions in all BiblioParsing modules
#    - Replace calls of deprecated functions by replacing functions and check args, returns and imports including globals

def remove_special_symbol(text, only_ascii = True, strip = True):
    '''The function `remove_special_symbol` removes accentuated characters in the string 'text'
    and ignore non-ascii characters if 'only_ascii' is true. Finally, spaces at the ends of 'text'
    are removed if strip is true.
    
    Args:
        text (str): The text where to remove special symbols.
        only_ascii (boolean): If True, non-ascii characters are removed from 'text' (default: True).
        strip (boolean): If True, spaces at the ends of 'text' are removed (default: True).
        
    Returns:
        (str): The modified string 'text'.
    
    '''
    # Standard library imports
    import functools
    import unicodedata

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


def rationalize_town_names(text, dic_town_symbols=None, dic_town_words=None):
    '''The function `rationalize_town_names` replaces in the string 'text'
    symbols and words defined by the keys of the dictionaries 'DIC_TOWN_SYMBOLS'
    and 'DIC_TOWN_WORDS' by their corresponding values in these dictionaries.
    
    Args:
        text (str): The string where changes will be done.
        
    Returns:
        (str): The modified string.
        
    Notes:
        The globals 'DIC_TOWN_SYMBOLS' and 'DIC_TOWN_WORDS' are imported from
        `BiblioSpecificGlobals` module of `BiblioParsing' package.
    
    '''
    
    if dic_town_symbols==None: 
        # Globals imports
        from BiblioParsing.BiblioSpecificGlobals import DIC_TOWN_SYMBOLS    
        dic_town_symbols=DIC_TOWN_SYMBOLS
    if dic_town_words==None: 
        # Globals imports
        from BiblioParsing.BiblioSpecificGlobals import DIC_TOWN_WORDS
        dic_town_words=DIC_TOWN_WORDS
    
    # Uniformizing symbols in town names using the dict 'DIC_TOWN_SYMBOLS'
    for town_symb in dic_town_symbols.keys():
        text = text.replace(town_symb, dic_town_symbols[town_symb])

    # Uniformizing words in town names using the dict 'DIC_TOWN_WORDS'
    for town_word in dic_town_words.keys():
        text = text.replace(town_word, dic_town_words[town_word])
    
    return text


################################# Deprecated functions ##########################################


def country_normalization(country):                                                                            # Deprecated, replaced by "normalize_country"
    
    '''
    Normalize the country name for coherence seeking between wos and scopus corpuses.
    '''
    # To Do: update docstring
    
    # Globals imports
    from BiblioParsing.BiblioGeneralGlobals import ALIAS_UK
    from BiblioParsing.BiblioGeneralGlobals import COUNTRIES    
    from BiblioParsing.BiblioSpecificGlobals import UNKNOWN
    
    country_clean = country
    if country not in COUNTRIES:
        if country in  ALIAS_UK:
            country_clean = 'United Kingdom'
        elif 'USA' in country:
            country_clean = 'United States'
        elif ('china' in country) or ('China' in country):
            country_clean = 'China'
        elif country == 'Russia':    
            country_clean = 'Russian Federation'
        elif country == 'U Arab Emirates':    
            country_clean = 'United Arab Emirates'
        elif country == 'Vietnam':   
            country_clean = 'Viet Nam'
        else:
            country_clean = UNKNOWN

    return country_clean


def town_names_uniformization(text):                                                                            # Deprecated, replaced by "rationalize_town_names"
    '''the `town_names_uniformization` function replaces in the string 'text'
    symbols and words defined by the keys of the dictionaries 'DIC_TOWN_SYMBOLS'
    and 'DIC_TOWN_WORDS' by their corresponding values in these dictionaries.
    
    Args:
        text (str): The string where changes will be done.
        
    Returns:
        (str): the modified string.
        
    Notes:
        The globals 'DIC_TOWN_SYMBOLS' and 'DIC_TOWN_WORDS' are imported from
        `BiblioSpecificGlobals` module of `BiblioParsing' package.
    
    '''
    # Globals imports
    from BiblioParsing.BiblioSpecificGlobals import DIC_TOWN_SYMBOLS
    from BiblioParsing.BiblioSpecificGlobals import DIC_TOWN_WORDS
    
    # Uniformizing symbols in town names using the dict 'DIC_TOWN_SYMBOLS'
    for town_symb in DIC_TOWN_SYMBOLS.keys():
        text = text.replace(town_symb, DIC_TOWN_SYMBOLS[town_symb])

    # Uniformizing words in town names using the dict 'DIC_TOWN_WORDS'
    for town_word in DIC_TOWN_WORDS.keys():
        text = text.replace(town_word, DIC_TOWN_WORDS[town_word])
    
    return text


def accent_remove(text, strip = True):                                                                                  # Deprecated, replaced by "remove_special_symbol"
    ''' The function `accent_remove` is deprecated and has been replaced by the function `special_symbol_remove` 
     imported from 'BiblioParsingUtils' module of 'BiblioAnalysis' package
     with the optional parameters "only_ascii" and "skip" set at the default value True.
        
    '''
    # Standard library imports
    import functools
    import unicodedata

    nfc = functools.partial(unicodedata.normalize,'NFD')
    text = nfc(text). \
               encode('ascii', 'ignore'). \
               decode('utf-8')
    if strip:
        text = text.strip()
    
    return text


def special_symbol_remove(text, only_ascii = True, strip = True):                                                        # Deprecated, replaced by "remove_special_symbol"
    '''The function `special_symbol_remove` remove accentuated characters in the string 'text'
    and ignore non-ascii characters if 'only_ascii' is true. Finally, spaces at the ends of 'text'
    are removed if strip is true.
    
    Args:
        text (str): the text where to remove special symbols.
        only_ascii (boolean): True to remove non-ascii characters from 'text' (default: True).
        strip (boolean): True to remove spaces at the ends of 'text' (default: True).
        
    Returns:
        (str): the modified string 'text'.
    
    '''
    # Standard library imports
    import functools
    import unicodedata

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
