__all__ = ['address_inst_full_list',          #
           'affiliation_uniformization',      #
           'build_address_affiliations_lists',
           'build_addresses_institutions',
           'build_institutions_dic',          #
           'build_norm_raw_affiliations_dict', 
           'extend_author_institutions',      #
           'getting_secondary_inst_list',     #
           'read_inst_types',
           'saving_raw_institutions',         #
           'setting_secondary_inst_filter',   #
           ]

# To do: def the internal functions and the deprecated ones


# Globals used from BiblioAnalysis_Utils.BiblioGeneralGlobals:  DASHES_CHANGE, SYMB_CHANGE,
#                                                                                                                                
# Globals used from BiblioAnalysis_Utils.BiblioSpecificGlobals: COL_NAMES, DIC_AMB_WORDS,
#                                                               DIC_INST_FILENAME, DIC_OUTDIR_PARSING , 
#                                                               EMPTY, INST_BASE_LIST,            
#                                                               INST_FILTER_LIST, RAW_INST_FILENAME, REP_UTILS, 
#                                                               RE_SUB, RE_SUB_FIRST, RE_ZIP_CODE,                                                               


# Functions used from BiblioAnalysis_Utils.BiblioGui: Select_multi_items
# Functions used from BiblioAnalysis_Utils.BiblioParsingUtils: country_normalization
#                                                              special_symbol_remove


def address_inst_full_list(full_address, inst_dic):

    '''The `address_inst_full_list` function allows building the affiliations list of a full address
    using the internal function `_check_institute`of `BiblioParsingUtils` module
    
    Args:
       full_address (str): the full address to be parsed in institutions and country.
       inst_dic (dict): a dict used for the normalization of the institutions names, 
                        with the raw names as keys and the normalized names as values.
        
    Returns:
        (namedtuple): tuple of two strings. 
                      - The first is the joined list of normalized institutions names 
                      found in the full address.
                      - The second is the joined list of raw institutions names of the full address 
                      with no fully corresponding normalized names.
        
    Notes:
        The globals 'RE_ZIP_CODE' and 'EMPTY' are imported from `BiblioSpecificGlobals` module 
        of `BiblioAnalysis_Utils` package.
        The function `country_normalization` is imported from `BiblioParsingUtils` module
        of `BiblioAnalysis_utils` package.
        
    '''
    
    # Standard library imports
    import re
    from collections import namedtuple
    from string import Template

    # 3rd party imports
    import pandas as pd
    from fuzzywuzzy import process
    
    # Local imports
    from BiblioAnalysis_Utils.BiblioParsingUtils import country_normalization
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import INST_BASE_LIST
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import RE_ZIP_CODE
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import EMPTY

    inst_full_list_ntup = namedtuple('inst_full_list_ntup',['norm_inst_list','raw_inst_list'])
    
    country_raw = full_address.split(",")[-1].strip()
    country = country_normalization(country_raw)
    add_country = "_" + country
    
    if RE_ZIP_CODE.findall(full_address):
        address_to_keep = re.sub(RE_ZIP_CODE,"",full_address) + ","
    else:
        address_to_keep = ", ".join(full_address.split(",")[:-1])    
    address_to_keep = address_to_keep.lower()
    
    # Building the list of normalized institutions which raw institutions are found in 'address_to_keep' 
    # and building the corresponding list of raw institutions found in 'address_to_keep'
    norm_inst_full_list = [] 
    raw_inst_found_list = []
    for raw_inst, norm_inst in inst_dic.items():        
        raw_inst_lower = raw_inst.lower()
        raw_inst_split = raw_inst_lower.split()                     
        if _check_institute(address_to_keep,raw_inst_split):            
            norm_inst_full_list.append(norm_inst + add_country)            
            raw_inst_found_list.append(raw_inst_lower)

    # Cleaning 'raw_inst_found_list' from partial institution names
    for inst_base in INST_BASE_LIST:
        if (inst_base.lower() in raw_inst_found_list) and (inst_base.lower()+"," not in address_to_keep) : 
            raw_inst_found_list.remove(inst_base.lower())
    for raw_inst_found in raw_inst_found_list:
        other_raw_inst_found_list = raw_inst_found_list.copy()
        other_raw_inst_found_list.remove(raw_inst_found)        
        for other_raw_inst_found in other_raw_inst_found_list:
            if raw_inst_found in other_raw_inst_found:
                raw_inst_found_list = other_raw_inst_found_list

    # Removing 'raw_inst_found_list' items from 'address_to_keep'             
    for raw_inst_found in raw_inst_found_list:            
        if raw_inst_found in address_to_keep: 
            address_to_keep = address_to_keep.replace(raw_inst_found,"")  
            address_to_keep = ", ".join(x.strip() for x in address_to_keep.split(","))

    while (address_to_keep and address_to_keep[0] == "-"): address_to_keep = address_to_keep[1:]
    address_to_keep = address_to_keep.replace("-", " ")
    address_to_keep = " ".join(x.strip() for x in address_to_keep.split(" ") if x!="")       
            
    # # Building the list of raw institutions remaning in 'address_to_keep'
    raw_inst_full_list = [x.strip() + add_country for x in address_to_keep.split(",") if (x!="" and x!=" ")]

    # Building a string from the final list of raw institutions  
    if raw_inst_full_list:
        raw_inst_full_list_str = ";".join(raw_inst_full_list)       
    else:
        raw_inst_full_list_str = EMPTY 
    
    # Building a string from the final list of normalized institutions without duplicates
    norm_inst_full_list = list(set(norm_inst_full_list))
    if norm_inst_full_list:
        norm_inst_full_list_str = ";".join(norm_inst_full_list)
    else:
        norm_inst_full_list_str = EMPTY 
    
    # Setting the namedtuple to return
    inst_full_list_tup =  inst_full_list_ntup(norm_inst_full_list_str,raw_inst_full_list_str) 
    
    return inst_full_list_tup


def affiliation_uniformization(affiliation_raw):    # A refondre profondément
    
    '''The `affiliation_uniformization' function aims at getting rid 
    of heterogeneous typing of affilations. 
    It first replaces particular characters by standard ones 
    using 'DASHES_CHANGE' and 'SYMB_CHANGE' globals.
    Then, it substitutes by 'University' its aliases using specific 
    regular expressions set in 'RE_SUB',and 'RE_SUB_FIRST' globals.
    Finally, it removes accents using `special_symbol_remove` function.
    
    Args:
        affiliation_raw (str): the raw affiliation to be normalized.

    Returns:
        (str): the normalized affiliation.
        
    Notes:
        The globals 'DASHES_CHANGE' and 'SYMB_CHANGE' are imported
        from `BiblioGeneralGlobals` module of `BiblioAnalysis_Utils` package.
        The globals 'RE_SUB',and 'RE_SUB_FIRST' are imported
        from `BiblioSpecificGlobals` module of `BiblioAnalysis_Utils` package.
        The function `special_symbol_remove` is used from `BiblioParsingUtils` 
        of `BiblioAnalysis_utils` package.
        
    '''
    
    # Standard library imports
    import re
    
    # Local imports
    from BiblioAnalysis_Utils.BiblioParsingUtils import special_symbol_remove
    from BiblioAnalysis_Utils.BiblioGeneralGlobals import DASHES_CHANGE
    from BiblioAnalysis_Utils.BiblioGeneralGlobals import SYMB_CHANGE
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import DIC_AMB_WORDS
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import RE_SUB
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import RE_SUB_FIRST
    
    def _normalize_amb_words(text): 
        for amb_word in DIC_AMB_WORDS.keys():
            text = text.replace(amb_word, DIC_AMB_WORDS[amb_word]).strip()
        text = " ".join(text.split())
        return text
    
    affiliation_raw = _normalize_amb_words(affiliation_raw)
    affiliation_raw = affiliation_raw.translate(DASHES_CHANGE)
    affiliation_raw = affiliation_raw.translate(SYMB_CHANGE)
    affiliation_raw = re.sub(RE_SUB_FIRST,'University' + ', ',affiliation_raw)
    affiliation_raw = re.sub(RE_SUB,'University' + ' ',affiliation_raw)
    affiliation = special_symbol_remove(affiliation_raw, only_ascii=True, skip=True)
    
    return affiliation


def build_institutions_dic(rep_utils = None, dic_inst_filename = None):
    
    '''The `builds_institutions_dic` fuction builds the dict 'inst_dic' 
    giving the mormalized names of institutions from a csv file `dic_inst_filename`.
    The name of the csv file is set in the `DIC_INST_FILENAME` global.
    
    Args: 
        rep_utils (str): name of the folder where the csv file is stored
        dic_inst_filename (str): name of the csv file.        
    
    Returns:       
        `dict`: `inst_dic` as {raw_inst:norm_inst} where 
                - raw_inst a raw institution name 
                - norm_inst is the normalized institution name.
        
    Note:
        The globals `DIC_INST_FILENAME` and `REP_UTILS` from `BiblioSpecificGlobals` module
        of `BiblioAnalysis_utils` package are used.
    
    '''
    
    # Standard library imports
    from pathlib import Path
    
    # 3rd party imports
    import pandas as pd
    
    # Local imports
    from BiblioAnalysis_Utils.BiblioSpecificGlobals  import DIC_INST_FILENAME
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import REP_UTILS    
    
    if dic_inst_filename == None: dic_inst_filename = DIC_INST_FILENAME
    if rep_utils == None: rep_utils = REP_UTILS 
    
    # Setting the file path for dic_inst_filename file reading    
    path_dic_inst = Path(__file__).parent / rep_utils / Path(dic_inst_filename)
    
    # Reading and cleaning the dic_inst_filename file
    inst_dic = pd.read_csv(path_dic_inst,sep=':',header=None,encoding='latin1')
    inst_dic.sort_values([0],inplace=True)
    inst_dic[0] = inst_dic[0].str.strip()
    inst_dic[1] = inst_dic[1].str.strip()
    inst_dic = dict(zip(inst_dic[0],inst_dic[1]))
    
    return inst_dic


def _check_institute(address,raw_inst_split):

    '''The funstion `_check_institute` checks if all the words contained in the list 'raw_inst_split'
    are part of the string 'address'.
    
    A word is defined as a string beginning and ending with a letter.
    For instance, 'cea-leti' or 'Laue-Langevin' are words but not 'Kern-' or '&aaZ)'.
    
    The regexp used is based on the following rules:
        - Alphanumerical (AM) characters are {a…z,A…Z,0…9,_}.
        - Non-alphanumerical (NAM) characters are all other characters.
        - '\b' detects transition between NAM and AM such as '@a', '<space>a', 'a-', '(a', 'a.', etc.
        - '\B' detects transition between AM and AM such as '1a', 'za', 'a_', '_a', etc.
    
    Matches are found between a word 'WORD' of the list `raw_inst_split` and a substring 'WORDS'
    of the string 'address' in 4 cases:

        - 'WORD' matches in '...dWORD' or '...dWORDd...' by the regexp "'\d+\BWORD\B\d+'". 
        ex: 'UMR' matches in 'a6UMR7040.' '@6UMR' or 'matches in '...*WORDd*...'', etc. 
                  doesn't match in 'aUMR', '6UMR-CNRS', '6@UMR7040', etc.

        - 'WORD' matches '...dWORD*...'  by the regexp "'\d+\BWORD\b'".  
        ex: 'UMR' matches in 'a6UMR-CNRS', etc.
                  doesn't match in '@6UMRCNRS', '@UMR-CNRS', etc.

        - 'WORD' matches in '...*WORD*...' by the regexp "'\bWORD\b'". 
        ex: 'UMR' matches in '(UMR7040)', '@UMR-CNRS', etc. where 'UMR'is in between NAM
                  doesn't match in 'UMR7040', '@6UMR_CNRS', etc, where an NAM at least misses around it.

        - 'WORD' matches in '...*WORDd...', by the regexp "'\bWORD\B\d+'"
        ex UMR = %(UMR43)+#
        ex: 'UMR' matches in '6@UMR7040', 'CNRS-UMR7040', etc.
                  doesn't match in 'CNRS_UMR7040', '#UMR_CNRS', etc.

    where '...' stands for any characters, 'd' stands for a digit and '*' stands for an NAM character. 
    
    The match is case insensitive.
    
    According the mentionned 4 rules an isolated '&' such as in 'Art & Metiers' 
    and words ending by a minus (ex: Kern-) are not catched. 
    A specific pretreatment of `raw_inst_split` and `address` should be done before calling this function.
    
    Examples:
        - _check_institute(['den'],'dept. of energy conversion, university of denmark,') is False.
        - _check_institute(['den','dept'],'DEN dept. of energy conversion, university of denmark,') is True.
    
    Args:
        address (str): the address where to check the matching of words.
        raw_inst_split (list): list of words to be found to match in the string 'address'.
    
    Returns:
        (boolean): 'True' for a full match.
                   'False' otherwise.
    '''
    # Standard library imports
    import re
    from string import Template
    
    raw_inst_split = list(set(raw_inst_split))
    
    # Taking care of the potential isolated special characters '&' and '-'   
    raw_inst_split = [x.replace("&","and") for x in raw_inst_split]    
    raw_inst_split_init = raw_inst_split.copy()

    # Removing small words
    small_words_list = ['a','et','de','and','for','of','the']
    for word in small_words_list:
        if word in raw_inst_split_init:
            raw_inst_split.remove(word)
    
    raw_inst_split_init = raw_inst_split.copy()
    for word in raw_inst_split_init:
        if len(word)==1:
            raw_inst_split.remove(word)
    
    # Adding \ to escape regexp reserved char
    for char in ["$",'(',')','[',']','^','-']: 
        escaped_char = '\\'+ char
        raw_inst_split = [x.replace(char,escaped_char) for x in raw_inst_split]
        
    items_number = len(raw_inst_split)

    # Building the 're_inst' regexp searching for 'raw_inst_split' items in 'address' 
    dic = {"inst"+str(i):inst for i,inst in enumerate(raw_inst_split)}

    template_inst = Template(r'|'.join([r'\d+\B$inst'+ str(i) + r'\B\d+'
                                      + '|'
                                      + r'\d+\B$inst' + str(i) + r'\b'
                                      + '|'
                                      + r'\b$inst' + str(i) + r'\b'
                                      + '|'
                                      + r'\b$inst' + str(i) + r'\B\d+'
                                      for i in range(items_number)]))

    re_inst = re.compile(template_inst.substitute(dic), re.IGNORECASE)

    # Checking mach of 'raw_inst_split' items in 'address' using 're_inst' regexp
    items_set = set(re_inst.findall(address))
    if  len(items_set) == items_number:
        return True
    else:
        return False


def extend_author_institutions(in_dir,inst_filter_list):
    ''' The `extend_author_institutions` function extends the .dat file of authors with institutions 
    initialy obtained by the parsing of the corpus, with complementary information about institutions
    selected by the user.
    
    Args:
        in_dir (path): path to the .dat file of authors with institutions.
        inst_filter_list (list): the affiliations filter list of tuples (institution, country). 

    Retruns:
        None
        
    Notes:
        The globals 'COL_NAMES' and 'DIC_OUTDIR_PARSING' from `BiblioSpecificGlobals` module 
        of `BiblioAnalysis_Utils` package are used.
    
    '''
    
    # Standard library imports
    from pathlib import Path
    
    # 3rd party imports
    import pandas as pd
    
    # Local imports
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import COL_NAMES
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import DIC_OUTDIR_PARSING
    
    def _address_inst_list(inst_names_list,institutions):

        secondary_institutions = []
        for inst in inst_names_list:
            if inst in institutions:
                secondary_institutions.append(1)
            else:
                secondary_institutions.append(0)  
             
        return secondary_institutions
    
    institutions_alias = COL_NAMES['auth_inst'][4]
    sec_institutions_alias = COL_NAMES['auth_inst'][5]
    
    # Setting the key for the name of the '.dat' file of authors with institutions 
    # obtained by parsing the corpus
    item = 'I2' 
    
    # Reading the '.dat' file                   
    read_usecols = [COL_NAMES['auth_inst'][x] for x in [0,1,2,3,4]]     
    df_I2= pd.read_csv(in_dir / Path(DIC_OUTDIR_PARSING[item]),
                     sep='\t',
                     usecols=read_usecols)
    
    # Setting an institution name for each of the institutions indicated in the institutions filter
    inst_names_list = [f'{x[0]}_{x[1]}' for x in inst_filter_list]   
    
    # Building the "sec_institution_alias" column in the 'df_I2' dataframe using "inst_filter_list"
    df_I2[sec_institutions_alias] = df_I2.apply(lambda row:
                                             _address_inst_list(inst_names_list,row[institutions_alias]),
                                             axis = 1)

    # Distributing in a 'df_inst_split' df the value lists of 'df_I2[sec_institutions_alias]' column  
    # into columns which names are in 'inst_names_list' list     
    df_inst_split = pd.DataFrame(df_I2[sec_institutions_alias].sort_index().to_list(),
                                          columns=inst_names_list)
    
    # Extending the 'df' dataframe with 'df_inst_split' dataframe
    df_I2 = pd.concat([df_I2, df_inst_split], axis=1)

    # Droping the 'df[sec_institutions_alias]' column which is no more usefull
    df_I2.drop([sec_institutions_alias], axis=1, inplace=True)
    
    # Saving the extended 'df_I2' dataframe in the same '.dat' file 
    df_I2.to_csv(in_dir/ Path(DIC_OUTDIR_PARSING[item]), 
                 index=False,
                 sep='\t') 


def getting_secondary_inst_list(out_dir_parsing):
    '''The `getting_secondary_inst_list` function provides the list of institutions of the corpus.
   
    Args:
        out_dir_parsing (path): the corpus parsing path for reading the "DIC_OUTDIR_PARSING['I2']" file 
                                that lists the authors with their institutions for each article.
       
    Returns:
        (list): list of strings 'country:institution'
       
    Notes:
        The globals 'COL_NAMES'and 'DIC_OUTDIR_PARSING' from `BiblioSpecificGlobals` module 
        of `BiblioAnalysis_Utils` package are used.       
    '''
   
    # Standard library imports
    from pathlib import Path
   
    # 3rd party imports
    import numpy as np
    import pandas as pd
   
    # Local imports
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import COL_NAMES
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import DIC_OUTDIR_PARSING
   
    
    institutions_alias = COL_NAMES['auth_inst'][4]
    country_alias = COL_NAMES['country'][2]   
    
    df_auth_inst = pd.read_csv(Path(out_dir_parsing) / Path(DIC_OUTDIR_PARSING['I2']),
                                sep = '\t')
    raw_institutions_list = []
    for auth_inst in df_auth_inst[institutions_alias]:
        raw_institutions_list.append(auth_inst.strip())
       
    institutions_list = list(np.concatenate([raw_inst.split(';') for raw_inst in raw_institutions_list]))
    institutions_list  = sorted(list(set(institutions_list)))
 
    country_institution_list = [x.split('_')[1] + ':' + x.split('_')[0] for x in institutions_list]
    country_institution_list = sorted(country_institution_list)
   
    return country_institution_list


def saving_raw_institutions(df_I2,out_dir):
    '''The `saving_raw_institutions`function allows to save the list of raw institutions of a corpus.
    The raw institutions are the ones that have not been complitly replaced by the normalized names 
    of institutions given in a specific file.
    
    Args:
        df_I2 (dataframe): a dataframe resulting from the corpus parsing containing the raw-institutions column.
        out_dir (str): a string for setting the the folder where the file of raw institutions is saved
                       as a csv file which name is given by the global `RAW_INST_FILENAME`.
    
    Returns:
    
    Note:
        The globals `COL_NAMES`and `RAW_INST_FILENAME` are imported from `BiblioSpecificGlobals` module 
        of `BiblioAnalysis_Utils` package.
    
    '''
    # Standard libraries import
    from pathlib import Path

    # 3rd party imports
    import pandas as pd

    # Local imports
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import COL_NAMES
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import RAW_INST_FILENAME
    
    raw_institutions_alias = COL_NAMES['auth_inst'][5]
    raw_institutions_authors_lists = df_I2[raw_institutions_alias].to_list()

    raw_institutions_full_lists = [x.split(';') for x in raw_institutions_authors_lists]

    raw_institutions_full_list = []
    [raw_institutions_full_list.extend(x) for x in raw_institutions_full_lists]

    raw_institutions_full_list = list(set(raw_institutions_full_list))
    raw_institutions_full_list.sort()

    df_raw_inst = pd.DataFrame(raw_institutions_full_list, columns = [raw_institutions_alias])
    df_raw_inst.to_csv(Path(out_dir) / Path(RAW_INST_FILENAME),
                       index=False,
                       sep='\t')

    
def setting_secondary_inst_filter(out_dir_parsing):
    
    '''The `setting_secondary_inst_filter` function allows building the affiliation filter "inst_filter_list"
    from the institutions list of the corpus using the `Select_multi_items` GUI.
    
    Args:
        out_dir_parsing (path): the corpus parsing path for reading the "DIC_OUTDIR_PARSING['I2']" file.
        
    Returns:
        (list): list of tuples (institution,country) selected by the user.
        
    Notes:
        The globals 'COL_NAMES' and 'DIC_OUTDIR_PARSING' from `BiblioSpecificGlobals` 
        of `BiblioAnalysis_utils` package are used.
        The function `Select_multi_items` from `BiblioGui module` of `BiblioAnalysis_utils` package is used.
        
    '''
    
    # Standard library imports
    from pathlib import Path
    
    # 3rd party imports
    import numpy as np
    import pandas as pd
    
    # Local imports
    from BiblioAnalysis_Utils.BiblioGui import Select_multi_items
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import COL_NAMES
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import DIC_OUTDIR_PARSING
    
    
    institutions_alias = COL_NAMES['auth_inst'][4]
    country_alias = COL_NAMES['country'][2]    
    
    df_auth_inst = pd.read_csv(Path(out_dir_parsing) / Path(DIC_OUTDIR_PARSING['I2']),
                                sep = '\t')
    raw_institutions_list = []
    for auth_inst in df_auth_inst[institutions_alias]:
        raw_institutions_list.append(auth_inst.strip())
        
    institutions_list = list(np.concatenate([raw_inst.split(';') for raw_inst in raw_institutions_list]))
    institutions_list  = sorted(list(set(institutions_list)))

    country_institution_list = [x.split('_')[1] + ':' + x.split('_')[0] for x in institutions_list]
    country_institution_list = sorted(country_institution_list)

    selected_list = Select_multi_items(country_institution_list,
                                       mode='multiple',
                                       fact=2,
                                       win_widthmm=80,
                                       win_heightmm=100,
                                       font_size=16)

    inst_filter_list = [(x.split(':')[1].strip(),x.split(':')[0].strip()) for x in selected_list]
    
    return inst_filter_list


################################### New functions for address parsing and affiliations normalization ###################################
#To Do: test in BiblioAnalysisNormAff notebook

def standardize_address(raw_address):
    
    '''The function `standardize_address` standardizes the string 'raw_address' by replacing
    all aliases of a word, such as 'University', 'Institute', 'Center' and' Department', 
    by a standardized version.
    The aliases of a given word are captured using a specific regex which is case sensitive defined 
    by the global 'DIC_WORD_RE_PATTERN'.
    The aliases may contain symbols from a given list of any language including accentuated ones. 
    The length of the alliases is limited to a maximum according to the longest alias known.
        ex: The longest alias known for the word 'University' is 'Universidade'. 
            Thus, 'University' aliases are limited to 12 symbols begenning with the base 'Univ' 
            + up to 8 symbols from the list '[aàädeéirstyz]' and possibly finishing with a dot.
            
    Then, dashes are replaced by a hyphen-minus using 'DASHES_CHANGE' global and apostrophes are replaced 
    by the standard cote using 'APOSTROPHE_CHANGE' global.         
    
    Args:
        raw_address (str): The full address to be standardized.
        
    Returns:
        (str): The full standardized address.
        
    Notes:
        The global 'DIC_WORD_RE_PATTERN' and 'UNKNOWN' are imported from the module `BiblioSpecificGlobals`  
        of the package `BiblioAnalysis_Utils`.
        The globals 'DASHES_CHANGE' and 'APOSTROPHE_CHANGE' are imported from the module `BiblioGeneralGlobals`  
        of the package `BiblioAnalysis_Utils`.
        The function `country_normalization` is imported from the module `BiblioParsingInstitutions`
        of the package `BiblioAnalysis_Utils`.
        
    '''
    
    # Standard library imports
    import re
    
    # Local imports
    from BiblioAnalysis_Utils.BiblioParsingUtils import country_normalization
    from BiblioAnalysis_Utils.BiblioGeneralGlobals import APOSTROPHE_CHANGE
    from BiblioAnalysis_Utils.BiblioGeneralGlobals import DASHES_CHANGE
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import DIC_WORD_RE_PATTERN
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import UNKNOWN
    
    # Uniformizing words
    standard_address = raw_address
    for word_to_subsitute, re_pattern in DIC_WORD_RE_PATTERN.items():
        standard_address = re.sub(re_pattern,word_to_subsitute + ' ',standard_address)
    standard_address = re.sub(r'\s+',' ',standard_address)
    standard_address = re.sub(r'\s,',',',standard_address)
    
    # Uniformizing dashes
    standard_address = standard_address.translate(DASHES_CHANGE)
    
    # Uniformizing apostrophes
    standard_address = standard_address.translate(APOSTROPHE_CHANGE)
    
    # Uniformizing countries
    country_pos = -1
    first_raw_affiliations_list = standard_address.split(',')
    # This split below is just for country finding even if affiliation may be separated by dashes
    raw_affiliations_list = sum([x.split(' - ') for x in first_raw_affiliations_list],[])        
    country = country_normalization(raw_affiliations_list[country_pos].strip())
    space = " "
    if country != UNKNOWN:
        standard_address = ','.join(first_raw_affiliations_list[:-1] + [space + country])
    else:
        standard_address = ','.join(first_raw_affiliations_list + [space + country])

    return standard_address


def search_items(affiliation, country, verbose = False):
    
    '''The function `search_items` searches for several item types in 'affiliation' after accents removal 
    and converting in lower case even if the search is case sensitive.
    It uses the following internal functions:
        - The function `_search_droping_bp` searches for words that are postal-box numbers such as 'BP54'.
        - The function `_search_droping_digits` searches for words that contains digits such as zip codes 
        which templates are given per country by the global 'ZIP_CODES' dict.
        - The function `_search_droping_suffix` searches for words ending by a suffix among 
        those given by the global 'DROPING_SUFFIX' such as 'strasse' in 'helmholtzstrasse'.
        - The function `_search_droping_town` searches for words that are towns listed 
        in the global 'COUNTRY_TOWNS'.
        - The function `_search_droping_words` searches for words given by the global 'DROPING_WORDS' 
        such as 'Avenue'.
        - The internal function `_search_keeping_words` to search in the chunks for isolated words 
        given by the global 'KEEPING_WORDS' using a templated regex.
        - The internal function `_search_keeping_prefix` to search in the chunks for prefixes 
        given by the global 'KEEPING_PREFIX' using a templated regex.
    
    As a reminder, in a regex:
        - '\b' captures the transition between a non-alphanumerical symbol and an alphanumerical symbol 
        and vice-versa.
        - '\B' captures the transition between two alphanumerical symbols.
    
    Args:
        affiliation (str): A chunck of a standardized address where droping items are searched.
        country (str): The string that contains the country.
        verbose (bool): True for allowing control prints (default: False).
       
    Returns:
        (namedtuple): A namedtuple which values are booleans returned by the internal functions that return
                      True if the corresponding searched item is found.
    
    Notes:
        The function `remove_special_symbol` is imported from the module `BiblioParsingUtils` 
        of the package `BiblioAnalysis_Utils`.
        The globals 'DROPING_SUFFIX', 'DROPING_WORDS', 'KEEPING_PREFIX' are imported from the module `BiblioSpecificGlobals`  
        of the package `BiblioAnalysis_Utils`.
        The globals 'COUNTRY_TOWNS' and 'ZIP_CODES' are imported from the module `BiblioGeneralGlobals`  
        of the package `BiblioAnalysis_Utils`.
    
    '''
    
    # Standard library imports
    import re
    from string import Template
    
    # Local imports 
    from BiblioAnalysis_Utils.BiblioParsingUtils import remove_special_symbol
    from BiblioAnalysis_Utils.BiblioParsingUtils import rationalize_town_names
    
    from BiblioAnalysis_Utils.BiblioGeneralGlobals import ZIP_CODES
        
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import BASIC_KEEPING_WORDS
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import COUNTRY_TOWNS
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import DROPING_SUFFIX
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import DROPING_WORDS    
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import FR_DROPING_WORDS
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import GEN_KEEPING_WORDS
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import KEEPING_PREFIX
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import KEEPING_WORDS
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import USER_KEEPING_WORDS
    
    
    ################################### Internal functions start ###################################
    def  _search_droping_bp(text):
        '''The internal function `_search_droping_bp` searches in 'text' for words 
        begenning with 'bp' followed by digits using a non case sensitive regex.
        
        Args:
            text (str): The string where the words are searched after being converted to lower case.
            
        Returns:
            (boolean): True if a word is found.
            
        '''

        re_bp = re.compile(r'\bbp\s?\d+[a-z]?\b'     # For instence capturing "bp12" in "azert BP12 yui_OP"
                                                     # capturing " bp 156X" in " bp 156X azert"
                           + '|'
                           + r'\b\d+bp\b')  # For instence capturing "08bp" in "azert 08BP yui_OP".

        flag = False
        result = re.search(re_bp,text.lower())
        if result is not None:
            if verbose:
                print('Droping word is postal-box abbreviation')
            flag = True
        return [flag] 

    def _search_droping_digits(text):
        '''The internal function `_search_droping_digits` searches in 'text' for words 
        similar to zip codes except those begenning with a prefix from the global 'KEEPING_PREFIX' 
        followed by 3 or 4 digits using case-sensitive regexes. 
        Regex for zip-codes search uses the global 'ZIP_CODES' dict for countries from 'ZIP_CODES.keys()'.
        Specific regex are set for 'United Kingdom', 'Canada' and 'United States'. 

        Args:
            text (str): the string where the words are searchedafter being converted to lower case.

        Returns:
            (boolean): True if a word different from those begenning with a prefix from the global 'KEEPING_PREFIX'  
                       followed by 3 or 4 digits is found.

       Notes:
            This function uses the globals 'KEEPING_PREFIX' and 'ZIP_CODES' imported in the calling function
            and the variable 'country'.

        '''

        # Setting regex for zip-codes search
        pattern = ''
        if country == 'United Kingdom':
            # Capturing: for instence, " BT7 1NN" or " WC1E 6BT" or " G128QQ"
            #            " a# #a", " a# #az", " a# ##a", " a# ##az",
            #            " a##a", " a##az", " a###a", " a###az",
            #
            #            " a#a #a", " a#a #az", " a#a ##a", " a#a ##az",
            #            " a#a#a", " a#a#az", " a#a##a", " a#a##az",
            #
            #            " a## #a", " a## #az", " a## ##a", " a## ##az",
            #            " a###a", " a###az", " a####a", " a####az",
            #            
            #            " a##a #a", " a##a #az", " a##a ##a", " a##a ##az",
            #            " a##a#a", " a##a#az", " a##a##a", " a##a##az",
            #            
            #            " az# #a", " az# #az", " az# ##a", " az# ##az",
            #            " az##a", " az##az", " az###a", " az###az",
            #
            #            " az#a #a", " az#a #az", " az#a ##a", " az#a ##az",
            #            " az#a#a", " az#a#az", " az#a##a", " az#a##az",
            #
            #            " az## #a", " az## #az", " az## ##a", " az## ##az",
            #            " az###a", " az###az", " az###a", " az####az",
            #
            #            " az##a #a", " az##a #az", " az##a ##a", " az##a ##az",
            #            " az##a#a", " az##a#az", " az##a#a", " az##a##az",
            pattern = r'^\s?[a-z]{1,2}\d{1,2}[a-z]{0,1}\s?\d{1,2}[a-z]{1,2}$'

        elif country == 'United States' or country == 'Canada':
            # Capturing: for instence, " NY" or ' NI BT48 0SG' or " ON K1N 6N5" 
            #            " az" or " az " + 6 or 7 characters in 2 parts separated by spaces
            pattern = r'^\s?[a-z]{2}$' + '|' + r'^\s?[a-z]{2}\s[a-z0-9]{3,4}\s[a-z0-9]{2,3}$'

        elif country in ZIP_CODES.keys():
            letters_list, digits_list = ZIP_CODES[country]['letters'], ZIP_CODES[country]['digits']

            if letters_list or digits_list:
                zip_template = Template(r'\b($zip_letters)[\s-]?(\d{$zip_digits})\b')

                letters_join = '|'.join(letters_list) if len(letters_list) else ''
                pattern_zip_list = [zip_template.substitute({"zip_letters": letters_join,
                                                             "zip_digits":digits})
                                    for digits in digits_list]
                pattern = '|'.join(pattern_zip_list)        
        else:
            print('country not found:', country)
            flag = False
            return [flag]

        zip_result = False
        if pattern: 
            re_zip = re.compile(pattern)
            if re.search(re_zip,text.lower()): zip_result = True   

        # Setting search regex of embedding digits
        re_digits = re.compile(r'\s?\d+(-\d+)?\b'      # For instence capturing " 1234" in "azert 1234-yui_OP"
                                                      # or " 1" in "azert 1-yui_OP" or " 1-23" in "azert 1-23-yui".                            
                               + '|'
                               + r'\b[a-z]+(-)?\d{2,}\b') # For instence capturing "azert12" in "azert12 UI_OPq" 
                                                      # or "azerty1234567" in "azerty1234567 ui_OPq".

        # Setting search regex of keeping-prefix
        # for instence, capturing "umr1234" in "azert UMR1234 YUI_OP" or "fr1234" in "azert-fr1234 Yui_OP".
        prefix_template = Template(r'\b$prefix[-]?\d{4}\b')
        pattern_prefix_list = [prefix_template.substitute({"prefix": prefix})
                               for prefix in KEEPING_PREFIX]   
        re_prefix = re.compile('|'.join(pattern_prefix_list))             


        prefix_result = False if (re.search(re_prefix,text.lower()) is None) else True
        if prefix_result and verbose: print('Keeping prefix: True')

        digits_result = False if (re.search(re_digits,text.lower()) is None) else True

        flag = False
        if not prefix_result and (zip_result or digits_result):    
            if verbose:
                print('Droping word is a zip code') if zip_result else  print('Droping word is a digits code')   
            flag = True

        return [flag]

    def _search_droping_suffix(text):
        '''The internal function `_search_droping_suffix` searches in 'text' for words 
        ending by a suffix among those given by the global 'DROPING_SUFFIX'  
        using a templated regex.
        
        Args:
            text (str): The string where the suffixes given by the global 'DROPING_SUFFIX' 
                        are searched after being converted to lower case.
            
        Returns:
            (boolean): True if a suffix given by the global 'DROPING_SUFFIX' is found.
            
        Notes:
            This function uses the global 'DROPING_SUFFIX' imported in the calling function.               

        '''
        
        droping_suffix_template = Template(r'\B$word\b'    # For instence capturing "platz" 
                                                            # in "Azertyplatz uiops12".
                                          +'|'
                                          +r'\b$word\b')   # For instence capturing "-gu" 
                                                            # in "Yeongtong-gu".
                    
        flag = False
        for word_to_drop in DROPING_SUFFIX:
            re_drop_words = re.compile(droping_suffix_template.substitute({"word":word_to_drop}))
            result = re.search(re_drop_words,text.lower())
            if result is not None:
                flag = True
                if verbose:
                    print('Droping word contains the suffix:', word_to_drop)
        return [flag]
   
    def _search_droping_town(text):
        '''The internal function `_search_droping_town` searches in 'text' for words in lower case
        that are towns listed in the global 'COUNTRY_TOWNS' for each country.
        
        Args:
            text (str): the string where the words are searched after being converted to lower case.
            
        Returns:
            (boolean): True if a word listed in the global 'COUNTRY_TOWNS' is equal to 'text' 
                       after spaces removal at ends.
                       
        Notes:
            This function uses the global 'COUNTRY_TOWNS' imported in the calling function.
            
        '''
        flag = False
        text_mod = rationalize_town_names(text.lower())
        if country in COUNTRY_TOWNS.keys():
            for word_to_drop in COUNTRY_TOWNS[country]:
                if word_to_drop == text_mod.strip(): 
                    if verbose:
                        print('Droping word is a town of ', country)
                    flag = True
        return [flag]   
    
    def _search_droping_words(text):
        '''The internal function `_search_droping_words` searches in 'text' for isolated words 
        given by the globals 'FR_DROPING_WORDS' and 'DROPING_WORDS' using a templated regex. If country is 'france'
        only the global 'FR_DROPING_WORDS' is used.
        
        Args:
            text (str): The string where the words are searched after being converted to lower case.
            
        Returns:
            (boolean): True if a word given by the global 'DROPING_WORDS' is found.
            
        Notes:
            This function uses the globals 'FR_DROPING_WORDS' and 'DROPING_WORDS' imported in the calling function.               

        '''
        
        droping_words_template = Template(  r'[\s(]$word[\s)]'     # For instence capturing "avenue" in "12 Avenue Azerty" or " cedex" in "azert cedex".
                                                                   # in "12 Avenue Azerty" or " cedex" in "azert cedex".
                                          + '|'
                                          + r'[\s]$word$$'
                                          + '|'
                                          + r'^$word\b')
                                                               

        flag = False
        if country.lower()=='france':
            droping_words_to_search = FR_DROPING_WORDS
        else:
            droping_words_to_search = FR_DROPING_WORDS + DROPING_WORDS
            
        for word_to_drop in droping_words_to_search:
            re_drop_words = re.compile(droping_words_template.substitute({"word":word_to_drop}))
            result = re.search(re_drop_words,text.lower())
            if result is not None:
                flag = True
                if verbose:
                    print('Droping word is the full word:', word_to_drop)
        return [flag]
    
    def _search_keeping_prefix(text):                               
        '''The internal function `_search_keaping_prefix` searches in 'text' for prefixes 
        given by the global 'KEEPING_PREFIX' using a templated regex if country is France.
        
        Args:
            text (str): the string where the words are searched after being converted to lower case.
            
        Returns:
            (boolean): True if a prefix given by the global 'KEEPING_PREFIX' is found.
            
        Notes:
            This function uses the global 'KEEPING_PREFIX' imported in the calling function.               

        '''              

        keeping_prefix_template = Template(r'\b$prefix\d{3,4}\b') 

        flag = False
        if country.lower() == 'france':
            for prefix_to_keep in KEEPING_PREFIX:
                re_keep_prefix = re.compile(keeping_prefix_template.substitute({"prefix":prefix_to_keep}))
                result = re.search(re_keep_prefix,text.lower())
                if result is not None:
                    if verbose:
                        print('Keeping word is the prefix:',prefix_to_keep)
                    flag = True
        return [flag]


    def _search_keeping_words(text):
        ''''The internal function `_search_keaping_words` searches in 'text' for isolated words 
        given by the global 'KEEPING_WORDS' using a templated regex.
        
        Args:
            text (str): the string where the words are searched after being converted to lower case.
            
        Returns:
            (boolean): True if a word given by the 'KEEPING_WORDS' global is found.
            
        Notes:
            This function uses the global 'KEEPING_WORDS' imported in the calling function.               
              

        '''
        keeping_words_template = Template(r'\b$word\b')

        gen_flag, basic_flag, user_flag  = False, False, False
        for word_to_keep in KEEPING_WORDS:
            re_keep_words = re.compile(keeping_words_template.substitute({"word":word_to_keep}))
            result = re.search(re_keep_words,text.lower())
            if result is not None:
                if verbose:
                    print('Keeping word is the full word:',word_to_keep)
                if word_to_keep in GEN_KEEPING_WORDS : gen_flag = True
                if word_to_keep in BASIC_KEEPING_WORDS : basic_flag = True 
                if word_to_keep in USER_KEEPING_WORDS : user_flag = True 

        return [gen_flag, basic_flag, user_flag]    
    #################################### Internal functions end ####################################

    from collections import namedtuple
    
    funct_list = [
                  _search_droping_bp,
                  _search_droping_digits,
                  _search_droping_suffix,
                  _search_droping_town,
                  _search_droping_words,
                  _search_keeping_prefix,
                  _search_keeping_words,
                  ]
    
    found_item_tup = namedtuple('found_item_tup', ['droping_bp',
                                                   'droping_digits',
                                                   'droping_suffix',
                                                   'droping_town',
                                                   'droping_words',
                                                   'keeping_prefix',
                                                   'gen_keeping_words',
                                                   'basic_keeping_words',
                                                   'user_keeping_words',
                                                   ])
    
    affiliation_mod = remove_special_symbol(affiliation, only_ascii = False, strip = False)
    flag_list = [funct(affiliation_mod) for funct in funct_list]
    
    # Flattening flag_list
    flag_list = sum(flag_list,[])
    found_item_flags = found_item_tup(*flag_list)

    return found_item_flags


def get_affiliations_list(std_address, drop_to_end = None, verbose = False):
    
    '''The function `get_affiliations_list` extracts first, the country and then, the list 
    of institutions from the standardized address 'std_address'. It splits the address in list of chuncks 
    separated by coma or isolated hyphen-minus.
    The country is present as the last chunk of the spliting.
    The other chunks are kept as institutions if they contain at least one word among 
    those listed in the global 'KEEPING_WORDS' or if they do not contain any item 
    searched by the function `search_droping_items`.
    The first chunck is always kept in the final institutions list.
    The spaces at the ends of the items of the final institutions list are removed.
    
    Args:
        std_address (str): The full address to be parsed in list of institutions and country.
        drop_to_end (boolean): If true, all chuncks are dropped after the first found to drop,
                               for some of the item types (not used).
        verbose (boolean): If true, printing of intermediate variables is allowed (default: False). 
        
    Returns:
        (tuple): A tuple composed of 3 items (list of kept chuncks, country and list of dropped chuncks).
        
    Notes:
        The function `search_droping_items` is imported from the module `BiblioParsingInstitutions`  
        of the package`BiblioAnalysis_Utils`.
        The function `country_normalization` is imported from the module `BiblioParsingInstitutions` 
        of the package`BiblioAnalysis_Utils`.
        The globals 'KEEPING_WORDS', 'KEEPING_PREFIX' and 'UNKNOWN' are imported from the module `BiblioSpecificGlobals` 
        of the package`BiblioAnalysis_Utils`.        
        
    '''
    
    # Standard library imports
    import re
    from string import Template
    
    # Local imports
    #from BiblioAnalysis_Utils.BiblioParsingInstitutions import search_items  (check used outside ?)
    
    # Splitting by coma the standard address in chuncks listed in an initial-affiliations list
    init_raw_affiliations_list = std_address.split(',')

    # Removing the first occurence of chunck duplicates from the initial-affiliations list
    # and putting them in a deduplicated-affiliations list
    drop_aff_idx_list = []
    for idx1, aff1 in enumerate(init_raw_affiliations_list): 
        drop_aff_idx_list.extend([min(idx1,idx2) for idx2,aff2 in enumerate(init_raw_affiliations_list) if idx1!=idx2 and aff1==aff2])
    dedup_raw_affiliations_list = []    
    dedup_raw_affiliations_list.extend([aff for idx, aff in enumerate(init_raw_affiliations_list) if idx not in set(drop_aff_idx_list)])             

    # Setting country index in raw-affiliations list
    country_pos = -1
    country = dedup_raw_affiliations_list[country_pos].strip()

    # Splitting by special characters the deduplicated chunks and putting them in a raw-affiliations list
    raw_affiliations_list = sum([x.split(' - ') for x in dedup_raw_affiliations_list],[])
    raw_affiliations_list = sum([x.split(' | ') for x in raw_affiliations_list],[])

    # Initializing the affiliations list by keeping systematically the first chunck of the full address
    affiliations_list = [raw_affiliations_list[0]]
    
    # Check affiliations only if length > 3 to avoid keeping affiliations of less than 3 characters
    check_affiliations_list = [aff for aff in raw_affiliations_list[1:] if len(aff)>3]
    
    if verbose: 
        print('Full standard address:',std_address)
        print('init_raw_affiliations_list:',init_raw_affiliations_list)
        print('dedup_raw_affiliations_list:',dedup_raw_affiliations_list)    
        print('country:', country)
        print('raw_affiliations_list flattened:',raw_affiliations_list)
        print('First affiliation:',dedup_raw_affiliations_list[0])
        print('check_affiliations_list:',check_affiliations_list)
        print()

    # Initializing the list of chuncks to drop from the raw-affiliations list
    affiliations_drop = []                                                                          
    
    # Searching for chuncks to keep and chuncks to drop in the raw-affiliations list, the first chunck and the country excepted
    if len(check_affiliations_list): 
        if verbose: print('Search results\n')
        for index,affiliation in enumerate(check_affiliations_list[:country_pos]):        
            if verbose: 
                print()
                print('index:', index, '  affiliation:', affiliation)
            found_item_flags = search_items(affiliation, country, verbose = verbose)
            if verbose: print('found_item_flags:', found_item_flags)
            add_affiliation_flag = False
            break_id = None
            droping_word_flags = [found_item_flags.droping_bp,
                                  found_item_flags.droping_digits,
                                  found_item_flags.droping_suffix,
                                  found_item_flags.droping_town,
                                  found_item_flags.droping_words]

            keeping_words_flags = [found_item_flags.gen_keeping_words,
                                  found_item_flags.basic_keeping_words,
                                  found_item_flags.user_keeping_words]

            if not any(droping_word_flags):
                affiliations_list.append(affiliation)
                add_affiliation_flag = True
                if verbose: print('No droping item found in:',affiliation,'\n')

            else:                
                if found_item_flags.droping_bp:
                    affiliations_drop.append(('droping_bp',check_affiliations_list[index:country_pos]))
                    break_id = 'droping_bp'
                    if verbose: print('Break identification:',break_id,'\n')
                    break 

                if found_item_flags.droping_digits:

                    if country.lower() in ['france','algeria']:
                        if not found_item_flags.keeping_prefix and not any(keeping_words_flags):
                            affiliations_drop.append(('droping_digits',check_affiliations_list[index:country_pos]))
                            break_id = 'droping_digits'
                            if verbose: print('Break identification:',break_id,'\n')
                            break
                        elif found_item_flags.gen_keeping_words:
                            if not add_affiliation_flag: 
                                affiliations_list.append(affiliation)
                                add_affiliation_flag = True
                            break_id = 'droping_digits aborted by gen_keeping_words'     
                            if verbose: print('Break identification:',break_id,'\n')
                        else: 
                            if not add_affiliation_flag: 
                                affiliations_list.append(affiliation)
                                add_affiliation_flag = True
                            if found_item_flags.basic_keeping_words: break_id = 'droping_digits aborted by basic_keeping_words'
                            if found_item_flags.user_keeping_words : break_id = 'droping_digits aborted by user_keeping_words'
                            if found_item_flags.keeping_prefix: break_id = 'droping_digits aborted by keeping_prefix'
                            if verbose: print('Break identification:',break_id,'\n')                    
                    else:
                        if not found_item_flags.gen_keeping_words and not found_item_flags.user_keeping_words:
                            affiliations_drop.append(('droping_digits',check_affiliations_list[index:country_pos]))
                            break_id = 'droping_digits'
                            if verbose: print('Break identification:',break_id,'\n')
                            break
                        elif found_item_flags.droping_words:
                            affiliations_drop.append(('droping_digits',check_affiliations_list[index:country_pos]))
                            break_id = 'droping_digits'
                            if verbose: print('Break identification:',break_id,'\n')
                            break
                        else:
                            if not add_affiliation_flag: 
                                affiliations_list.append(affiliation)
                                add_affiliation_flag = True
                                break_id = 'droping_digits aborted by user_keeping_words'
                                if verbose: print('Break identification:',break_id,'\n')

                if found_item_flags.droping_town:
                    if len(check_affiliations_list[index:country_pos])<=2:   
                        affiliations_drop.append(('droping_town',check_affiliations_list[index:country_pos]))
                        break_id = 'droping_town'
                        if verbose: print('Break identification:',break_id,'\n')    
                        break
                    else:
                        affiliations_drop.append(('droping_town',affiliation))
                        break_id = 'droping_town aborted by index of town in affiliations list'
                        if verbose: print('Break identification:',break_id,'\n')

                else:
                    if found_item_flags.droping_suffix:
                        if found_item_flags.gen_keeping_words or found_item_flags.user_keeping_words:
                            if not add_affiliation_flag: 
                                affiliations_list.append(affiliation)
                                add_affiliation_flag = True
                            if found_item_flags.gen_keeping_words: break_id = 'droping_suffix aborted by gen_keeping_words'
                            if found_item_flags.user_keeping_words: break_id = 'droping_suffix aborted by user_keeping_words'
                            if verbose: print('Break identification:',break_id,'\n')    
                        else:
                            affiliations_drop.append(('droping_suffix',check_affiliations_list[index:country_pos]))
                            break_id = 'droping_suffix'
                            if verbose: print('Break identification:',break_id,'\n')
                            break   

                    if found_item_flags.droping_words:
                        # Keeping affiliation when a keeping word is found only if no droping digit is found
                        # this keeps "department bldg civil" which is wanted even if "bldg" is a droping word 
                        # unfortunatly, this keeps unwanted "campus university", "ciudad university"... 
                        if any(keeping_words_flags) and not found_item_flags.droping_digits:  
                            if not add_affiliation_flag: 
                                affiliations_list.append(affiliation)
                                add_affiliation_flag = True
                            if found_item_flags.user_keeping_words: break_id = 'droping_word aborted by user_keeping_words'
                            if found_item_flags.basic_keeping_words: break_id = 'droping_word aborted by basic_keeping_words'
                            if found_item_flags.gen_keeping_words: break_id = 'droping_word aborted by gen_keeping_words'
                            if verbose: print('Break identification:',break_id,'\n')
                        else:
                            # Droping affiliation from affiliations_list if already added because of a former drop abort
                            if add_affiliation_flag:
                                affiliations_list = affiliations_list[:-1]
                                add_affiliation_flag = False
                            affiliations_drop.append(('droping_words',check_affiliations_list[index:country_pos]))
                            break_id = 'droping_words'
                            if verbose: print('Break identification:',break_id,'\n')
                            break             

    # Removing spaces from the affiliations kept 
    affiliations_list = [x.strip() for x in affiliations_list]
    if verbose:
        print('affiliations_list stripped:',affiliations_list)
        print()
    
    return (country,affiliations_list,affiliations_drop) 


def build_norm_raw_affiliations_dict(country_affiliations_file_path = None, verbose = False):
    '''The function `build_norm_raw_affiliations_dict` builds a dict keyyed by country. 
    The value per country is a dict keyyed by normalized affiliation. The value per normalized 
    affiliation is a list of sets of words representing the raw affiliations corresponding 
    to the normalized affiliation.
    
    Args:
        country_affiliations_file_path (path): Full path to the file "Country_affiliations.xlsx"; 
                                               if None, it is set using the globals 'COUNTRY_AFFILIATIONS_FILE' 
                                               and 'REP_UTILS'.
        verbose (bool): If true, variables are printed for code control (default = False).
        
    Returns:
        (dict): The built dict.

    Note:
        internalfunctions : _build_words_set, _build_words_sets_list       
        remove_special_symbol from BiblioAnalysis_Utils.BiblioParsingUtils
        COUNTRY_AFFILIATIONS_FILE, DIC_WORD_RE_PATTERN, MISSING_SPACE_ACRONYMS, SMALL_WORDS_DROP, REP_UTILS
        APOSTROPHE_CHANGE, DASHES_CHANGE, SYMB_DROP, SYMB_CHANGE
    
    '''

    # Standard library imports
    from pathlib import Path

    # 3rd party imports
    import openpyxl
    import pandas as pd

    # Local imports 
    import BiblioAnalysis_Utils as bau
    from BiblioAnalysis_Utils.BiblioParsingUtils import remove_special_symbol
    
    from BiblioAnalysis_Utils.BiblioGeneralGlobals import APOSTROPHE_CHANGE
    from BiblioAnalysis_Utils.BiblioGeneralGlobals import DASHES_CHANGE
    from BiblioAnalysis_Utils.BiblioGeneralGlobals import SYMB_CHANGE
    from BiblioAnalysis_Utils.BiblioGeneralGlobals import SYMB_DROP
    
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import COUNTRY_AFFILIATIONS_FILE 
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import DIC_WORD_RE_PATTERN    
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import MISSING_SPACE_ACRONYMS
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import SMALL_WORDS_DROP    
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import REP_UTILS


    ################################################ Local functions start ################################################

    def _build_words_set(raw_aff):
        '''The internal function `_build_words_set` builds sets of words from a raw affiliation
        after standardization of words and symbols, removing special symbols, adding missing spaces
        and droping small words. It uses the function `remove_special_symbol` and the globals: 
        - DIC_WORD_RE_PATTERN,
        - DASHES_CHANGE,
        - APOSTROPHE_CHANGE,
        - SYMB_CHANGE,
        - SYMB_DROP,
        - MISSING_SPACE_ACRONYMS,
        - SMALL_WORDS_DROP.

        Args:
            raw_aff (str): The raw affiliation used to build the sets of words.

        Returns:
            (tuple): Tuple of to sets of words. The first set is the canonical set of words issuing from the string 'raw_aff'. 
                     The second set is an added set if some specific accronyms are present in the first set of words. 

        Note:
            The globals... are imported from...
            The function `remove_special_symbols` is imported from the module `Biblio....`  
            of the package`BiblioAnalysis_Utils`.

        '''
        # Standard library imports
        import re
        from string import Template   

        # Setting substitution templates for searching small words or acronyms
        small_words_template = Template(r'[\s(]$word[\s)]'    # For instence capturing 'of' in 'technical university of denmark'                                                              
                                        + '|'
                                        + r'[\s]$word$$'      # For instence capturing 'd' in 'institut d ingenierie'
                                        + '|'
                                        + r'^$word\b')        # For instence capturing 'the' in 'the denmark university'

        accronymes_template = Template(r'[\s(]$word[\s)]'    # For instence capturing 'umr' in 'umr dddd' or 'umr dd'                                                             
                                      + '|'
                                      + r'[\s]$word$$'      
                                      + '|'
                                      + r'^$word\b')

        # Removing accents and spaces at ends
        raw_aff_mod = remove_special_symbol(raw_aff, only_ascii = False, strip = True)


        # Uniformizing words
        std_raw_aff = raw_aff_mod
        std_raw_aff_add = ""
        for word_to_substitute, re_pattern in DIC_WORD_RE_PATTERN.items():
            std_raw_aff = re.sub(re_pattern,word_to_substitute + ' ',std_raw_aff)
        std_raw_aff = re.sub(r'\s+',' ',std_raw_aff)
        std_raw_aff = re.sub(r'\s,',',',std_raw_aff)
        std_raw_aff = std_raw_aff.lower()
        
        # Uniformizing dashes
        std_raw_aff = std_raw_aff.translate(DASHES_CHANGE)

        # Uniformizing apostrophes
        std_raw_aff = std_raw_aff.translate(APOSTROPHE_CHANGE)

        # Uniformizing symbols
        std_raw_aff = std_raw_aff.translate(SYMB_CHANGE)

        # Droping particular symbols
        std_raw_aff = std_raw_aff.translate(SYMB_DROP)
        if verbose: print('       std_raw_aff:', std_raw_aff)

        # Building the corresponding set of words to std_raw_aff
        raw_aff_words_set = set(std_raw_aff.strip().split(' '))

        # Managing missing space in raw affiliations related to particuliar institutions cases such as UMR or U followed by digits
        for accron in MISSING_SPACE_ACRONYMS:
            re_accron = re.compile(accronymes_template.substitute({"word":accron}))
            if re.search(re_accron,std_raw_aff.lower()) and len(raw_aff_words_set)==2:
                std_raw_aff_add = "".join(std_raw_aff.split(" "))

        # Droping small words
        # print('       raw_aff_words_set_init:', raw_aff_words_set)
        for word_to_drop in SMALL_WORDS_DROP:
            re_drop_words = re.compile(small_words_template.substitute({"word":word_to_drop}))
            if re.search(re_drop_words,std_raw_aff.lower()):
                raw_aff_words_set = raw_aff_words_set - {word_to_drop}

        # Updating raw_aff_words_set_list using std_raw_aff_add
        raw_aff_words_set_add = {}
        if std_raw_aff_add:
            raw_aff_words_set_add = set(std_raw_aff_add.split(' '))

        #print('       raw_aff_words_set_final:', raw_aff_words_set)
        #print('       raw_aff_words_set_add:', raw_aff_words_set_add)

        return (raw_aff_words_set, raw_aff_words_set_add)


    def _build_words_sets_list(raw_aff_list): 
        '''The internal function `_build_words_sets_list` builds a list of words sets from a list of raw affiliations.   
        This function calls the internal function `_build_words_set`.

        Args: 
            raw_aff_list (list): The list of raw affiliations as strings.

        Returns:
            (list): List of words sets.

        '''

        raw_aff_words_sets_list = []
        for idx, raw_aff in enumerate(raw_aff_list):
            if raw_aff and raw_aff!=' ':
                #print('     ' + str(idx) + '- Raw affiliation:', raw_aff)
                
                # Building the set of words for raw affiliation
                raw_aff_words_set, raw_aff_words_set_add = _build_words_set(raw_aff)

                # Upadating the list of words sets with the set raw_aff_words_set 
                raw_aff_words_sets_list.append(raw_aff_words_set)

                # Upadating the list of words sets using the set raw_aff_words_set_add
                if raw_aff_words_set_add: raw_aff_words_sets_list.append(raw_aff_words_set_add)

            #print('    raw_aff_words_sets_list:', raw_aff_words_sets_list)
            #print()

        return raw_aff_words_sets_list

    ################################################# Local functions end #################################################  
    
    # Setting the path for the 'Country_affilialions.xlsx' file
    if not country_affiliations_file_path:
        country_affiliations_file_path = Path(bau.__file__).parent / Path(REP_UTILS) / Path(COUNTRY_AFFILIATIONS_FILE)
   
    # Reading the 'Country_affilialions.xlsx' file in the dataframe dic    
    wb = openpyxl.load_workbook(country_affiliations_file_path)
    country_aff_df = pd.read_excel(country_affiliations_file_path, 
                                   sheet_name=wb.sheetnames)

    norm_raw_aff_dict = {}
    for country_aff_df_item in country_aff_df.items():
        country = country_aff_df_item[0]
        norm_raw_aff_dict[country] = {}
        norm_raw_aff_df = country_aff_df_item[1]    
        norm_raw_aff_nb = len(norm_raw_aff_df["Norm affiliations"])

        if verbose:
            print('Country:', country)
            print('Number of norm affiliations:', norm_raw_aff_nb)
            print()
            print('List of norm affiliations:', norm_raw_aff_df["Norm affiliations"])
            print()

        for num, norm_aff in enumerate(norm_raw_aff_df["Norm affiliations"]):
            raw_aff_list = [item for item in list(norm_raw_aff_df.loc[num])[1:] if not(pd.isnull(item)) == True]

            if verbose: 
                print()
                print()
                print(str(num) + '- Norm affiliation:', norm_aff)
                print('   Raw affiliations list:',raw_aff_list)
                print()

            norm_raw_aff_dict[country][norm_aff] = _build_words_sets_list(raw_aff_list)

            if verbose:
                print('  norm_raw_aff_dict[' + country + ']['+ norm_aff + ']:', norm_raw_aff_dict[country][norm_aff])
                print()
            
    return norm_raw_aff_dict


def read_inst_types(inst_types_file_path = None, inst_types_usecols = None):
    '''The function `read_inst_types` builds a dict keyyed by normalized affiliations types.
    The value per type is the order level of the type.
   
    Args:
        inst_types_file_path (path): The full path to the file "Inst_types.xlsx"; 
                                     if None, it is set using the globals 'INST_TYPES_FILE' 
                                     and 'REP_UTILS'.
        inst_types_usecols (list of str): The list of columns names for order levels and abbreviations 
                                          of affiliation types in the file "Inst_types.xlsx";
                                          if None, it is set using the global 'INST_TYPES_USECOLS'. 
        
    Returns:
        (dict): The built dict.
    

    '''

    # Standard library imports
    from pathlib import Path

    # 3rd party imports
    import pandas as pd

    # Local imports
    import BiblioAnalysis_Utils as bau
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import INST_TYPES_FILE 
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import INST_TYPES_USECOLS 
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import REP_UTILS
    
    # Setting the full path for the 'Inst_types.xlsx' file
    if not inst_types_file_path:
        inst_types_file = INST_TYPES_FILE
        inst_types_file_path = Path(bau.__file__).parent / Path(REP_UTILS) / Path(inst_types_file)
        
    if not inst_types_usecols:
        inst_types_usecols = INST_TYPES_USECOLS

    # Reading the 'Country_affilialions.xlsx' file in the dataframe dic

    inst_types_df = pd.read_excel(inst_types_file_path, usecols = INST_TYPES_USECOLS)

    levels = [x for x in inst_types_df['Level'] ]
    abbreviations = [x for x in inst_types_df['Abbreviation'] ]
    aff_type_dict = dict(zip(abbreviations, levels))

    return aff_type_dict


def get_norm_affiliations_list(country, affiliations_list, norm_raw_aff_dict, aff_type_dict, verbose=False):
    '''
    
    '''

    # Standard library imports
    import re
    from string import Template

    # 3rd party imports

    # Local imports
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import COUNTRY_AFFILIATIONS_FILE 
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import REP_UTILS
    from BiblioAnalysis_Utils.BiblioParsingUtils import remove_special_symbol    
            
    set_words_template = Template(r'[\s]$word[\s)]'     # For instence capturing "word" in "word of set" 
                                                        # or " word" in "set with word".
                                                        # or "word" in "Azert Word Azerty".
                                  + '|'
                                  + r'[\s]$word$$'
                                  + '|'
                                  + r'^$word\b')

    address_norm_affiliations_list = []
    address_unknown_affiliations_list = [] 
    for affiliation in affiliations_list:
        if verbose: print(' -', affiliation)
        norm_affiliation_list = []

        # Removing accents and converting to lower case
        aff_mod = remove_special_symbol(affiliation, only_ascii = False, strip = True)
        aff_mod = aff_mod.lower()
        if verbose:
            print()
            print('aff_mod:',aff_mod)
            print()

        # Searching for words set in affiliation
        for num, norm_aff in enumerate(norm_raw_aff_dict[country].keys()):

            if verbose:
                print()
                print(str(num) + ' norm_aff:', norm_aff)
                print()

            for words_set in norm_raw_aff_dict[country][norm_aff]:
                if verbose :print('  words_set:', words_set)
                words_set_tags = []
                for word in words_set:
                    if verbose: print('    word:', word)
                    re_search_words = re.compile(set_words_template.substitute({"word":word}))
                    if re.search(re_search_words,aff_mod) :
                        words_set_tags.append('true')
                        if verbose: print('     words_set_tags:',words_set_tags)
                    else:
                        words_set_tags.append('false')
                        if verbose: print('     words_set_tags:',words_set_tags)

                if 'false' not in words_set_tags:               
                    norm_affiliation_list.append(norm_aff)

                if verbose: 
                    print('  words_set_tags:',words_set_tags)
                    print('  norm_affiliation_list:', norm_affiliation_list)
                    print()

        if verbose: print('  norm_affiliation_list:', norm_affiliation_list)

        if norm_affiliation_list==[]:
            address_unknown_affiliations_list.append(affiliation)

        address_norm_affiliations_list = address_norm_affiliations_list + norm_affiliation_list 

    address_norm_affiliations_set = set(address_norm_affiliations_list)
    if verbose: 
        print('address_norm_affiliations_list:',address_norm_affiliations_list)
        print('address_norm_affiliations_set:     ',address_norm_affiliations_set)
    
    paris_nb = 0
    for norm_aff in address_norm_affiliations_set:
        if 'Univ' in norm_aff and 'Paris' in norm_aff: paris_nb+=1

    if paris_nb >1 and 'Paris-Cité Univ' in address_norm_affiliations_set:
        address_norm_affiliations_set = address_norm_affiliations_set - {'Paris-Cité Univ'}
    if verbose: print('address_norm_affiliations_set:     ',address_norm_affiliations_set)
    
    idx_dict = dict(zip(aff_type_dict.keys(),[0 ]* len(aff_type_dict.keys())))
    norm_aff_pos_list = []
    address_norm_affiliation_dict = {}      
    for norm_aff in address_norm_affiliations_set:        
        norm_aff_type = norm_aff.split(' ')[-1]
        if verbose:
            print('norm_aff_type:', norm_aff_type)
            print('str(idx_dict[norm_aff_type]):', str(idx_dict[norm_aff_type]))
        
        norm_aff_pos = str(aff_type_dict[norm_aff_type]) + str(idx_dict[norm_aff_type])
        if verbose: 
            print('norm_aff_pos init:',norm_aff_pos)
            print('norm_aff_pos_list init:', norm_aff_pos_list)
        if int(norm_aff_pos) in  norm_aff_pos_list : 
            idx_dict[norm_aff_type]+=1
            
        norm_aff_pos = str(aff_type_dict[norm_aff_type]) + str(idx_dict[norm_aff_type])
        if verbose: 
            print('norm_aff_pos end:',norm_aff_pos)
            print('idx_dict[norm_aff_type]:', idx_dict[norm_aff_type])
        
        
        norm_aff_pos_list.append(int(norm_aff_pos))
        if verbose: 
            print('norm_aff_pos_list end:', norm_aff_pos_list)
            print()
   
        address_norm_affiliation_dict[norm_aff_pos] = norm_aff 
        
    if verbose: print('address_norm_affiliation_dict:     ', address_norm_affiliation_dict) 
    norm_aff_pos_list.sort()

    address_norm_affiliation_list = [None] * len(address_norm_affiliations_set)
    for idx in range(len(norm_aff_pos_list)):
        address_norm_affiliation_list[idx] = address_norm_affiliation_dict[str(norm_aff_pos_list[idx])]
    
    return (address_norm_affiliation_list, address_unknown_affiliations_list)


def build_address_affiliations_lists(raw_address, norm_raw_aff_dict, aff_type_dict, verbose = False):
    '''The function `build_address_affiliations_lists` builds the list of normalized affiliations
    for the raw address 'raw_address' after standardization.
    It also returns the country and the unknown affiliations for this address. 
    
    Args:
        raw_address (str): The address for which the list of normalized affiliations is built.
        norm_raw_aff_dict (dict): The dict built by the function `build_norm_raw_affiliations_dict`.
        aff_type_dict (dict): The dict built by the function `read_inst_types`
    
    Returns:
        (tuple): A tuple of 3 items; 
                 first item is the country asstring; 
                 second item is the list of normalized affiliations;
                 third item is the list of unknown affiliations.
    
    Note:
        The functions 'standardize_address' and 'get_affiliations_list' are imported from    .
        The function 'get_norm_affiliations_list' is imported from    .
    '''

    from BiblioAnalysis_Utils.BiblioGeneralGlobals import SYMB_CHANGE
    
    std_address = standardize_address(raw_address)
    if verbose:
        print()
        print('Standardized address:              ', std_address)

    country, affiliations_list, affiliations_drop = get_affiliations_list(std_address, drop_to_end = None, verbose = False)
    affiliations_list_mod = [affiliation.translate(SYMB_CHANGE) for affiliation in affiliations_list]
    
    if verbose:
        print()
        print('Country:                           ', country)
        print()
        print('Affiliations list:                 ', affiliations_list)
        print('Modified affiliations list:        ', affiliations_list_mod)
        print('Affiliations dropped:              ', affiliations_drop) 

    if country in norm_raw_aff_dict.keys():
        address_norm_affiliation_list, address_unknown_affiliations_list = get_norm_affiliations_list(country, affiliations_list_mod, norm_raw_aff_dict, aff_type_dict, verbose=False)
    else:
        address_norm_affiliation_list = []
        address_unknown_affiliations_list = affiliations_list
    
    return (country, address_norm_affiliation_list, address_unknown_affiliations_list)


def build_addresses_institutions(path_parsing, norm_raw_aff_dict, aff_type_dict):
    '''The function `build_addresses_institutions` builds a dataframe of addresses
    with the corresponding country, normalized institutions and unknown institutions 
    and saves it in a new .dat file which name is given by the global 'DIC_OUTDIR_PARSING[ADI]'. 
    It uses the .dat file of the corpus addresses which name is given by the global 
    'DIC_OUTDIR_PARSING[AD]'.
    
    Args:
        path_parsing (path): Path to the .dat file of addresses.
        norm_raw_aff_dict (dict): The dict giving the normalized affiliations per country  
                                  with the respective list of words sets describing 
                                  each mormalized affiliations.
        aff_type_dict (dict): The dict giving the order level of each institution type 
                              for sorting the list of normalized institutions.

    Retruns:
        (str): Function status message.
        
    Notes:
        The globals 'COL_NAMES' and 'DIC_OUTDIR_PARSING' from `BiblioSpecificGlobals` module 
        of `BiblioAnalysis_Utils` package are used.
    
    '''
    
    # Standard libraries import
    from pathlib import Path

    # 3rd party imports
    import pandas as pd

    # Local imports
    from BiblioAnalysis_Utils.BiblioParsingInstitutions import build_address_affiliations_lists
    
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import COL_NAMES
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import DIC_OUTDIR_PARSING

    def _address_aff_list(raw_address):
        # get the tuple (country, addresse_norm_affiliations_list, address_unknown_affiliations_list)
        tup = build_address_affiliations_lists(raw_address, 
                                               norm_raw_aff_dict, 
                                               aff_type_dict, 
                                               verbose = False) 
        return tup 

    # Setting global aliases
    addresses_dat_alias     = DIC_OUTDIR_PARSING['AD']
    add_inst_dat_alias      = DIC_OUTDIR_PARSING['ADI']
    pub_id_alias            = COL_NAMES['address'][0]
    idx_address_alias       = COL_NAMES['address'][1]
    address_alias           = COL_NAMES['address'][2]
    country_alias           = COL_NAMES['address_inst'][3]
    norm_institutions_alias = COL_NAMES['address_inst'][4]
    raw_institutions_alias  = COL_NAMES['address_inst'][5]
    
    # Setting local aliases
    temp_institutions_alias = "Full institutions"

    # Reading the '.dat' file                   
    read_usecols = [pub_id_alias, idx_address_alias, address_alias]     
    df_AD= pd.read_csv(Path(path_parsing) / Path(addresses_dat_alias),
                       sep='\t',
                       usecols=read_usecols)


    # Building the "institutions_alias" column in the 'df_AD' dataframe
    #df_AD[country_alias], df_AD[norm_institutions_alias], df_AD[raw_institutions_alias]  = df_AD.apply(lambda row:
    df_AD[temp_institutions_alias] = df_AD.apply(lambda row:
                                                 _address_aff_list(row[address_alias]),
                                                 axis = 1)

    # Splitting in 3 columns the tupple of each item of the column named temp_institutions_alias of dg
    df_inst_split = pd.DataFrame(df_AD[temp_institutions_alias].sort_index().to_list(),
                                 columns=[country_alias,norm_institutions_alias,raw_institutions_alias])

    # Converting the list of strings of each item of a given column of df_inst_split, in a string of items joined by ';'
    norm_inst_list = ['; '.join(item) for item in df_inst_split[norm_institutions_alias]]
    raw_inst_list = ['; '.join(item) for item in df_inst_split[raw_institutions_alias]]

    # Building dataframes convenient for adding  the 3 columns splitted from column named temp_institutions_alias to df_AD
    df_countries = df_inst_split[country_alias]
    df_norm_institutions = pd.DataFrame(norm_inst_list, columns = [norm_institutions_alias])
    df_raw_institutions = pd.DataFrame(raw_inst_list, columns = [raw_institutions_alias])

    # Adding to df_AD the 3 columns splitted from column named temp_institutions_alias
    df_AD = pd.concat([df_AD, df_countries, df_norm_institutions, df_raw_institutions], axis = 1)

    # Droping in df_AD the column named temp_institutions_alias which is no more useful
    df_AD.drop([temp_institutions_alias], axis=1, inplace=True)

    # Saving the extended 'df_AD' dataframe in a new '.dat' file 
    df_AD.to_csv(Path(path_parsing) / Path(add_inst_dat_alias), 
                 index=False,
                 sep='\t') 
    
    message = 'File "'+ add_inst_dat_alias + '" created or updated in: \n      ' + str(Path(path_parsing) / Path(addresses_dat_alias))
    return message