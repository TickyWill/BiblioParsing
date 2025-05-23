__all__ = ['address_inst_full_list',
           'build_norm_raw_affiliations_dict',
           'build_norm_raw_institutions',
           'extend_author_institutions',
           'read_inst_types',
           'read_towns_per_country',
           'standardize_address',
           ]


def _get_norm_affiliations_list(country, affiliations_list, norm_raw_aff_dict, 
                                aff_type_dict, verbose = False):
    '''
    
    '''

    # Standard library imports
    import re
    from string import Template

    # Local library imports
    from BiblioParsing.BiblioParsingUtils import remove_special_symbol
    
    # Globals imports
    from BiblioParsing.BiblioGeneralGlobals import REP_UTILS
        
            
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
                        if verbose: print('     words_set_tags:', words_set_tags)
                    else:
                        words_set_tags.append('false')
                        if verbose: print('     words_set_tags:', words_set_tags)

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
        print('address_norm_affiliations_list:', address_norm_affiliations_list)
        print('address_norm_affiliations_set:     ', address_norm_affiliations_set)
    
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


def _build_address_affiliations_lists(std_address, norm_raw_aff_dict, aff_type_dict, 
                                      towns_dict, drop_status, verbose = False):
    '''The function `_build_address_affiliations_lists` builds the list of normalized affiliations
    for the standardized address 'std_address'.
    It also returns the country and the unknown affiliations for this address. 
    
    Args:
        std_address (str): The standardized address for which the list of normalized affiliations is built.
        norm_raw_aff_dict (dict): The dict built by the function `build_norm_raw_affiliations_dict`.
        aff_type_dict (dict): The dict built by the function `read_inst_types`.
        drop_status (boolean): If true, droping items are searched to drop chunks from the address.
    
    Returns:
        (tuple): A tuple of 3 items; 
                 first item is the country asstring; 
                 second item is the list of normalized affiliations;
                 third item is the list of unknown affiliations.
    
    Note:
        The functions '_get_affiliations_list' and '_get_norm_affiliations_list'
        are imported from  BiblioParsingInstitutions module BiblioParsing package.
    '''
    
    # Globals imports
    from BiblioParsing.BiblioGeneralGlobals import SYMB_CHANGE

    if verbose:
        print()
        print('Standardized address:              ', std_address)

    return_tup = _get_affiliations_list(std_address, towns_dict, drop_status = drop_status, verbose = False)
    country, affiliations_list, affiliations_drop = return_tup
    affiliations_list_mod = [affiliation.translate(SYMB_CHANGE) for affiliation in affiliations_list]
    
    if verbose:
        print()
        print('Country:                           ', country)
        print()
        print('Affiliations list:                 ', affiliations_list)
        print('Modified affiliations list:        ', affiliations_list_mod)
        print('Affiliations dropped:              ', affiliations_drop) 

    if country in norm_raw_aff_dict.keys():
        return_tup = _get_norm_affiliations_list(country, affiliations_list_mod, norm_raw_aff_dict, 
                                                 aff_type_dict, verbose = False)
        address_norm_affiliation_list, address_unknown_affiliations_list = return_tup
    else:
        address_norm_affiliation_list = []
        address_unknown_affiliations_list = affiliations_list
    
    return (country, address_norm_affiliation_list, address_unknown_affiliations_list)


def address_inst_full_list(full_address, norm_raw_aff_dict, aff_type_dict, towns_dict, drop_status):
    """The `address_inst_full_list` function allows building the affiliations list of a full address
    using the `_build_address_affiliations_lists` internal function of `BiblioParsingInstitutions` module.
    
    Args:
        full_address (str): the full address to be parsed in institutions and country.
        norm_raw_aff_dict (dict): a dict used for the normalization of the institutions names, 
                                 with the normalized names as keys and the raw names as values.
        aff_type_dict (dict): a dict used to set the order of the normalized names of institutions 
                              by institution type.
        drop_status (boolean): If true, droping items are searched to drop chunks from the address.
        
    Returns:
        (namedtuple): tuple of two strings. 
                      - The first is the joined list of normalized institutions names 
                      found in the full address.
                      - The second is the joined list of raw institutions names of the full address 
                      with no fully corresponding normalized names.
        
    Notes:
        The global 'EMPTY' is imported from `BiblioSpecificGlobals` module 
        of `BiblioParsing` package.
    """

    # Standard library imports
    from collections import namedtuple

    # Globals imports
    from BiblioParsing.BiblioSpecificGlobals import EMPTY

    inst_full_list_ntup = namedtuple('inst_full_list_ntup',['norm_inst_list','raw_inst_list'])

    aff_list_tup = _build_address_affiliations_lists(full_address, norm_raw_aff_dict,
                                                     aff_type_dict, towns_dict, drop_status,
                                                     verbose = False)
    country, norm_inst_full_list, raw_inst_full_list = aff_list_tup

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
    inst_full_list_tup =  inst_full_list_ntup(norm_inst_full_list_str, raw_inst_full_list_str)

    return inst_full_list_tup


def extend_author_institutions(item_df, inst_filter_list):
    """ The `extend_author_institutions` function extends the df of authors with institutions 
    initialy obtained by the parsing of the corpus, with complementary information about institutions
    selected by the user.
    
    Args:
        item_df (df): Dataframe of authors with institutions.
        inst_filter_list (list): the affiliations filter list of tuples (institution, collumn name). 

    Retruns:
        (df): The extended dataframe.
        
    Notes:
        The global 'COL_NAMES' is imported from `BiblioSpecificGlobals` module 
        of `BiblioParsing` package.
    
    """

    # 3rd party imports
    import pandas as pd

    # Globals imports
    from BiblioParsing.BiblioSpecificGlobals import COL_NAMES

    def _address_inst_list(inst_names_list, institutions):
        secondary_institutions = []
        for inst in inst_names_list:
            if inst in institutions:
                secondary_institutions.append(1)
            else:
                secondary_institutions.append(0)
        return secondary_institutions

    # Setting useful column names aliases
    institutions_alias = COL_NAMES['auth_inst'][4]
    temp_col_alias     = "temp_col"

    # Getting the useful columns of the item df                   
    read_usecols = [COL_NAMES['auth_inst'][x] for x in [0,1,2,3,4]]
    item_df      = item_df[read_usecols]

    # Setting an institution name for each of the institutions indicated in the institutions filter
    inst_names_list = [f'{x[0]}' for x in inst_filter_list]
    inst_col_list   = [f'{x[1]}' for x in inst_filter_list]

    # Building the "sec_institution_alias" column in the 'item_df' dataframe using "inst_filter_list"
    item_dg = item_df.copy()
    item_dg[temp_col_alias] = item_dg.apply(lambda row: _address_inst_list(inst_names_list,
                                                                           row[institutions_alias]), axis = 1)
    item_dg.reset_index(inplace = True, drop = True)

    # Distributing in a 'inst_split_df' df the value lists of 'item_dg[temp_col_alias]' column
    # into columns which names are in 'inst_col_list' list
    inst_split_df = pd.DataFrame(item_dg[temp_col_alias].sort_index().to_list(),
                                 columns = inst_col_list)

    # Extending the 'df' dataframe with 'inst_split_df' dataframe
    new_item_df = pd.concat([item_dg, inst_split_df], axis = 1)

    # Droping the 'df[temp_col_alias]' column which is no more useful
    new_item_df.drop([temp_col_alias], axis = 1, inplace = True)

    return new_item_df


def _standardize_address_old(raw_address):
    
    '''The function `_standardize_address` standardizes the string 'raw_address' by replacing
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
        of the package `BiblioParsing`.
        The globals 'DASHES_CHANGE' and 'APOSTROPHE_CHANGE' are imported from the module `BiblioGeneralGlobals`  
        of the package `BiblioParsing`.
        The function `normalize_country` is imported from the module `BiblioParsingInstitutions`
        of the package `BiblioParsing`.
        
    '''
    
    # Standard library imports
    import re
    
    # Local library imports
    from BiblioParsing.BiblioParsingUtils import normalize_country
    
    # Globals imports
    from BiblioParsing.BiblioGeneralGlobals import APOSTROPHE_CHANGE
    from BiblioParsing.BiblioGeneralGlobals import DASHES_CHANGE
    from BiblioParsing.BiblioSpecificGlobals import DIC_WORD_RE_PATTERN
    from BiblioParsing.BiblioSpecificGlobals import UNKNOWN
    
    # Uniformizing words
    standard_address = raw_address
    for word_to_subsitute, re_pattern in DIC_WORD_RE_PATTERN.items():
        standard_address = re.sub(re_pattern,word_to_subsitute + ' ', standard_address)
    standard_address = re.sub(r'\s+', ' ', standard_address)
    standard_address = re.sub(r'\s,', ',', standard_address)
    
    # Uniformizing dashes
    standard_address = standard_address.translate(DASHES_CHANGE)
    
    # Uniformizing apostrophes
    standard_address = standard_address.translate(APOSTROPHE_CHANGE)
    
    # Uniformizing countries
    country_pos = -1
    first_raw_affiliations_list = standard_address.split(',')
    # This split below is just for country finding even if affiliation may be separated by dashes
    raw_affiliations_list = sum([x.split(' - ') for x in first_raw_affiliations_list], [])        
    country = normalize_country(raw_affiliations_list[country_pos].strip())
    space = " "
    if country != UNKNOWN:
        standard_address = ','.join(first_raw_affiliations_list[:-1] + [space + country])
    else:
        standard_address = ','.join(first_raw_affiliations_list + [space + country])

    return standard_address


def standardize_address(raw_address):
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
    Finally, the country is normalized through the `normalize_country` function imported from 
    the `BiblioParsing` package imported as "bp".

    Args:
        raw_address (str): The full address to be standardized.
    Returns:
        (str): The full standardized address.
    """
    # Standard library imports
    import re
    
    # Local library imports
    from BiblioParsing.BiblioParsingUtils import normalize_country
    
    # Globals imports
    from BiblioParsing.BiblioGeneralGlobals import APOSTROPHE_CHANGE
    from BiblioParsing.BiblioGeneralGlobals import DASHES_CHANGE
    from BiblioParsing.BiblioGeneralGlobals import SYMB_DROP
    from BiblioParsing.BiblioSpecificGlobals import DIC_WORD_RE_PATTERN
    from BiblioParsing.BiblioSpecificGlobals import UNKNOWN

    # Uniformizing words
    standard_address = raw_address
    for word_to_substitute, re_pattern in DIC_WORD_RE_PATTERN.items():
        if word_to_substitute=='University':
            re_pattern = re.compile(r'\b[a-z]?Univ[aàäcdeéirstyz]{0,8}\b\.?')
        standard_address = re.sub(re_pattern, word_to_substitute + ' ', standard_address)
    standard_address = re.sub(r'\s+', ' ', standard_address)
    standard_address = re.sub(r'\s,', ',', standard_address)

    # Uniformizing dashes
    standard_address = standard_address.translate(DASHES_CHANGE)

    # Uniformizing apostrophes
    standard_address = standard_address.translate(APOSTROPHE_CHANGE)

    # Dropping symbols
    standard_address = standard_address.translate(SYMB_DROP)

    # Uniformizing countries
    country_pos = -1
    first_raw_affiliations_list = standard_address.split(',')
    # This split below is just for country finding even if affiliation may be separated by dashes
    raw_affiliations_list = sum([x.split(' - ') for x in first_raw_affiliations_list], [])
    country = normalize_country(raw_affiliations_list[country_pos].strip())
    space = " "
    if country!=UNKNOWN:
        standard_address = ','.join(first_raw_affiliations_list[:-1] + [space + country])
    else:
        standard_address = ','.join(first_raw_affiliations_list + [space + country])
    return standard_address


def _search_items(affiliation, country, towns_dict, verbose = False):
    
    '''The function `_search_items` searches for several item types in 'affiliation' after accents removal 
    and converting in lower case even if the search is case sensitive.
    It uses the following internal functions:
        - The function `_search_droping_bp` searches for words that are postal-box numbers such as 'BP54'.
        - The function `_search_droping_digits` searches for words that contains digits such as zip codes 
        which templates are given per country by the global 'ZIP_CODES' dict.
        - The function `_search_droping_suffix` searches for words ending by a suffix among 
        those given by the global 'DROPING_SUFFIX' such as 'strasse' in 'helmholtzstrasse'.
        - The function `_search_droping_town` searches for words that are towns listed 
        in the dict 'towns_dict'.
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
        of the package `BiblioParsing`.
        The globals 'DROPING_SUFFIX', 'DROPING_WORDS', 'KEEPING_PREFIX' are imported from the module `BiblioSpecificGlobals`  
        of the package `BiblioParsing`.
        The global 'ZIP_CODES' is imported from the module `BiblioGeneralGlobals`  
        of the package `BiblioParsing`.
    
    '''
    
    # Standard library imports
    import re
    from collections import namedtuple
    from string import Template
    
    # Local library imports 
    from BiblioParsing.BiblioParsingUtils import remove_special_symbol
    from BiblioParsing.BiblioParsingUtils import rationalize_town_names
    
    # Globals imports
    from BiblioParsing.BiblioGeneralGlobals import ZIP_CODES        
    from BiblioParsing.BiblioSpecificGlobals import BASIC_KEEPING_WORDS
    from BiblioParsing.BiblioSpecificGlobals import DROPING_SUFFIX
    from BiblioParsing.BiblioSpecificGlobals import DROPING_WORDS    
    from BiblioParsing.BiblioSpecificGlobals import FR_DROPING_WORDS
    from BiblioParsing.BiblioSpecificGlobals import GEN_KEEPING_WORDS
    from BiblioParsing.BiblioSpecificGlobals import KEEPING_PREFIX
    from BiblioParsing.BiblioSpecificGlobals import KEEPING_WORDS
    from BiblioParsing.BiblioSpecificGlobals import USER_KEEPING_WORDS    
    
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
        result = re.search(re_bp, text.lower())
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
                print('Droping word is a zip code') if zip_result else print('Droping word is a digits code')   
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
            result = re.search(re_drop_words, text.lower())
            if result is not None:
                flag = True
                if verbose:
                    print('Droping word contains the suffix:', word_to_drop)
        return [flag]
   
    def _search_droping_town(text):
        '''The internal function `_search_droping_town` searches in 'text' for words in lower case
        that are towns listed in the dict 'towns_dict' for each country.
        
        Args:
            text (str): the string where the words are searched after being converted to lower case.
            
        Returns:
            (boolean): True if a word listed in the dict 'towns_dict' is equal to 'text' 
                       after spaces removal at ends.
                       
        Notes:
            
        '''
        flag = False
        text_mod = rationalize_town_names(text.lower())
        if country in towns_dict.keys():
            for word_to_drop in towns_dict[country]:
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
            result = re.search(re_drop_words, text.lower())
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
                result = re.search(re_keep_prefix, text.lower())
                if result is not None:
                    if verbose:
                        print('Keeping word is the prefix:', prefix_to_keep)
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
            result = re.search(re_keep_words, text.lower())
            if result is not None:
                if verbose:
                    print('Keeping word is the full word:', word_to_keep)
                if word_to_keep in GEN_KEEPING_WORDS : gen_flag = True
                if word_to_keep in BASIC_KEEPING_WORDS : basic_flag = True 
                if word_to_keep in USER_KEEPING_WORDS : user_flag = True 

        return [gen_flag, basic_flag, user_flag]    
    #################################### Internal functions end ####################################
    
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
    flag_list = sum(flag_list, [])
    found_item_flags = found_item_tup(*flag_list)

    return found_item_flags


def _get_affiliations_list(std_address, towns_dict, drop_status = True, verbose = False):
    
    '''The function `_get_affiliations_list` extracts first, the country and then, the list 
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
        drop_status (boolean): If true, droping items are searched to drop chunks from the address
                               (default: True).
        verbose (boolean): If true, printing of intermediate variables is allowed (default: False). 
        
    Returns:
        (tuple): A tuple composed of 3 items (list of kept chuncks, country and list of dropped chuncks).
        
    Notes:
        The function `search_droping_items` is imported from the module `BiblioParsingInstitutions`  
        of the package`BiblioParsing`.
        The function `normalize_country` is imported from the module `BiblioParsingInstitutions` 
        of the package`BiblioParsing`.
        The globals 'KEEPING_WORDS', 'KEEPING_PREFIX' and 'UNKNOWN' are imported from the module `BiblioSpecificGlobals` 
        of the package`BiblioParsing`.        
        
    '''
    
    # Standard library imports
    import re
    from string import Template
    
    # Local library imports
    import BiblioParsing as bp
    
    # Splitting by coma the standard address in chuncks listed in an initial-affiliations list
    init_raw_affiliations_list = std_address.split(',')

    # Removing the first occurence of chunck duplicates from the initial-affiliations list
    # and putting them in a deduplicated-affiliations list
    drop_aff_idx_list = []
    for idx1, aff1 in enumerate(init_raw_affiliations_list): 
        drop_aff_idx_list.extend([min(idx1, idx2) for idx2, aff2 in enumerate(init_raw_affiliations_list) 
                                  if idx1!=idx2 and aff1==aff2])
    dedup_raw_affiliations_list = []    
    dedup_raw_affiliations_list.extend([aff for idx, aff in enumerate(init_raw_affiliations_list) 
                                        if idx not in set(drop_aff_idx_list)])             

    # Setting country index in raw-affiliations list
    country_pos = -1
    country = dedup_raw_affiliations_list[country_pos].strip()

    # Splitting by special characters the deduplicated chunks and putting them in a raw-affiliations list
    raw_affiliations_list = sum([x.split(' - ') for x in dedup_raw_affiliations_list], [])
    raw_affiliations_list = sum([x.split(' | ') for x in raw_affiliations_list], [])

    if drop_status:
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
                found_item_flags = _search_items(affiliation, country, towns_dict, verbose = verbose)
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
                    if verbose: print('No droping item found in:', affiliation, '\n')

                else:                
                    if found_item_flags.droping_bp:
                        affiliations_drop.append(('droping_bp', check_affiliations_list[index:country_pos]))
                        break_id = 'droping_bp'
                        if verbose: print('Break identification:', break_id, '\n')
                        break 

                    if found_item_flags.droping_digits:

                        if country.lower() in ['france','algeria']:
                            if not found_item_flags.keeping_prefix and not any(keeping_words_flags):
                                affiliations_drop.append(('droping_digits', check_affiliations_list[index:country_pos]))
                                break_id = 'droping_digits'
                                if verbose: print('Break identification:', break_id, '\n')
                                break
                            elif found_item_flags.gen_keeping_words:
                                if not add_affiliation_flag: 
                                    affiliations_list.append(affiliation)
                                    add_affiliation_flag = True
                                break_id = 'droping_digits aborted by gen_keeping_words'     
                                if verbose: print('Break identification:', break_id, '\n')
                            else: 
                                if not add_affiliation_flag: 
                                    affiliations_list.append(affiliation)
                                    add_affiliation_flag = True
                                if found_item_flags.basic_keeping_words: break_id = 'droping_digits aborted by basic_keeping_words'
                                if found_item_flags.user_keeping_words : break_id = 'droping_digits aborted by user_keeping_words'
                                if found_item_flags.keeping_prefix: break_id = 'droping_digits aborted by keeping_prefix'
                                if verbose: print('Break identification:', break_id, '\n')                    
                        else:
                            if not found_item_flags.gen_keeping_words and not found_item_flags.user_keeping_words:
                                affiliations_drop.append(('droping_digits',check_affiliations_list[index:country_pos]))
                                break_id = 'droping_digits'
                                if verbose: print('Break identification:', break_id, '\n')
                                break
                            elif found_item_flags.droping_words:
                                affiliations_drop.append(('droping_digits',check_affiliations_list[index:country_pos]))
                                break_id = 'droping_digits'
                                if verbose: print('Break identification:', break_id, '\n')
                                break
                            else:
                                if not add_affiliation_flag: 
                                    affiliations_list.append(affiliation)
                                    add_affiliation_flag = True
                                    break_id = 'droping_digits aborted by user_keeping_words'
                                    if verbose: print('Break identification:', break_id, '\n')

                    if found_item_flags.droping_town:
                        if len(check_affiliations_list[index:country_pos])<=2:   
                            affiliations_drop.append(('droping_town', check_affiliations_list[index:country_pos]))
                            break_id = 'droping_town'
                            if verbose: print('Break identification:', break_id, '\n')    
                            break
                        else:
                            affiliations_drop.append(('droping_town', affiliation))
                            break_id = 'droping_town aborted by index of town in affiliations list'
                            if verbose: print('Break identification:', break_id, '\n')

                    else:
                        if found_item_flags.droping_suffix:
                            if found_item_flags.gen_keeping_words or found_item_flags.user_keeping_words:
                                if not add_affiliation_flag: 
                                    affiliations_list.append(affiliation)
                                    add_affiliation_flag = True
                                if found_item_flags.gen_keeping_words: break_id = 'droping_suffix aborted by gen_keeping_words'
                                if found_item_flags.user_keeping_words: break_id = 'droping_suffix aborted by user_keeping_words'
                                if verbose: print('Break identification:', break_id, '\n')    
                            else:
                                affiliations_drop.append(('droping_suffix',check_affiliations_list[index:country_pos]))
                                break_id = 'droping_suffix'
                                if verbose: print('Break identification:', break_id, '\n')
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
                                if verbose: print('Break identification:', break_id, '\n')
                            else:
                                # Droping affiliation from affiliations_list if already added because of a former drop abort
                                if add_affiliation_flag:
                                    affiliations_list = affiliations_list[:-1]
                                    add_affiliation_flag = False
                                affiliations_drop.append(('droping_words', check_affiliations_list[index:country_pos]))
                                break_id = 'droping_words'
                                if verbose: print('Break identification:', break_id, '\n')
                                break             
    else:
        affiliations_list = raw_affiliations_list
        affiliations_drop = []

    # Removing spaces from the kept affiliations 
    affiliations_list = [x.strip() for x in affiliations_list]
    if verbose:
        print('affiliations_list stripped:', affiliations_list)
        print()
        
    # Removing country and country alias from the kept affiliations 
    affiliations_list = [x for x in affiliations_list if x != country and x not in bp.ALIAS_UK]        
    if verbose:
        print('affiliations_list without country aliases:', affiliations_list)
        print()
    
    return (country, affiliations_list, affiliations_drop)


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
        remove_special_symbol from BiblioParsing.BiblioParsingUtils
        COUNTRY_AFFILIATIONS_FILE, DIC_WORD_RE_PATTERN, MISSING_SPACE_ACRONYMS, SMALL_WORDS_DROP, REP_UTILS
        APOSTROPHE_CHANGE, DASHES_CHANGE, SYMB_DROP, SYMB_CHANGE
    
    '''

    # Standard library imports
    from pathlib import Path

    # 3rd party imports
    import openpyxl
    import pandas as pd

    # Local library imports 
    import BiblioParsing as bp
    from BiblioParsing.BiblioParsingUtils import remove_special_symbol
    
    # Globals imports
    from BiblioParsing.BiblioGeneralGlobals import APOSTROPHE_CHANGE
    from BiblioParsing.BiblioGeneralGlobals import DASHES_CHANGE
    from BiblioParsing.BiblioGeneralGlobals import REP_UTILS
    from BiblioParsing.BiblioGeneralGlobals import SYMB_CHANGE
    from BiblioParsing.BiblioGeneralGlobals import SYMB_DROP   
    from BiblioParsing.BiblioSpecificGlobals import COUNTRY_AFFILIATIONS_FILE 
    from BiblioParsing.BiblioSpecificGlobals import DIC_WORD_RE_PATTERN    
    from BiblioParsing.BiblioSpecificGlobals import MISSING_SPACE_ACRONYMS
    from BiblioParsing.BiblioSpecificGlobals import SMALL_WORDS_DROP 

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
            of the package`BiblioParsing`.

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
            std_raw_aff = re.sub(re_pattern, word_to_substitute + ' ', std_raw_aff)
        std_raw_aff = re.sub(r'\s+', ' ', std_raw_aff)
        std_raw_aff = re.sub(r'\s,', ',', std_raw_aff)
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

        # Managing missing space in raw affiliations related 
        # to particuliar institutions cases such as UMR or U followed by digits
        for accron in MISSING_SPACE_ACRONYMS:
            re_accron = re.compile(accronymes_template.substitute({"word":accron}))
            if re.search(re_accron,std_raw_aff.lower()) and len(raw_aff_words_set)==2:
                std_raw_aff_add = "".join(std_raw_aff.split(" "))

        # Droping small words
        for word_to_drop in SMALL_WORDS_DROP:
            re_drop_words = re.compile(small_words_template.substitute({"word":word_to_drop}))
            if re.search(re_drop_words,std_raw_aff.lower()):
                raw_aff_words_set = raw_aff_words_set - {word_to_drop}

        # Updating raw_aff_words_set_list using std_raw_aff_add
        raw_aff_words_set_add = {}
        if std_raw_aff_add:
            raw_aff_words_set_add = set(std_raw_aff_add.split(' '))

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
                # Building the set of words for raw affiliation
                raw_aff_words_set, raw_aff_words_set_add = _build_words_set(raw_aff)

                # Upadating the list of words sets with the set raw_aff_words_set 
                raw_aff_words_sets_list.append(raw_aff_words_set)

                # Upadating the list of words sets using the set raw_aff_words_set_add
                if raw_aff_words_set_add: raw_aff_words_sets_list.append(raw_aff_words_set_add)

        return raw_aff_words_sets_list

    ################################################# Local functions end #################################################  
    
    # Setting the path for the 'Country_affilialions.xlsx' file
    if not country_affiliations_file_path:
        country_affiliations_file_path = Path(bp.__file__).parent / Path(REP_UTILS) / Path(COUNTRY_AFFILIATIONS_FILE)
   
    # Reading the 'Country_affilialions.xlsx' file in the dataframe dic    
    wb = openpyxl.load_workbook(country_affiliations_file_path)
    country_aff_df = pd.read_excel(country_affiliations_file_path, 
                                   sheet_name = wb.sheetnames)

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
            norm_aff = norm_aff.strip()
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
    import BiblioParsing as bp
    
    # Globals imports
    from BiblioParsing.BiblioGeneralGlobals import REP_UTILS
    from BiblioParsing.BiblioSpecificGlobals import INST_TYPES_FILE 
    from BiblioParsing.BiblioSpecificGlobals import INST_TYPES_USECOLS
    
    # Setting the full path for the 'Inst_types.xlsx' file
    if not inst_types_file_path:
        inst_types_file = INST_TYPES_FILE
        inst_types_file_path = Path(bp.__file__).parent / Path(REP_UTILS) / Path(inst_types_file)
        
    if not inst_types_usecols:
        inst_types_usecols = INST_TYPES_USECOLS

    # Reading the 'Country_affilialions.xlsx' file in the dataframe dic
    inst_types_df = pd.read_excel(inst_types_file_path, usecols = INST_TYPES_USECOLS)

    levels = [x for x in inst_types_df['Level']]
    abbreviations = [x for x in inst_types_df['Abbreviation']]
    aff_type_dict = dict(zip(abbreviations, levels))

    return aff_type_dict


def read_towns_per_country(country_towns_file = None, country_towns_folder_path = None):
    
    '''The function `read_towns_per_country` builds a list of towns per country.
    It calls the functions `rationalize_town_names`and `remove_special_symbol`
    defined in the module 'BiblioParsingUtils' of the package 'BiblioParsing'.
    
    Args:
        country_towns_file (str): File name of the list of towns per country.
        country_towns_folder_path (path): Path to the folder of the file 'country_towns_file'.
        
    Returns:
        (dict): A dict keyyed by country and valued by the list of towns of the country.
        
    Notes:
        The data are extracted by default from the excel file 
        which name is given by the global 'COUNTRY_TOWNS_FILE' 
        in the BiblioSpecifGlobals module of the BiblioParsing package.
        It is located in the folder which name is given by the global 
        REP_UTILS in the BiblioGeneralGlobal module of the package BiblioParsing.
        
    '''
    # Standard library imports
    from pathlib import Path

    # 3rd party library imports
    import openpyxl
    import pandas as pd

    # Local imports
    import BiblioParsing as bp
    import BiblioParsing.BiblioGeneralGlobals as gg
    import BiblioParsing.BiblioSpecificGlobals as sg
    from BiblioParsing.BiblioParsingUtils import rationalize_town_names
    from BiblioParsing.BiblioParsingUtils import remove_special_symbol

    # Setting the path of the file of towns par country
    if not country_towns_folder_path:
        country_towns_folder_path = Path(bp.__file__).parent / Path(gg.REP_UTILS)
    if not country_towns_file:
        file_path = country_towns_folder_path / Path(sg.COUNTRY_TOWNS_FILE)
    else:
        file_path = country_towns_folder_path / Path(country_towns_file)

    # Reading the file of towns per country in a dict of dataframes 
    wb = openpyxl.load_workbook(file_path)
    df_dict = pd.read_excel(file_path, 
                            sheet_name = wb.sheetnames)
    
    towns_dict = {x[0]:x[1]['Town name'].tolist() for x in df_dict.items()}
    for country in towns_dict.keys():        
        list_towns = []
        for town in towns_dict[country]:
            town = town.lower()
            town = rationalize_town_names(town, dic_town_symbols = sg.DIC_TOWN_SYMBOLS,
                                          dic_town_words = sg.DIC_TOWN_WORDS)
            town = remove_special_symbol(town, only_ascii = False, strip = False)
            town = town.strip()
            list_towns.append(town)
        towns_dict[country]= list_towns    
    return towns_dict


def _check_norm_raw_aff_dict(norm_raw_aff_dict, aff_type_dict, user_country_affiliations_file_path):
    wrong_affil_types_dict = {}
    aff_types_set = set(aff_type_dict.keys())
    for country, country_dict in norm_raw_aff_dict.items():
        norm_affil_types_set = set([norm_affil.split(' ')[-1] for norm_affil in country_dict.keys()])
        norm_affil_types_in = aff_types_set.intersection(norm_affil_types_set)
        norm_affil_types_out = norm_affil_types_set - norm_affil_types_in
        if norm_affil_types_out:
            wrong_affil_types_dict[country] = list(norm_affil_types_out)
    return wrong_affil_types_dict


def build_norm_raw_institutions(df_address,
                                inst_types_file_path=None,
                                country_affiliations_file_path=None,
                                country_towns_file=None,
                                country_towns_folder_path=None,
                                verbose=False,
                                progress_param=None):    
    """The function `build_norm_raw_institutions_wos` parses the addresses 
    of each publication of the Wos corpus to retrieve the country, 
    the normalized institutions and the institutions not yet normalized for each address.

    Args:
        df_address (dataframe): the dataframe of the addresses resulting \
        from the concatenation/deduplication of databases.
        verbose (bool): If set to 'True' allows prints for code control (default: False).
        progress_param (tup): (Function for updating ProgressBar tkinter widget status, \
        The initial progress status (int), The final progress status (int)) \
        (optional, default = None)
    Returns:
        (tuple): (df_country, df_norm_institutions, df_raw_institutions, wrong_affil_types_dict).        
    Notes:
        The global 'COL_NAMES' is imported from `BiblioSpecificGlobals` module of `BiblioParsing` package.
        The functions `_build_address_affiliations_lists`, `build_norm_raw_affiliations_dict` and `read_inst_types`  
        are imported from `BiblioParsingInstitutions` module of `BiblioParsing` package.
        The function `build_item_df_from_tup` is imported from `BiblioParsingInstitutions` module 
        of `BiblioParsing` package.
    """

    # Standard library imports
    import re
    from collections import namedtuple
    
    # 3rd party library imports
    import pandas as pd
    
    # Local library imports
    from BiblioParsing.BiblioParsingUtils import build_item_df_from_tup
    from BiblioParsing.BiblioParsingInstitutions import build_norm_raw_affiliations_dict
    from BiblioParsing.BiblioParsingInstitutions import read_inst_types
    from BiblioParsing.BiblioParsingInstitutions import read_towns_per_country
    from BiblioParsing.BiblioParsingInstitutions import standardize_address
    
    # Globals imports    
    from BiblioParsing.BiblioSpecificGlobals import COL_NAMES
    from BiblioParsing.BiblioSpecificGlobals import EMPTY
    
    # Setting useful aliases
    pub_id_alias           = COL_NAMES['pub_id']
    address_col_List_alias = COL_NAMES['address']
    country_col_list_alias = COL_NAMES['country']
    inst_col_list_alias    = COL_NAMES['institution']
    address_alias          = address_col_List_alias[2]
    country_alias          = country_col_list_alias[2]
    institution_alias      = inst_col_list_alias[2]
    
    # Setting useful col names
    norm_institution_list = inst_col_list_alias
    raw_institution_list = inst_col_list_alias + [address_alias]
    
    # Setting named tuples
    country     = namedtuple('country', country_col_list_alias)
    norm_institution = namedtuple('norm_institution', norm_institution_list)
    raw_institution = namedtuple('raw_institution', raw_institution_list)
    
    # Getting useful dicts for affiliation normalization
    aff_type_dict = read_inst_types(inst_types_file_path=inst_types_file_path,
                                    inst_types_usecols=None)
    norm_raw_aff_dict = build_norm_raw_affiliations_dict(country_affiliations_file_path=country_affiliations_file_path)
    towns_dict = read_towns_per_country(country_towns_file = country_towns_file,
                                        country_towns_folder_path = country_towns_folder_path)
    wrong_affil_types_dict = _check_norm_raw_aff_dict(norm_raw_aff_dict, aff_type_dict,
                                                      country_affiliations_file_path)

    if not wrong_affil_types_dict:
        step_nb = len(df_address)
        step = 0
        if progress_param:
            progress_callback, init_progress, final_progress = progress_param
            progress_step = (final_progress-init_progress) / step_nb
            progress_status = init_progress
            progress_callback(progress_status)

        list_countries = []
        list_norm_institutions = []
        list_raw_institutions = []
        for pub_id, address_dg in df_address.groupby(pub_id_alias):
            if verbose:
                print("\n\nPub_id:", pub_id)
                print("\naddress_dg:\n", address_dg)     
            for idx, raw_address in enumerate(address_dg[address_alias].tolist()):
                std_address = standardize_address(raw_address)
                address_country = ""
                address_norm_affiliation_list = []
                address_raw_affiliation_list = []            
                try:
                    aff_list_tup = _build_address_affiliations_lists(std_address,
                                                                     norm_raw_aff_dict,
                                                                     aff_type_dict,
                                                                     towns_dict,
                                                                     drop_status=True,
                                                                     verbose=False)
                    address_country, address_norm_affiliation_list, address_raw_affiliation_list = aff_list_tup
                except KeyError:
                    print("\n\nError Pub_id / idx:", pub_id," / ", idx)
                    print("\naddress_dg:\n", address_dg[address_alias].tolist()[idx])
                    pass
                address_norm_affiliations = EMPTY
                address_raw_affiliations = EMPTY
                if address_norm_affiliation_list: 
                    address_norm_affiliations = "; ".join(address_norm_affiliation_list)
                if address_raw_affiliation_list: 
                    address_raw_affiliations = "; ".join(address_raw_affiliation_list)
                if address_country:
                    list_countries.append(country(pub_id, idx, address_country))
                list_norm_institutions.append(norm_institution(pub_id, idx, address_norm_affiliations))
                list_raw_institutions.append(raw_institution(pub_id, idx, address_raw_affiliations, std_address))
                step += 1

                if verbose:
                    print('\nIdx address:                       ', idx)
                    print('Country:                           ', address_country)
                    print('address_norm_affiliation_list:     ', address_norm_affiliations)
                    print('address_unknown_affiliations_list: ', address_raw_affiliations)
                    print(f"        Number of addresses analyzed: {step} / {step_nb}")
                else:
                    print(f"        Number of addresses analyzed: {step} / {step_nb}", end="\r")

                if progress_param:
                    progress_status += progress_step
                    progress_callback(progress_status)

        # Building a clean countries dataframe and accordingly updating the parsing success rate dict
        df_country, _ = build_item_df_from_tup(list_countries, country_col_list_alias,
                                               country_alias, pub_id_alias)

        # Building a clean institutions dataframe and accordingly updating the parsing success rate dict
        df_norm_institution, _ = build_item_df_from_tup(list_norm_institutions, norm_institution_list,
                                                        institution_alias, pub_id_alias)

        # Building a clean institutions dataframe and accordingly updating the parsing success rate dict
        df_raw_institution, _ = build_item_df_from_tup(list_raw_institutions, raw_institution_list,
                                                       institution_alias, pub_id_alias)
        if not(len(df_country)==len(df_norm_institution)==len(df_raw_institution)):
            warning = (f'WARNING: Lengths of "df_address", "df_country" and "df_institution" dataframes are not equal '
                       f'in "_build_addresses_countries_institutions_wos" function of "BiblioParsingWos.py" module')
            print(warning)
    else:
        # Returning empty dataframes
        df_country = pd.DataFrame()
        df_norm_institution= pd.DataFrame()
        df_raw_institution = pd.DataFrame()

    if progress_param:
        progress_callback, _, final_progress = progress_param
        progress_callback(final_progress)

    return df_country, df_norm_institution, df_raw_institution, wrong_affil_types_dict
