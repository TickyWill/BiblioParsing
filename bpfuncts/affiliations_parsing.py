"""Module of functions for parsing of authors' affiliations.
"""

__all__ = ['build_addr_affils_tup',
           'build_norm_and_raw_affils',
           'extend_author_affils',
           ]


# Standard library imports
import re
from collections import namedtuple

# 3rd party imports
import pandas as pd

# Local library imports
import bpfuncts.affiliations_globals as bp_ag
import bpfuncts.general_globals as bp_gg
import bpfuncts.parsing_cols_globals as bp_pcg
import bpfuncts.regex_globals as bp_rg
from bpfuncts.affil_norm_utils import build_affils_useful_dicts
from bpfuncts.general_utils import remove_special_symbol
from bpfuncts.parsing_utils import rationalize_town_names
from bpfuncts.parsing_utils import build_item_df_from_tup
from bpfuncts.parsing_utils import standardize_address


def _set_norm_affils_cols():
    """Builds 2 dict setting columns lists and selected columns names 
    for the process of parsing author affiliations and getting their 
    normalized affiliations.

    Returns:
        (tup): (A dict valued by column-names lists defined by the 'COL_NAMES' global, \
        A dict valued by column names of parsing results defined by the 'COL_NAMES' global).
    """
    cols_lists_dic = {'country_cols_list'   : bp_pcg.COL_NAMES['country'],
                      'affil_cols_list'     : bp_pcg.COL_NAMES['institution'],
                      'auth_affil_cols_list': bp_pcg.COL_NAMES['auth_inst'],
                     }

    cols_dic = {'pub_id_col'     : bp_pcg.COL_NAMES['pub_id'],
                'address_id_col' : bp_pcg.COL_NAMES['address'][1],
                'address_col'    : bp_pcg.COL_NAMES['address'][2],
                'country_col'    : bp_pcg.COL_NAMES['country'][2],
                'affil_col'      : bp_pcg.COL_NAMES['institution'][2],
                'norm_affils_col': bp_pcg.COL_NAMES['auth_inst'][4],
               }
    return cols_lists_dic, cols_dic


def  _search_dropping_bp(params_list, verbose=False):
    """Searches in the passed string for words beginning with 'bp' followed
    by digits using a not case-sensitive regex.

    Args:
        params_list (list): Composed of the string where the words are searched \
        after being converted to lower case, of the string (unused) \
        that contains the country and of the dict used to identify the towns \
        in the address (unused).
        verbose (bool): True for allowing control prints (default: False).
    Returns:
        (list): Composed of one boolean; True if a word beginning with 'bp' \
        followed by digits is found.
    """
    # Setting useful params values from params_list
    text = params_list[0]

    # Setting search regex of "bp" or "BP"
    re_droping_bp = re.compile(bp_rg.AFFIL_DROPPING_PATTERNS_DIC['bp'])
    flag = False
    result = re.search(re_droping_bp, text.lower())
    if result is not None:
        if verbose:
            print('Dropping word is postal-box abbreviation')
        flag = True
    return [flag]


def _set_droping_zipcode_pattern(country):
    """The regex for zip-codes search uses the global 'ZIP_CODES' dict for countries 
    from 'ZIP_CODES.keys()'.

    Specific regex are set for 'United Kingdom', 'Canada' and 'United States'.
    """
    # Setting patterns for regex definition for zip-codes search
    return_status = False
    pattern = ''
    if country=='United Kingdom':
        pattern = bp_rg.AFFIL_DROPPING_PATTERNS_DIC['united_kingdom_zip']

    elif country in ['United States','Canada']:
        pattern = bp_rg.AFFIL_DROPPING_PATTERNS_DIC['north_america_zip']

    elif country in bp_gg.ZIP_CODES.keys():
        letters_list = bp_gg.ZIP_CODES[country]['letters']
        digits_list = bp_gg.ZIP_CODES[country]['digits']
        if letters_list or digits_list:
            zip_template = bp_rg.AFFIL_DROPPING_PATTERNS_DIC['other_zip']
            letters_join = '|'.join(letters_list) if len(letters_list) else ''
            pattern_zip_list = [zip_template.substitute({"zip_letters": letters_join,
                                                         "zip_digits":digits})
                                for digits in digits_list]
            pattern = '|'.join(pattern_zip_list)
    else:
        print('country not found:', country)
        return_status = True
    return pattern, return_status


def _set_digits_keeping_prefix_regex():
    """Builts regex for prefixe search in addresses.
    """
    # Setting search regex of keeping-prefix
    prefix_template = bp_rg.AFFIL_KEEPING_PATTERNS_DIC['digits_prefix']
    pattern_prefix_list = [prefix_template.substitute({"prefix": prefix})
                           for prefix in bp_ag.KEEPING_PREFIX]
    re_digits_keeping_prefix = re.compile('|'.join(pattern_prefix_list))

    return re_digits_keeping_prefix


def _search_dropping_digits(params_list, verbose=False):
    """Searches in the passed string for words similar to zip codes except those 
    beginning with a prefix from the global 'KEEPING_PREFIX' followed by 3 or 4 digits
    using case-sensitive regexes.

    Args:
        params_list (list): Composed of the string where the words are searched \
        after being converted to lower case, of the string that contains the country \
        and of the dict used to identify the towns in the address (unused).
        verbose (bool): True for allowing control prints (default: False).
    Returns:
        (list): Composed of one boolean; True if a word is found different \
        from those beginning with a prefix from the global 'KEEPING_PREFIX' \
        followed by 3 or 4 digits .
    """
    # Setting useful params values from params_list
    text, country, _ = params_list

    # Setting regex of embedding-digits search
    re_dropping_digits = re.compile(bp_rg.AFFIL_DROPPING_PATTERNS_DIC['digits'])

    # Setting regex of prefix search
    re_digits_keeping_prefix = _set_digits_keeping_prefix_regex()

    # Setting regex for zip-codes search
    pattern, return_status = _set_droping_zipcode_pattern(country)

    flag = False
    if not return_status:
        dropping_zip_result = False
        if pattern:
            re_drop_zip = re.compile(pattern)
            if re.search(re_drop_zip, text.lower()):
                dropping_zip_result = True

        digits_keeping_prefix_result = False
        if re.search(re_digits_keeping_prefix, text.lower()):
            digits_keeping_prefix_result = True
            if verbose:
                print('Keeping prefix: True')

        dropping_digits_result = False
        if re.search(re_dropping_digits, text.lower()):
            dropping_digits_result = True

        if not digits_keeping_prefix_result and (dropping_zip_result
                                                 or dropping_digits_result):
            flag = True
            if verbose:
                txt_code = 'zip code' if dropping_zip_result else 'digits code'
                print(f'Dropping word is a {txt_code}')
    return [flag]


def _search_dropping_suffix(params_list, verbose=False):
    """Searches in the passed string for words ending by a suffix among 
    those given by the global 'DROPPING_SUFFIX' using a templated regex.

    Args:
        params_list (list): Composed of the string where the words are searched \
        after being converted to lower case, of the string (unused) that contains \
        the country and of the dict used to identify the towns in the address (unused).
        verbose (bool): True for allowing control prints (default: False).
    Returns:
        (list): Composed of one boolean; True if a suffix given by the 'DROPPING_SUFFIX' \
        global is found.
    """
    # Setting useful params values from params_list
    text = params_list[0]

    # Setting regex for dropping-suffix search
    suffix_template = bp_rg.AFFIL_DROPPING_PATTERNS_DIC['suffix']
    flag = False
    for word_to_drop in bp_ag.DROPPING_SUFFIX:
        re_drop_words = re.compile(suffix_template.substitute({"word":word_to_drop}))
        result = re.search(re_drop_words, text.lower())
        if result is not None:
            flag = True
            if verbose:
                print('Dropping word contains the suffix:', word_to_drop)
    return [flag]


def _search_dropping_town(params_list, verbose=False):
    """Searches in the passed string for words in lower case
    that are towns for each country as given in the passed dict 
    of towns per country.

    Args:
        params_list (list): Composed of the string where the words are searched \
        after being converted to lower case, of the string that contains the country \
        and of the dict used to identify the towns in the address.
        verbose (bool): True for allowing control prints (default: False).
    Returns:
        (list): Composed of one boolean; True if a word listed in the values \
        of the dict 'towns_dict' is equal to the passed string after spaces removal \
        at ends.
    """
    # Setting useful params values from params_list
    text, country, towns_dict = params_list

    flag = False
    text_mod = rationalize_town_names(text.lower())
    if country in towns_dict.keys():
        for word_to_drop in towns_dict[country]:
            if word_to_drop==text_mod.strip():
                if verbose:
                    print('Dropping word is a town of ', country)
                flag = True
    return [flag]


def _search_dropping_words(params_list, verbose=False):
    """Searches in the passed string for isolated words given by the 'FR_DROPPING_WORDS'
    and 'DROPPING_WORDS' globals using a templated regex.

    If country is 'France' only the 'FR_DROPPING_WORDS' global is used.

    Args:
        params_list (list): Composed of the string where the words are searched \
        after being converted to lower case, of the string that contains the country \
        and of the dict used to identify the towns in the address (unused).
        verbose (bool): True for allowing control prints (default: False).
    Returns:
        (list): Composed of one boolean; True if a word given by the 'DROPPING_WORDS' \
        or 'FR_DROPPING_WORDS' globals is found.
    """
    # Setting useful params values from params_list
    text, country, _ = params_list

    # Setting templated regex for dropping-words search
    dropping_words_template = bp_rg.AFFIL_DROPPING_PATTERNS_DIC['word']

    flag = False
    if country.lower()=='france':
        dropping_words_to_search = bp_ag.FR_DROPPING_WORDS
    else:
        dropping_words_to_search = bp_ag.FR_DROPPING_WORDS + bp_ag.DROPPING_WORDS

    for word_to_drop in dropping_words_to_search:
        re_drop_words = re.compile(dropping_words_template.substitute({"word":word_to_drop}))
        result = re.search(re_drop_words, text.lower())
        if result is not None:
            flag = True
            if verbose:
                print('Dropping word is the full word:', word_to_drop)
    return [flag]


def _search_keeping_prefix(params_list, verbose=False):
    """'Searches in the passed string for prefixes given by the global 'KEEPING_PREFIX' 
    using a templated regex if country is France.

    Args:
        params_list (list): Composed of the string where the words are searched \
        after being converted to lower case, of the string that contains the country \
        and of the dict used to identify the towns in the address (unused).
        verbose (bool): True for allowing control prints (default: False).
    Returns:
        (list): Composed of one boolean; True if a prefix given by the 'KEEPING_PREFIX' \
        global is found.
    """
    # Setting useful params values from params_list
    text, country, _ = params_list

    # Setting templated regex for keeping prefixes search
    keeping_prefix_template = bp_rg.AFFIL_KEEPING_PATTERNS_DIC['prefix']

    flag = False
    if country.lower()=='france':
        for prefix_to_keep in bp_ag.KEEPING_PREFIX:
            re_keep_prefix = re.compile(keeping_prefix_template.substitute({"prefix":prefix_to_keep}))
            result = re.search(re_keep_prefix, text.lower())
            if result is not None:
                if verbose:
                    print('Keeping word is the prefix:', prefix_to_keep)
                flag = True
    return [flag]


def _search_keeping_words(params_list, verbose=False):
    """Searches in the passed string for isolated words given by the 'KEEPING_WORDS' 
    global using a templated regex.

    Args:
        params_list (list): Composed of the string where the words are searched \
        after being converted to lower case, of the string (unused) that contains \
        the country and of the dict used to identify the towns in the address (unused).
        verbose (bool): True for allowing control prints (default: False).
    Returns:
        (list): Composed of 3 booleans all False if no word given by the 'KEEPING_WORDS' \
        global is found; the first is True if a word given by the 'GEN_KEEPING_WORDS' \
        global is found; the second is True if a word given by the 'BASIC_KEEPING_WORDS' \
        global is found; the third is True if a word given by the 'USER_KEEPING_WORDS' \
        global is found.
    """
    # Setting useful params values from params_list
    text = params_list[0]

    # Setting templated regex for keeping-words search
    keeping_word_template = bp_rg.AFFIL_KEEPING_PATTERNS_DIC['word']

    gen_flag, basic_flag, user_flag = False, False, False
    for word_to_keep in bp_ag.KEEPING_WORDS:
        re_keeping_word = re.compile(keeping_word_template.substitute({"word":word_to_keep}))
        result = re.search(re_keeping_word, text.lower())
        if result is not None:
            if verbose:
                print('Keeping word is the full word:', word_to_keep)
            if word_to_keep in bp_ag.GEN_KEEPING_WORDS:
                gen_flag = True
            if word_to_keep in bp_ag.BASIC_KEEPING_WORDS:
                basic_flag = True
            if word_to_keep in bp_ag.USER_KEEPING_WORDS:
                user_flag = True
    return [gen_flag, basic_flag, user_flag]


def _search_items(affiliation, country, towns_dict, verbose=False):
    """Searches for several item types in the passed chunk of address after accents removal
    and converting in lower case even if the search is case-sensitive.

    It uses the following internal functions:
        - The function `_search_dropping_bp` searches for words that are postal-box numbers such as 'BP54'.
        - The function `_search_dropping_digits` searches for words that contains digits such as zip codes \
        which templates are given per country by the 'ZIP_CODES' dict global.
        - The function `_search_dropping_suffix` searches for words ending by a suffix among \
        those given by the 'DROPPING_SUFFIX'  global such as 'strasse' in 'helmholtzstrasse'.
        - The function `_search_dropping_town` searches for words that are towns listed \
        in the 'towns_dict' dict.
        - The function `_search_dropping_words` searches for words given by the 'DROPPING_WORDS' global \
        such as 'Avenue'.
        - The function `_search_keeping_words` searches for isolated words given by the 'KEEPING_WORDS' \
        global using a templated regex.
        - The function `_search_keeping_prefix` searches for prefixes given by the 'KEEPING_PREFIX' \
        global using a templated regex.

    Args:
        affiliation (str): A chunk of a standardized address where dropping items are searched.
        country (str): The string that contains the country.
        towns_dict (dict): The data used to identify the towns in the address in order to drop them; \
        it is keyed by countries and valued by a list of towns of the country.
        verbose (bool): True for allowing control prints (default: False).
    Returns:
        (namedtuple): A namedtuple which values are booleans returned by the internal functions \
        that returns a list of booleans that are True if the corresponding searched item is found.
    """
    funct_list = [_search_dropping_bp, _search_dropping_digits, _search_dropping_suffix, _search_dropping_town,
                  _search_dropping_words, _search_keeping_prefix, _search_keeping_words]

    found_item_tup = namedtuple('found_item_tup', ['dropping_bp', 'dropping_digits', 'dropping_suffix',
                                                   'dropping_town', 'dropping_words', 'keeping_prefix',
                                                   'gen_keeping_words', 'basic_keeping_words',
                                                   'user_keeping_words'])

    affiliation_mod = remove_special_symbol(affiliation, only_ascii=False, strip=False)
    params_list = [affiliation_mod, country, towns_dict]
    flag_list = [funct(params_list, verbose) for funct in funct_list]

    # Flattening flag_list
    flag_list = sum(flag_list, [])
    found_item_flags = found_item_tup(*flag_list)
    return found_item_flags


def _check_dropping_digits_flag(digits_drop_params, found_item_flags,
                                keeping_words_flags, verbose):
    (affiliation, sub_check_affils_list, country,
     affils_list, affils_drop, add_affiliation_flag) = digits_drop_params
    break_status = False
    if country.lower() in ['france', 'algeria']:
        if not found_item_flags.keeping_prefix and not any(keeping_words_flags):
            affils_drop.append(('dropping_digits', sub_check_affils_list))
            if verbose:
                print('Break identification:', 'dropping_digits', '\n')
            break_status = True
        if not break_status:
            if found_item_flags.gen_keeping_words:
                if not add_affiliation_flag:
                    affils_list.append(affiliation)
                    add_affiliation_flag = True
                if verbose:
                    print('Break identification:', 'dropping_digits aborted by gen_keeping_words', '\n')
            else:
                if not add_affiliation_flag:
                    affils_list.append(affiliation)
                    add_affiliation_flag = True
                if verbose:
                    break_id = ''
                    if found_item_flags.basic_keeping_words:
                        break_id = 'dropping_digits aborted by basic_keeping_words'
                    if found_item_flags.user_keeping_words:
                        break_id = 'dropping_digits aborted by user_keeping_words'
                    if found_item_flags.keeping_prefix:
                        break_id = 'dropping_digits aborted by keeping_prefix'
                    print('Break identification:', break_id, '\n')
    else:
        if not found_item_flags.gen_keeping_words and not found_item_flags.user_keeping_words:
            affils_drop.append(('dropping_digits', sub_check_affils_list))
            if verbose:
                print('Break identification:', 'dropping_digits', '\n')
            break_status = True

        if found_item_flags.dropping_words and not break_status:
            affils_drop.append(('dropping_digits', sub_check_affils_list))
            if verbose:
                print('Break identification:', 'dropping_digits', '\n')
            break_status = True

        if not add_affiliation_flag and not break_status:
            affils_list.append(affiliation)
            add_affiliation_flag = True
            if verbose:
                print('Break identification:', 'dropping_digits aborted by user_keeping_words', '\n')

    return affils_list, affils_drop, add_affiliation_flag, break_status


def _check_dropping_words_flag(words_drop_params, found_item_flags,
                               keeping_words_flags, verbose):
    (affiliation, sub_check_affils_list, affils_list,
     affils_drop, add_affiliation_flag) = words_drop_params
    break_status = False
    # Keeping affiliation when a keeping word is found only if no dropping digit is found
    # this keeps "department bldg civil" which is wanted even if "bldg" is a dropping word
    # unfortunately, this keeps unwanted "campus university", "ciudad university"...
    if any(keeping_words_flags) and not found_item_flags.dropping_digits:
        if not add_affiliation_flag:
            affils_list.append(affiliation)
            add_affiliation_flag = True
        if verbose:
            break_id = ''
            if found_item_flags.user_keeping_words:
                break_id = 'dropping_word aborted by user_keeping_words'
            if found_item_flags.basic_keeping_words:
                break_id = 'dropping_word aborted by basic_keeping_words'
            if found_item_flags.gen_keeping_words:
                break_id = 'dropping_word aborted by gen_keeping_words'
            print('Break identification:', break_id, '\n')
    else:
        # Dropping affiliation from affiliations list
        # if already added because of a former drop abort
        if add_affiliation_flag:
            affils_list = affils_list[:-1]
            add_affiliation_flag = False
        affils_drop.append(('dropping_words', sub_check_affils_list))
        if verbose:
            print('Break identification:', 'dropping_words', '\n')
        break_status = True
    return affils_list, affils_drop, add_affiliation_flag, break_status


def _check_dropping_suffix_flag(suffix_drop_params, found_item_flags, verbose):
    (affiliation, sub_check_affils_list, affils_list,
     affils_drop, add_affiliation_flag) = suffix_drop_params
    break_status = False
    if found_item_flags.gen_keeping_words or found_item_flags.user_keeping_words:
        if not add_affiliation_flag:
            affils_list.append(affiliation)
            add_affiliation_flag = True
        if verbose:
            break_id = ''
            if found_item_flags.gen_keeping_words:
                break_id = 'dropping_suffix aborted by gen_keeping_words'
            if found_item_flags.user_keeping_words:
                break_id = 'dropping_suffix aborted by user_keeping_words'
            print('Break identification:', break_id, '\n')
    else:
        affils_drop.append(('dropping_suffix', sub_check_affils_list))
        if verbose:
            print('Break identification:', 'dropping_suffix', '\n')
        break_status = True
    return affils_list, affils_drop, add_affiliation_flag, break_status


def _check_dropping_town_flag(town_drop_params, verbose):
    affiliation, sub_check_affils_list, affils_drop = town_drop_params
    break_status = False
    if len(sub_check_affils_list)<=2:
        affils_drop.append(('dropping_town', sub_check_affils_list))
        if verbose:
            print('Break identification:', 'dropping_town', '\n')
        break_status = True
    else:
        affils_drop.append(('dropping_town', affiliation))
        if verbose:
            break_id = 'dropping_town aborted by index of town in affiliations list'
            print('Break identification:', break_id, '\n')
    return affils_drop, break_status


def _clean_affils(affils_drop_params, towns_dict, verbose=False):
    (affiliation, sub_check_affils_list, country, affils_list,
     affils_drop) = affils_drop_params
    found_item_flags = _search_items(affiliation, country, towns_dict, verbose=verbose)
    if verbose:
        print('found_item_flags:', found_item_flags)
    dropping_word_flags = [found_item_flags.dropping_bp, found_item_flags.dropping_digits,
                           found_item_flags.dropping_suffix, found_item_flags.dropping_town,
                           found_item_flags.dropping_words]

    keeping_words_flags = [found_item_flags.gen_keeping_words, found_item_flags.basic_keeping_words,
                           found_item_flags.user_keeping_words]

    break_status = False
    if not any(dropping_word_flags):
        affils_list.append(affiliation)
        if verbose:
            print('No dropping item found in:', affiliation, '\n')
    else:
        add_affiliation_flag = False
        if found_item_flags.dropping_bp:
            affils_drop.append(('dropping_bp', sub_check_affils_list))
            if verbose:
                print('Break identification:', 'dropping_bp', '\n')
            break_status = True

        if found_item_flags.dropping_digits and not break_status:
            digits_drop_params = [affiliation, sub_check_affils_list, country,
                                  affils_list, affils_drop, add_affiliation_flag]
            digits_tup = _check_dropping_digits_flag(digits_drop_params, found_item_flags,
                                                     keeping_words_flags, verbose)
            affils_list, affils_drop, add_affiliation_flag, break_status = digits_tup

        if found_item_flags.dropping_town and not break_status:
            town_drop_params = [affiliation, sub_check_affils_list, affils_drop]
            town_tup = _check_dropping_town_flag(town_drop_params, verbose)
            affils_drop, break_status = town_tup
        else:
            if found_item_flags.dropping_suffix and not break_status:
                suffix_drop_params = [affiliation, sub_check_affils_list, affils_list,
                                      affils_drop, add_affiliation_flag]
                suffix_tup = _check_dropping_suffix_flag(suffix_drop_params, found_item_flags,
                                                         verbose)
                affils_list, affils_drop, add_affiliation_flag, break_status = suffix_tup

            if found_item_flags.dropping_words and not break_status:
                words_drop_params = [affiliation, sub_check_affils_list, affils_list,
                                      affils_drop, add_affiliation_flag]
                words_tup = _check_dropping_words_flag(words_drop_params, found_item_flags,
                                                        keeping_words_flags, verbose)
                affils_list, affils_drop, add_affiliation_flag, break_status = words_tup
    return affils_list, affils_drop, break_status


def _get_affils_list(std_address, towns_dict, drop_status=True, verbose=False):
    """Extracts first, the country and then, the list of affiliations from the standardized
    address.

    It splits the address in list of chunks separated by coma or isolated hyphen-minus.
    The country is present as the last chunk of the splitting.
    The other chunks are kept as affiliations if they contain at least one word among 
    those listed in the global 'KEEPING_WORDS' or if they do not contain any item 
    searched by the function `search_dropping_items`.
    The first chunk is always kept in the final affiliations list.
    The spaces at the ends of the items of the final affiliations list are removed.

    Args:
        std_address (str): The full address to be parsed in list of affiliations and country.
        towns_dict (dict): The data used to identify the towns in the address in order to drop them; \
        it is keyed by countries and valued by a list of towns of the country.
        drop_status (bool): If True (default: True), dropping items are searched to drop chunks \
        from the address.
        verbose (bool): True for allowing control prints (default: False).
    Returns:
        (tuple): A tuple composed of 3 items (list of kept chunks, country and list of dropped chunks).
    """
    # Splitting by coma the standard address in chunks listed in an initial-affiliations list
    init_raw_affils_list = std_address.split(',')

    # Removing the first occurrence of chunk duplicates from the initial-affiliations list
    # and putting them in a deduplicated-affiliations list
    drop_aff_idx_list = []
    for idx1, aff1 in enumerate(init_raw_affils_list):
        drop_aff_idx_list.extend([min(idx1, idx2) for idx2, aff2 in enumerate(init_raw_affils_list)
                                  if idx1!=idx2 and aff1==aff2])
    dedup_raw_affils_list = []
    dedup_raw_affils_list.extend([aff for idx, aff in enumerate(init_raw_affils_list)
                                        if idx not in set(drop_aff_idx_list)])

    # Setting country index in raw-affiliations list
    country_pos = -1
    country = dedup_raw_affils_list[country_pos].strip()

    # Splitting by special characters the deduplicated chunks and putting them in a raw-affiliations list
    raw_affils_list = sum([x.split(' - ') for x in dedup_raw_affils_list], [])
    raw_affils_list = sum([x.split(' | ') for x in raw_affils_list], [])

    if drop_status:
        # Initializing the affiliations list by keeping systematically the first chunk of the full address
        affils_list = [raw_affils_list[0]]

        # Check affiliations only if length > 3 to avoid keeping affiliations of less than 3 characters
        check_affils_list = [aff for aff in raw_affils_list[1:] if len(aff)>3]

        if verbose:
            print('Full standard address:',std_address)
            print('init_raw_affils_list:',init_raw_affils_list)
            print('dedup_raw_affils_list:',dedup_raw_affils_list)
            print('country:', country)
            print('raw_affils_list flattened:',raw_affils_list)
            print('First affiliation:',dedup_raw_affils_list[0])
            print('check_affils_list:',check_affils_list, "\n")

        # Initializing the list of chunks to drop from the raw-affiliations list
        affils_drop = []

        # Searching for chunks to keep and chunks to drop in the raw-affiliations list,
        # the first chunk and the country excepted
        if check_affils_list:
            if verbose:
                print('Search results\n')
            for affil_idx, affiliation in enumerate(check_affils_list[:country_pos]):
                affiliation = affiliation.translate(bp_gg.SYMB_CHANGE)
                if verbose:
                    print('\naffil_idx:', affil_idx, '  affiliation:', affiliation)
                sub_check_affils_list = check_affils_list[affil_idx:country_pos]
                affils_drop_params = [affiliation, sub_check_affils_list, country,
                                      affils_list, affils_drop]
                affil_tup = _clean_affils(affils_drop_params, towns_dict,
                                          verbose=verbose)
                affils_list, affils_drop, break_status = affil_tup
                if break_status:
                    break
    else:
        affils_list = raw_affils_list
        affils_drop = []

    # Removing spaces from the kept affiliations
    affils_list = [x.strip() for x in affils_list]
    if verbose:
        print('affils_list stripped:', affils_list, "\n")

    # Removing country and country alias from the kept affiliations
    uk_aliases = bp_gg.COUNTRY_ALIASES["United Kingdom"]
    affils_list = [x for x in affils_list if x!=country and x not in uk_aliases]
    if verbose:
        print('affils_list without country aliases:', affils_list, "\n")

    return country, affils_list, affils_drop


def _build_norm_affiliation_list(affiliation, country, norm_raw_aff_dict, verbose):
    norm_affiliation_list = []

    # Removing accents and converting to lower case
    aff_mod = remove_special_symbol(affiliation, only_ascii=False, strip=True)
    aff_mod = aff_mod.lower()
    if verbose:
        print('\naff_mod:', aff_mod, "\n")

    # Searching for words set in affiliation
    for num, norm_aff in enumerate(norm_raw_aff_dict[country].keys()):

        if verbose:
            print("\n", str(num) + ' norm_aff:', norm_aff, "\n")

        for words_set in norm_raw_aff_dict[country][norm_aff]:
            if verbose:
                print('  words_set:', words_set)
            words_set_tags = []
            for word in words_set:
                re_search_words = re.compile(bp_rg.AFFIL_WORDS_SET_TEMPLATE.substitute({"word":word}))
                if re.search(re_search_words, aff_mod):
                    words_set_tags.append('true')
                else:
                    words_set_tags.append('false')
                if verbose:
                    print('    word:', word, '\n    words_set_tags:', words_set_tags)

            if 'false' not in words_set_tags:
                norm_affiliation_list.append(norm_aff)

            if verbose:
                print('  final words_set_tags:',words_set_tags)
                print('  norm_affiliation_list:', norm_affiliation_list, "\n")

    if verbose:
        print('  norm_affiliation_list:', norm_affiliation_list)
    return norm_affiliation_list


def _check_paris_univ(address_norm_affiliations_set, verbose):
    paris_nb = 0
    for norm_aff in address_norm_affiliations_set:
        if 'Univ' in norm_aff and 'Paris' in norm_aff:
            paris_nb += 1

    if paris_nb>1 and 'Paris-Cité Univ' in address_norm_affiliations_set:
        address_norm_affiliations_set = address_norm_affiliations_set - {'Paris-Cité Univ'}
    if verbose:
        print('address_norm_affiliations_set:     ',address_norm_affiliations_set)
    return address_norm_affiliations_set


def _reorder_address_norm_affiliations(address_norm_affiliations_set, aff_type_dict, verbose):
    idx_dict = dict(zip(aff_type_dict.keys(), [0 ]* len(aff_type_dict.keys())))
    norm_aff_pos_list = []
    address_norm_affiliation_dict = {}
    for norm_aff in address_norm_affiliations_set:
        norm_aff_type = norm_aff.split(' ')[-1]
        if verbose:
            print(f'norm_aff_type: {norm_aff_type}')
            print(f'str(idx_dict[norm_aff_type]): {str(idx_dict[norm_aff_type])}')

        norm_aff_pos = str(aff_type_dict[norm_aff_type]) + str(idx_dict[norm_aff_type])
        if verbose:
            print(f'norm_aff_pos init: {norm_aff_pos}\nnorm_aff_pos_list init: {norm_aff_pos_list}')
        if int(norm_aff_pos) in norm_aff_pos_list:
            idx_dict[norm_aff_type] += 1

        norm_aff_pos = str(aff_type_dict[norm_aff_type]) + str(idx_dict[norm_aff_type])
        if verbose:
            print(f'norm_aff_pos end: {norm_aff_pos}\nidx_dict[norm_aff_type]: {idx_dict[norm_aff_type]}')

        norm_aff_pos_list.append(int(norm_aff_pos))
        if verbose:
            print(f'norm_aff_pos_list end: {norm_aff_pos_list}\n')

        address_norm_affiliation_dict[norm_aff_pos] = norm_aff

    if verbose:
        print('address_norm_affiliation_dict:     ', address_norm_affiliation_dict)

    norm_aff_pos_list.sort()
    address_norm_affiliation_list = [None] * len(address_norm_affiliations_set)
    for idx, norm_aff_pos in enumerate(norm_aff_pos_list):
        address_norm_affiliation_list[idx] = address_norm_affiliation_dict[str(norm_aff_pos)]
    return address_norm_affiliation_list


def _get_norm_affils_list(country, affiliations_list, norm_raw_aff_dict,
                          aff_type_dict, verbose=False):
    """ToDo: docstring fill.
    """
    address_norm_affiliations_list = []
    address_unknown_affiliations_list = []
    for affiliation in affiliations_list:
        if verbose:
            print(' -', affiliation)

        norm_affiliation_list = _build_norm_affiliation_list(affiliation, country,
                                                             norm_raw_aff_dict, verbose)

        if not norm_affiliation_list:
            address_unknown_affiliations_list.append(affiliation)

        address_norm_affiliations_list = address_norm_affiliations_list + norm_affiliation_list

    address_norm_affiliations_set = set(address_norm_affiliations_list)
    if verbose:
        print('address_norm_affiliations_list:', address_norm_affiliations_list)
        print('address_norm_affiliations_set:     ', address_norm_affiliations_set)

    address_norm_affiliations_set = _check_paris_univ(address_norm_affiliations_set, verbose)

    address_norm_affiliation_list = _reorder_address_norm_affiliations(address_norm_affiliations_set,
                                                                       aff_type_dict, verbose)
    return address_norm_affiliation_list, address_unknown_affiliations_list


def _build_addr_affils_lists(std_address, affil_dicts, drop_status, verbose=False):
    """Builds the list of normalized affiliations for a standardized address.

    It also returns the country and the unknown affiliations for this address. 
    To do that, it uses the `_get_affils_list` and `_get_norm_affils_list` 
    internal functions. 
    The 'affil_dicts' data for affiliations normalization are built through 
    the `build_affils_useful_dicts` function imported from the `affil_norm_utils` 
    module.

    Args:
        std_address (str): The standardized address for which the list of normalized affiliations is built.
        affil_dicts (dict): The data (dict) for affiliations normalization.
        drop_status (bool): If true, dropping items are searched to drop chunks from the address.
        verbose (bool): True for allowing control prints (default: False).
    Returns:
        (tuple): A tuple of 3 items; first item is the country as string; \
        second item is the list of normalized affiliations; \
        third item is the list of unknown affiliations.
    """
    if verbose:
        print('\nStandardized address:              ', std_address)

    # Building the useful data for affiliations normalization
    affils_dicts_keys = ['affil_types_dict', 'norm_raw_affils_dict', 'towns_dict']
    affil_types_dict, norm_raw_affils_dict, towns_dict = [affil_dicts[key] for key in affils_dicts_keys]
    affil_countries = list(norm_raw_affils_dict.keys())

    return_tup = _get_affils_list(std_address, towns_dict, drop_status=drop_status)
    country, affils_list, affils_drop = return_tup
    affils_list_mod = [affiliation.translate(bp_gg.SYMB_CHANGE) for affiliation in affils_list]

    if verbose:
        print('\nCountry:                           ', country)
        print('\nAffiliations list:                 ', affils_list)
        print('Modified affiliations list:        ', affils_list_mod)
        print('Affiliations dropped:              ', affils_drop)

    addr_norm_affils_list = []
    addr_unknown_affils_list = affils_list
    if country in affil_countries:
        return_tup = _get_norm_affils_list(country, affils_list_mod, norm_raw_affils_dict,
                                           affil_types_dict, verbose=False)
        addr_norm_affils_list, addr_unknown_affils_list = return_tup
    return country, addr_norm_affils_list, addr_unknown_affils_list


def build_addr_affils_tup(full_address, affil_params_dic, drop_status):
    """Builds the affiliations list of a full address using the `_build_addr_affils_lists` 
    internal function of the same module.

    Args:
        full_address (str): the full address to be parsed in affiliations and country.
        affil_params_dic (dict): Keyed by ['affil_types_file_path', 'country_affils_file_path', \
        'country_towns_folder_path', 'country_towns_file'] and valued by the user as the full path to the data \
        per country of raw affiliations per normalized one, the full path to the data of affiliations-types \
        used to normalize the affiliations, the name of the file of the data of towns per country and the full \
        path to the folder where these are available.
        drop_status (bool): If true, dropping items are searched to drop chunks from the address.
    Returns:
        (namedtuple): A tuple of two strings; the first is the joined list of normalized affiliations \
        names found in the full address; the second is the joined list of raw affiliations names \
        of the full address with no fully corresponding normalized names.
    """
    affils_full_list_ntup = namedtuple('affils_full_list_ntup', ['norm_affils_list','raw_affils_list'])
    norm_affils_full_list_str = ""
    raw_affils_full_list_str = ""

    # Getting useful data for affiliations normalization
    affil_dicts = build_affils_useful_dicts(affil_params_dic)
    wrong_affil_types_dict = affil_dicts['wrong_affil_types_dict']

    if not wrong_affil_types_dict:
        aff_list_tup = _build_addr_affils_lists(full_address, affil_dicts, drop_status, verbose=False)
        _, norm_affils_full_list, raw_affils_full_list = aff_list_tup

        raw_affils_full_list_str = bp_ag.EMPTY
        if raw_affils_full_list:
            raw_affils_full_list_str = ";".join(raw_affils_full_list)

        # Building a string from the final list of normalized affiliations without duplicates
        norm_affils_full_list = list(set(norm_affils_full_list))
        norm_affils_full_list_str = bp_ag.EMPTY
        if norm_affils_full_list:
            norm_affils_full_list_str = ";".join(norm_affils_full_list)

    # Setting the namedtuple to return
    affils_full_list_tup =  affils_full_list_ntup(norm_affils_full_list_str, raw_affils_full_list_str)
    return affils_full_list_tup


def _build_complements_list(affil_names_list, affiliations):
    complements_list = []
    for affil in affil_names_list:
        if affil in affiliations:
            complements_list.append(1)
        else:
            complements_list.append(0)
    return complements_list


def extend_author_affils(item_df, affil_filter_list):
    """Extends the data of authors affiliations initially obtained by the parsing
    of the corpus, with complementary information about an affiliation selected by the user.

    The selection is given by the user through a list of 2-items tuples composed 
    of a normalized affiliation and the corresponding column name. For each normalized
    affiliation, the corresponding column is filled with 1 for each of the author 
    affiliated to this affiliation. Otherwise, it is filled with 0.

    Args:
        item_df (dataframe): The data of authors with affiliation.
        affil_filter_list (list): The list of tuples selected by the user.
    Returns:
        (dataframe): The extended data with the columns given by the user.
    """
    # Setting useful column names
    cols_lists_dic, cols_dic = _set_norm_affils_cols()
    read_usecols = cols_lists_dic['auth_affil_cols_list'][0:5]
    norm_affils_col = cols_dic['norm_affils_col']
    temp_col = "complements_col"

    # Getting the useful columns of the item df
    item_df = item_df[read_usecols]

    # Setting an affiliation name for each of the affiliations indicated in the affiliations filter
    affil_names_list = [f'{x[0]}' for x in affil_filter_list]
    affil_col_list = [f'{x[1]}' for x in affil_filter_list]

    # Building a list of 0 or 1 in 'temp_col' column added to the initial data using "affil_filter_list"
    item_dg = item_df.copy()
    item_dg[temp_col] = item_dg.apply(lambda row: _build_complements_list(affil_names_list,
                                                                          row[norm_affils_col]),
                                      axis=1)
    item_dg.reset_index(inplace=True, drop=True)

    # Distributing the value lists of 'temp_col' column in a dataframe
    # into columns which names are given by 'affil_col_list' list
    complements_split_df = pd.DataFrame(item_dg[temp_col].sort_index().to_list(), columns=affil_col_list)

    # Extending the initial data with the previously built data from 'temp_col' column
    new_item_df = pd.concat([item_dg, complements_split_df], axis=1)

    # Dropping the temp_col column which is no more useful
    new_item_df.drop([temp_col], axis=1, inplace=True)
    return new_item_df


def build_norm_and_raw_affils(addresses_df, affil_params_dic=None, verbose=False, progress_param=None):
    """Parses the addresses of each publication of the corpus to retrieve the country, 
    the normalized affiliations and the affiliations not yet normalized for each address.

    Args:
        addresses_df (dataframe): the data of the addresses resulting from the parsing of \
        the corpus after concatenation and deduplication of partial parsings.
        affil_params_dic (dict): Optional dict (default=None) keyed by ['affil_types_file_path', \
        'country_affils_file_path', 'country_towns_folder_path', 'country_towns_file'] and valued \
        by the user as the full path to the data per country of raw affiliations per normalized one, \
        the full path to the data of affiliations-types used to normalize the affiliations, \
        the name of the file of the data of towns per country and the full path to the folder \
        where these data are available.
        verbose (bool): If set to 'True' allows prints for code control (default: False).
        progress_param (tup): (Function for updating ProgressBar tkinter widget status, \
        The initial progress status (int), The final progress status (int)) \
        (optional, default=None)
    Returns:
        (tuple): (countries data per address (dataframe), normalized affiliations per address (dataframe), \
        raw affiliations per address (dataframe), A dict of wrong type of normalized affiliations \
        for correction by the user).
    """
    # Setting useful column names
    cols_lists_dic, cols_dic = _set_norm_affils_cols()
    cols_lists_keys = ['country_cols_list', 'affil_cols_list']
    country_cols_list, affil_cols_list = [cols_lists_dic[key] for key in cols_lists_keys]
    cols_keys = ['pub_id_col', 'address_id_col', 'address_col', 'country_col', 'affil_col']
    pub_id_col, address_id_col, address_col, country_col, affil_col = [cols_dic[key] for key in cols_keys]

    # Setting useful cols lists
    norm_affil_cols_list = affil_cols_list
    raw_affil_cols_list = affil_cols_list + [address_col]

    # Setting named tuples
    country = namedtuple('country', country_cols_list)
    norm_affiliation = namedtuple('norm_affiliation', norm_affil_cols_list)
    raw_affiliation = namedtuple('raw_affiliation', raw_affil_cols_list)

    # Getting useful data for affiliations normalization
    affil_dicts = build_affils_useful_dicts(affil_params_dic)
    wrong_affil_types_dict = affil_dicts['wrong_affil_types_dict']

    if not wrong_affil_types_dict:
        step_nb = len(addresses_df)
        step = 0
        if progress_param:
            progress_callback, init_progress, final_progress = progress_param
            progress_step = (final_progress-init_progress) / step_nb
            progress_status = init_progress
            progress_callback(progress_status)

        countries_list = []
        norm_affiliations_list = []
        raw_affiliations_list = []
        for pub_id, pub_id_addresses_dg in addresses_df.groupby(pub_id_col):
            if verbose:
                print("\n\nPub_id:", pub_id, "\npub_id_addresses_dg:\n", pub_id_addresses_dg)
            for idx, row in pub_id_addresses_dg.iterrows():
                address_idx = row[address_id_col]
                raw_address = row[address_col]
                std_address = standardize_address(raw_address)
                address_country = ""
                addr_norm_affils_list = []
                addr_raw_affils_list = []
                try:
                    affil_list_tup = _build_addr_affils_lists(std_address, affil_dicts,
                                                              drop_status=True, verbose=False)
                    address_country, addr_norm_affils_list, addr_raw_affils_list = affil_list_tup
                except KeyError:
                    print("\n\nError Pub_id / idx:", pub_id," / ", idx)
                    print("\npub_id_addresses_dg:\n", pub_id_addresses_dg[address_col].tolist()[idx])
                addr_norm_affils = bp_ag.EMPTY
                addr_raw_affils = bp_ag.EMPTY
                if addr_norm_affils_list:
                    addr_norm_affils = "; ".join(addr_norm_affils_list)
                if addr_raw_affils_list:
                    addr_raw_affils = "; ".join(addr_raw_affils_list)
                if address_country:
                    countries_list.append(country(pub_id, address_idx, address_country))
                norm_affiliations_list.append(norm_affiliation(pub_id, address_idx, addr_norm_affils))
                raw_affiliations_list.append(raw_affiliation(pub_id, address_idx, addr_raw_affils, std_address))
                step += 1

                if verbose:
                    print('\nAddress idx:                       ', address_idx)
                    print('Country:                           ', address_country)
                    print('address norm-affiliation list:     ', addr_norm_affils)
                    print('address unknown-affiliations list: ', addr_raw_affils)
                    print(f"        Number of addresses analyzed: {step} / {step_nb}")
                else:
                    print(f"        Number of addresses analyzed: {step} / {step_nb}", end="\r")

                if progress_param:
                    progress_status += progress_step
                    progress_callback(progress_status)

        # Building clean data of countries
        country_df, _ = build_item_df_from_tup(countries_list, country_cols_list, country_col, pub_id_col)

        # Building clean data of noramized affiliations
        norm_affiliation_df, _ = build_item_df_from_tup(norm_affiliations_list, norm_affil_cols_list,
                                                        affil_col, pub_id_col)

        # Building clean data of raw affiliations
        raw_affiliation_df, _ = build_item_df_from_tup(raw_affiliations_list, raw_affil_cols_list,
                                                       affil_col, pub_id_col)
    else:
        # Returning empty dataframes
        country_df, norm_affiliation_df, raw_affiliation_df = [pd.DataFrame()] * 3

    if progress_param:
        progress_callback, _, final_progress = progress_param
        progress_callback(final_progress)

    return country_df, norm_affiliation_df, raw_affiliation_df, wrong_affil_types_dict
