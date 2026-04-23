__all__ = ['build_affils_useful_dicts',
           'build_norm_raw_affils_dict',
           'read_affil_types',
           'read_towns_per_country',
           ]


# Standard library imports
import re
from pathlib import Path
from string import Template

# 3rd party imports
import openpyxl
import pandas as pd

# Local library imports
import BiblioParsing as bp
import BiblioParsing.general_globals as bp_gg
import BiblioParsing.regex_globals as bp_rg
import BiblioParsing.specific_globals as bp_sg
from BiblioParsing.parsing_utils import remove_special_symbol
from BiblioParsing.parsing_utils import rationalize_town_names
from BiblioParsing.parsing_utils import set_address_uniform_words


def _build_words_set(raw_aff, verbose=False):
    """Builds sets of words from a raw affiliation after standardization of words and symbols, 
    removing special symbols, adding missing spaces and droping small words.

    Args:
        raw_aff (str): The raw affiliation used to build the sets of words.
        verbose (bool): If true, variables are printed for code control (default: False).
    Returns:
        (tuple): Tuple of to sets of words; The first set is the canonical set of words \
        issuing from the string 'raw_aff'; The second set is an added set if some specific \
        accronyms are present in the first set of words.
    """
    # Setting substitution templates for searching small words or acronyms
    small_words_template = Template(r'[\s(]$word[\s)]' # For instance capturing 'of' in 'technical university of denmark'
                                    + '|'
                                    + r'[\s]$word$$' # For instance capturing 'd' in 'institut d ingenierie'
                                    + '|'
                                    + r'^$word\b') # For instance capturing 'the' in 'the denmark university'

    acronyms_template = Template(r'[\s(]$word[\s)]' # For instance capturing 'umr' in 'umr dddd' or 'umr dd'
                                  + '|'
                                  + r'[\s]$word$$'
                                  + '|'
                                  + r'^$word\b')

    # Removing accents and spaces at ends
    raw_aff_mod = remove_special_symbol(raw_aff, only_ascii=False, strip=True)

    # Uniformizing words
    std_raw_aff = set_address_uniform_words(raw_aff_mod)
    std_raw_aff = std_raw_aff.lower()

    # Uniformizing dashes
    std_raw_aff = std_raw_aff.translate(bp_gg.DASHES_CHANGE)

    # Uniformizing apostrophes
    std_raw_aff = std_raw_aff.translate(bp_gg.APOSTROPHE_CHANGE)

    # Uniformizing symbols
    std_raw_aff = std_raw_aff.translate(bp_gg.SYMB_CHANGE)

    # Droping particular symbols
    std_raw_aff = std_raw_aff.translate(bp_gg.SYMB_DROP)
    if verbose:
        print('       std_raw_aff:', std_raw_aff)

    # Building the corresponding set of words to std_raw_aff
    raw_aff_words_set = set(std_raw_aff.strip().split(' '))

    # Managing missing space in raw affiliations related
    # to particuliar institutions cases such as UMR or U followed by digits
    std_raw_aff_add = ""
    for accron in bp_sg.MISSING_SPACE_ACRONYMS:
        re_accron = re.compile(acronyms_template.substitute({"word":accron}))
        if re.search(re_accron,std_raw_aff.lower()) and len(raw_aff_words_set)==2:
            std_raw_aff_add = "".join(std_raw_aff.split(" "))

    # Droping small words
    for word_to_drop in bp_sg.SMALL_WORDS_DROP:
        re_drop_words = re.compile(small_words_template.substitute({"word":word_to_drop}))
        if re.search(re_drop_words,std_raw_aff.lower()):
            raw_aff_words_set = raw_aff_words_set - {word_to_drop}

    # Updating raw_aff_words_set_list using std_raw_aff_add
    raw_aff_words_set_add = {}
    if std_raw_aff_add:
        raw_aff_words_set_add = set(std_raw_aff_add.split(' '))
    return raw_aff_words_set, raw_aff_words_set_add


def _build_words_sets_list(raw_aff_list, verbose=False):
    """Builds a list of words sets from a list of raw affiliations.

    Args:
        raw_aff_list (list): The list of raw affiliations as strings.
        verbose (bool): If true, variables are printed for code control (default: False).
    Returns:
        (list): List of words sets.
    """
    raw_aff_words_sets_list = []
    for raw_aff in raw_aff_list:
        if raw_aff and raw_aff!=' ':
            # Building the set of words for raw affiliation
            raw_aff_words_set, raw_aff_words_set_add = _build_words_set(raw_aff, verbose)

            # Updating the list of words sets with the set raw_aff_words_set
            raw_aff_words_sets_list.append(raw_aff_words_set)

            # Updating the list of words sets using the set raw_aff_words_set_add
            if raw_aff_words_set_add:
                raw_aff_words_sets_list.append(raw_aff_words_set_add)

    return raw_aff_words_sets_list


def build_norm_raw_affils_dict(country_affiliations_file_path=None, verbose=False):
    """Builds a dict keyed by country and the value per country is a dict keyed 
    by normalized affiliation and valued by a list of sets of words representing 
    the raw affiliations corresponding to the normalized affiliation.

    Args:
        country_affiliations_file_path (path): Full path to the file of normalized affiliations \
        with they possible corresponding raw affiliation built by the user"; if None, it is set \
        using the 'COUNTRY_AFFILIATIONS_FILE' and 'REP_UTILS' globals.
        verbose (bool): If true, variables are printed for code control (default: False).
    Returns:
        (dict): The built dict.
    """
    # Setting useful column names
    norm_affil_col = bp_sg.AFFIL_COL_NAMES['norm_affil_col']

    # Setting the path for the 'Country_affiliations.xlsx' file
    if not country_affiliations_file_path:
        country_affiliations_file_path = Path(bp.__file__).parent / Path(bp_gg.REP_UTILS) / Path(bp_sg.COUNTRY_AFFILIATIONS_FILE)

    # Reading the 'Country_affiliations.xlsx' multisheet XLSX file in a dict
    wb = openpyxl.load_workbook(country_affiliations_file_path)
    country_aff_dict = pd.read_excel(country_affiliations_file_path, sheet_name=wb.sheetnames)

    norm_raw_aff_dict = {}
    for country_aff_item in country_aff_dict.items():
        country = country_aff_item[0]
        norm_raw_aff_df = country_aff_item[1]
        norm_raw_aff_nb = len(norm_raw_aff_df[norm_affil_col])

        if verbose:
            print('Country:', country)
            print('Number of normalized affiliations:', norm_raw_aff_nb)
            print('\nList of normalized affiliations:', norm_raw_aff_df[norm_affil_col], "\n")

        norm_raw_aff_dict[country] = {}
        for num, norm_aff in enumerate(norm_raw_aff_df[norm_affil_col]):
            norm_aff = norm_aff.strip()
            raw_aff_list = [item for item in list(norm_raw_aff_df.loc[num])[1:] if not(pd.isnull(item)) is True]

            if verbose:
                print(f"\n\n{str(num)}- Normalized affiliation: {norm_aff}")
                print('   Raw affiliations list:', raw_aff_list, "\n")

            norm_raw_aff_dict[country][norm_aff] = _build_words_sets_list(raw_aff_list, verbose)

            if verbose:
                print(f"   norm_raw_aff_dict[{country}][{norm_aff}]: {norm_raw_aff_dict[country][norm_aff]}\n")

    return norm_raw_aff_dict


def read_affil_types(affil_types_file_path=None):
    """Builds a dict keyed by normalized affiliations types and the value per type 
    is the order level of the type.

    Args:
        affil_types_file_path (path): The full path to the file of ordered affiliations types; \
        if None, it is set using the 'INST_TYPES_FILE' and 'REP_UTILS' globals.
    Returns:
        (dict): The built dict.
    """
    # Setting useful column names
    level_col, short_col = bp_sg.INST_TYPES_USECOLS

    # Setting the full path for the file of ordered institutions types
    if not affil_types_file_path:
        inst_types_file = bp_sg.INST_TYPES_FILE
        affil_types_file_path = Path(bp.__file__).parent / Path(bp_gg.REP_UTILS) / Path(inst_types_file)

    # Reading the file in a dataframe
    inst_types_df = pd.read_excel(affil_types_file_path, usecols=bp_sg.INST_TYPES_USECOLS)

    levels = list(inst_types_df[level_col])
    abbreviations = list(inst_types_df[short_col])
    aff_type_dict = dict(zip(abbreviations, levels))

    return aff_type_dict


def read_towns_per_country(country_towns_file=None, country_towns_folder_path=None):
    """Builds dict keyed by countries and valued by a list of towns of the country.

    It uses the functions `rationalize_town_names`and `remove_special_symbol`
    imported from the `BiblioParsing.BiblioParsingUtils` module.

    Args:
        country_towns_file (str): File name of the list of towns per country.
        country_towns_folder_path (path): The full path to the folder \
        of the 'country_towns_file' file.
    Returns:
        (dict): The built dict.
    """
    # Setting the path of the file of towns par country
    if not country_towns_folder_path:
        country_towns_folder_path = Path(bp.__file__).parent / Path(bp_gg.REP_UTILS)
    if not country_towns_file:
        file_path = country_towns_folder_path / Path(bp_sg.COUNTRY_TOWNS_FILE)
    else:
        file_path = country_towns_folder_path / Path(country_towns_file)

    # Reading the file of towns per country in a dict of dataframes
    wb = openpyxl.load_workbook(file_path)
    dfs_dict = pd.read_excel(file_path, sheet_name=wb.sheetnames)

    towns_dict = {x[0]:x[1]['Town name'].tolist() for x in dfs_dict.items()}
    for country in towns_dict.keys():
        list_towns = []
        for town in towns_dict[country]:
            town = town.lower()
            town = rationalize_town_names(town, dic_town_symbols=bp_sg.DIC_TOWN_SYMBOLS,
                                          dic_town_words=bp_sg.DIC_TOWN_WORDS)
            town = remove_special_symbol(town, only_ascii=False, strip=False)
            town = town.strip()
            list_towns.append(town)
        towns_dict[country] = list_towns
    return towns_dict


def build_affils_useful_dicts(affil_params_dic):
    """Builds useful data for normalization of authors' affiliations.

    The data are built as 3 dicts through the `read_affil_types`, `build_norm_raw_affils_dict` 
    and `read_towns_per_country` functions of the same module.

    Args:
        affil_params_dic (dict): Keyed by ['affil_types_file_path', 'country_affils_file_path', \
        'country_towns_folder_path', 'country_towns_file'] and valued by the user as the full path to the data \
        per country of raw affiliations per normalized one, the full path to the data of affiliations-types \
        used to normalize the affiliations, the name of the file of the data of towns per country and the full \
        path to the folder where these are available.
    Returns:
        (tup): The 3 built dicts.
    Notes:
        When the 'country_affils_file_path', 'affil_types_file_path', 'country_towns_folder_path' \
        and 'country_towns_file' parameters are set to None, the values are defined by default internally to \
        the `build_norm_raw_affils_dict`, `read_affil_types` and `read_towns_per_country` functions.
    """
    (affil_types_file_path, country_affils_file_path,
     country_towns_folder_path, country_towns_file) = [None] * 4
    if affil_params_dic:
        params_keys = ['affil_types_file_path', 'country_affils_file_path',
                       'country_towns_folder_path', 'country_towns_file']
        (affil_types_file_path, country_affils_file_path,
         country_towns_folder_path, country_towns_file) = [affil_params_dic[key] for key in params_keys]

    # Building the useful data for affiliations normalization
    affil_types_dict = read_affil_types(affil_types_file_path=affil_types_file_path)
    norm_raw_affils_dict = build_norm_raw_affils_dict(country_affiliations_file_path=country_affils_file_path,
                                                     verbose=False)
    towns_dict = read_towns_per_country(country_towns_file=country_towns_file,
                                        country_towns_folder_path=country_towns_folder_path)
    return affil_types_dict, norm_raw_affils_dict, towns_dict
