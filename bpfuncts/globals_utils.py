__all__ = ['build_countries_globals',
           'read_yaml_affiliations_globals',
           'read_yaml_general_globals',
           'read_yaml_parsing_cols_globals',
           'read_yaml_parsing_globals',
           'read_yaml_regex_globals',
          ]


# Standard library imports
import ast
from pathlib import Path

# 3rd party imports
import pandas as pd
import yaml


def _check_gf_path(gf_path):
    if not gf_path:
        gf_path = Path(__file__).parent / Path('GlobalsYaml')
    return gf_path


def read_yaml_general_globals(gf_path=None):
    # Setting the YAML file path
    gf_path = _check_gf_path(gf_path)
    general_globals_file_path = gf_path / Path("GeneralGlobals.yaml")

    # Reading the YAML file setting the general globals
    with open(general_globals_file_path, encoding='utf8') as infile:
        general_globals_dic = yaml.safe_load(infile)
    return general_globals_dic


def read_yaml_parsing_globals(gf_path=None):
    # Setting the YAML file path
    gf_path = _check_gf_path(gf_path)
    parsing_globals_file_path = gf_path / Path("ParsingGlobals.yaml")

    # Reading the YAML file setting the parsing's globals
    with open(parsing_globals_file_path, encoding='utf8') as infile:
        parsing_globals_dic = yaml.safe_load(infile)
    return parsing_globals_dic


def read_yaml_parsing_cols_globals(gf_path=None):
    # Setting the YAML file path
    gf_path = _check_gf_path(gf_path)
    parsing_cols_globals_file_path = gf_path / Path("ParsingColsGlobals.yaml")

    # Reading the YAML file setting the parsing-columns' globals
    with open(parsing_cols_globals_file_path, encoding='utf8') as infile:
        parsing_cols_globals_dic = yaml.safe_load(infile)
    return parsing_cols_globals_dic


def read_yaml_regex_globals(gf_path=None):
    # Setting the YAML file path
    gf_path = _check_gf_path(gf_path)
    regex_globals_file_path = gf_path / Path("RegexGlobals.yaml")

    # Reading the YAML file setting the regex' globals
    with open(regex_globals_file_path, encoding='utf8') as infile:
        regex_globals_dic = yaml.safe_load(infile)
    return regex_globals_dic


def read_yaml_affiliations_globals(gf_path=None):
    # Setting the YAML file path
    gf_path = _check_gf_path(gf_path)
    affiliations_globals_file_path = gf_path / Path("AffiliationsGlobals.yaml")

    # Reading the YAML file setting the affiliations' globals
    with open(affiliations_globals_file_path, encoding='utf8') as infile:
        affiliations_globals_dic = yaml.safe_load(infile)
    return affiliations_globals_dic


def build_countries_globals(rep_utils, countries_info, countries_col_names):
    """Builds countries list and their attributes as given in the dedicated EXCEL file.

    The name of the file is given by the 'countries_info' global defined in the same module. 
    The file is located in the folder of the package which name is given by the 'rep_utils' global 
    defined in the same module. 
    The function builds the 'countries' countries list and the 'countries_gps' dict keyed by countries 
    and valued by a tuple '(lat,long)' where 'lat' is the country capital latitude and 'long', the country capital 
    longitude expressed in decimal degrees. 
    It builds also the 'countries_codes' dict keyed by countries and valued by the ISO code (3 letters) of the country 
    and the 'zip_codes' hierarchical dict where the outer dict is keyed by countries and valued by an inner dict keyed 
    by 'letters' and 'digits' of the zip-code.

        ex: zip_codes['France'] = {'letters': ['f', 'fr'], 'digits': [5, 6]} where the given digits are the possible \
        number of digits in the zip-code.

    Finally, it builds the 'countries_continent' dict keyed by countryes and valued by the country's continent.

    Returns:
        (list, dict, dict, dict): tuple of the built data.
    """
    # Setting columns name aliases
    col_keys = ['country', 'gps', 'short_name', 'zip_letters', 'zip_digits', 'continent']
    (countries_col, gps_col, short_col, zip_letters_col,
     zip_digits_col, continent_col) = [countries_col_names[key] for key in col_keys]

    # Setting the specific file paths for countries information
    path_countries_info = Path(__file__).parent / Path(rep_utils) / Path(countries_info)
    df = pd.read_excel(path_countries_info)

    countries = df[countries_col].to_list()
    countries_gps = {x[0]:ast.literal_eval(x[1])
                     for x in zip(df[countries_col], df[gps_col])}
    countries_codes = {x[0]:x[1] for x in zip(df[countries_col], df[short_col])}
    zip_codes = {x[0]:{'letters':ast.literal_eval(x[1]), 'digits':ast.literal_eval(x[2])}
                 for x in zip(df[countries_col], df[zip_letters_col], df[zip_digits_col])}
    countries_continent = {x[0]:x[1] for x in zip(df[countries_col], df[continent_col])}

    return countries, countries_gps, countries_codes, zip_codes, countries_continent
