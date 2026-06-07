""" `biblioparsing` package __init__.
"""
__version__ = '3.0.0'
__author__ = 'BiblioAnalysis team'
__license__ = 'MIT'

# Standard library imports
import os

# 3rd party imports
import nltk

# Local imports
from biblioparsing.general_utils import *
from biblioparsing.general_globals import *
from biblioparsing.parsing_cols_globals import *
from biblioparsing.parsing_globals import *
from biblioparsing.regex_globals import *
from biblioparsing.affiliations_globals import *
from biblioparsing.parsing_utils import *
from biblioparsing.affil_norm_utils import *
from biblioparsing.scopus_rawdata_utils import *
from biblioparsing.scopus_parsing import *
from biblioparsing.scopus_parsing_complements import *
from biblioparsing.wos_rawdata_utils import *
from biblioparsing.wos_parsing import *
from biblioparsing.wos_parsing_complements import *
from biblioparsing.affiliations_parsing import *
from biblioparsing.concat_parsing import *
from biblioparsing.main_parsing import *
from biblioparsing.demo_utils import *

def download_nltk_data():
    """The function `download_nltk_data` downloads complementary libraries for nltk
    if they have not been already downloaded.

    To do that, it first checks if any of the potential full path of their dedicated folder exists.
    If not, it downloads the required libraries.
    """
    for nltk_path in nltk.data.path:
        if os.path.exists(nltk_path):
            return

    # Downloading useful complementary libraries since no nltk data have been already downloaded
    nltk.download('averaged_perceptron_tagger')
    nltk.download('punkt')
    nltk.download('wordnet')

download_nltk_data()
