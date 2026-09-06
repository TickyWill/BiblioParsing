""" `BiblioParsing` package __init__.
"""
__version__ = '3.1.0'
__author__ = 'BiblioAnalysis team'
__license__ = 'MIT'

# Standard library imports
import os

# 3rd party imports
import nltk

# Local imports
from bpfuncts.general_utils import *
from bpfuncts.general_globals import *
from bpfuncts.parsing_cols_globals import *
from bpfuncts.parsing_globals import *
from bpfuncts.regex_globals import *
from bpfuncts.affiliations_globals import *
from bpfuncts.parsing_utils import *
from bpfuncts.affil_norm_utils import *
from bpfuncts.scopus_rawdata_utils import *
from bpfuncts.scopus_parsing import *
from bpfuncts.scopus_parsing_complements import *
from bpfuncts.wos_rawdata_utils import *
from bpfuncts.wos_parsing import *
from bpfuncts.wos_parsing_complements import *
from bpfuncts.affiliations_parsing import *
from bpfuncts.concat_parsing import *
from bpfuncts.main_parsing import *
from bpfuncts.demo_utils import *

def download_nltk_data():
    """The function `download_nltk_data` downloads complementary libraries for nltk
    if they have not been already downloaded.

    To do that, it first checks if any of the potential full path of their dedicated folder exists.
    If not, it downloads the required libraries.
    Complementary libraries for nltk are downloaded into 'C:/Users/<user home>/AppData/Roaming/nltk_data'.

    For more information see: https://www.nltk.org/data.html
    """
    for nltk_path in nltk.data.path:
        if os.path.exists(nltk_path):
            return

    # Downloading useful complementary libraries since no nltk data have been already downloaded
    nltk.download('averaged_perceptron_tagger_eng')
    nltk.download('punkt_tab')
    nltk.download('wordnet')

download_nltk_data()
