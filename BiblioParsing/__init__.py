__version__ = '3.0.0'
__author__ = 'BiblioAnalysis team'
__license__ = 'MIT'

# Standard library imports
import os

# 3rd party imports
import nltk

# Local imports
from BiblioParsing.general_globals import *
from BiblioParsing.regex_globals import *
from BiblioParsing.specific_globals import *
from BiblioParsing.parsing_utils import *
from BiblioParsing.affil_norm_utils import *
from BiblioParsing.scopus_rawdata_utils import *
from BiblioParsing.scopus_parsing import *
from BiblioParsing.scopus_parsing_complements import *
from BiblioParsing.wos_rawdata_utils import *
from BiblioParsing.wos_parsing import *
from BiblioParsing.wos_parsing_complements import *
from BiblioParsing.affiliations_parsing import *
from BiblioParsing.concat_parsing import *
from BiblioParsing.main_parsing import *
from BiblioParsing.demo_utils import *

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
