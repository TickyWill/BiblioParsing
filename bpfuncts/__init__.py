""" `BiblioParsing` package __init__.
"""
__version__ = '3.1.0'
__author__ = 'BiblioAnalysis team'
__license__ = 'MIT'

# Standard library imports
import os
import sys
from pathlib import Path

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
from bpfuncts.scopus_references_parsing import *
from bpfuncts.scopus_subjects_parsing import *
from bpfuncts.scopus_parsing import *
from bpfuncts.wos_rawdata_utils import *
from bpfuncts.wos_references_parsing import *
from bpfuncts.wos_subjects_parsing import *
from bpfuncts.wos_parsing import *
from bpfuncts.affiliations_parsing import *
from bpfuncts.concat_parsing import *
from bpfuncts.main_parsing import *
from bpfuncts.demo_utils import *


def download_nltk_data():
    """Downloads complementary libraries for nltk if they have not been already downloaded.

    The libraries to dowload depend on the python version.
    To do that, it first checks if any of the potential full path of their dedicated folder exists.
    If not, it downloads the required libraries.
    Complementary libraries for nltk are downloaded into 'C:/Users/<user home>/AppData/Roaming/nltk_data'.

    For more information see: https://www.nltk.org/data.html
    """
    # Setting the complementary libraries to download
    nltk_data_folders = ['taggers', 'tokenizers', 'corpora']
    if sys.version_info>=(3, 9):
        nltk_data_sub_folders = ['averaged_perceptron_tagger_eng', 'punkt_tab'] + ['wordnet']
    else:
        nltk_data_sub_folders = ['averaged_perceptron_tagger', 'punkt'] + ['wordnet']
    nltk_data_dict = dict(zip(nltk_data_folders, nltk_data_sub_folders))

    # Checking if the complementary libraries are already available
    status_list = []
    for nltk_path in nltk.data.path:
        status = False
        if os.path.exists(nltk_path):
            status = True
            for folder, sub_folder in nltk_data_dict.items():
                folder_path = Path(nltk_path) / Path(folder)
                sub_folder_path = folder_path / Path(sub_folder)
                if not folder_path.is_dir() or not sub_folder_path.is_dir():
                    status = False
        status_list.append(status)

    # Downloading the missing libraries in 'C:/Users/<user home>/AppData/Roaming/nltk_data'
    if not any(status_list):
        for nltk_data_to_load in nltk_data_sub_folders:
            nltk.download(nltk_data_to_load)


download_nltk_data()
