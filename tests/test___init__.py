"""
Unit tests for the __init__.py module
"""

import os

import nltk
from nltk.data import ZipFilePathPointer

from bpfuncts import download_nltk_data

def test_download_nltk_data():
    download_nltk_data()

    assert os.path.exists(nltk.data.find('taggers\\averaged_perceptron_tagger_eng'))
    assert os.path.exists(nltk.data.find('tokenizers\\punkt'))
    assert isinstance(nltk.data.find('corpora\\wordnet.zip'), ZipFilePathPointer)