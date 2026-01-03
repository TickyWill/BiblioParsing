__version__ = '2.0.0'
__author__ = 'BiblioAnalysis team'
__license__ = 'MIT'

from BiblioParsing.BiblioGeneralGlobals import *
from BiblioParsing.BiblioRegexpGlobals import *
from BiblioParsing.BiblioSpecificGlobals import *
from BiblioParsing.BiblioParsingUtils import *
from BiblioParsing.BiblioParsingWos import *
from BiblioParsing.BiblioParsingScopus import *
from BiblioParsing.BiblioParsingInstitutions import *
from BiblioParsing.BiblioParsingConcat import *
from BiblioParsing.BiblioParsingMain import *
from BiblioParsing.DemoUtils import *

def download_nltk_data():
    ''' The function `download_nltk_data` downloads complementary libraries for nltk 
    if they have not been already downloaded. 
    To do that, it first checks if any of the potential full path of their dedicated folder exist.  
    If not, it downloads the required libraries.
    '''
    
    # Standard library imports
    import os
    
    # 3rd party imports
    import nltk
    
    for nltk_path in nltk.data.path: 
        if os.path.exists(nltk_path): return
   
    # Downloading useful complementary libraries since no nltk data have been already downloaded    
    nltk.download('averaged_perceptron_tagger')
    nltk.download('punkt')
    nltk.download('wordnet')

download_nltk_data()






