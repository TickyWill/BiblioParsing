# Standard library imports
import os

# 3rd party imports
import nltk

nltk_data_status = False
for nltk_path in nltk.data.path:    
    if os.path.exists(nltk_path):
        nltk_data_status = True
        
if not nltk_data_status:
    nltk.download('averaged_perceptron_tagger')
    nltk.download('punkt')
    nltk.download('wordnet')
