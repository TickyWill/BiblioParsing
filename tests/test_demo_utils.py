"""

"""

import bpfuncts.demo_utils as demo_utils

def test__get_demo_config():
    assert demo_utils._get_demo_config() == {
        'Description': [
            "The field 'PARSING_FOLDER_ARCHI' sets the default architecture of the working folder for BiblioParsing.",
            "The field 'PARSING_FILE_NAMES' sets the default file names for each of the parsed items."
        ],
        'PARSING_FOLDER_ARCHI': {
            'folder_root': 'BiblioParsing_Files',
            'corpus': {
                'corpus_root': 'Corpus',
                'database': {
                    'rawdata': 'rawdata',
                    'parsing': 'parsing'
                },
                'concat': {
                    'root': 'concatenation',
                    'parsing': 'parsing'
                },
                'dedup': {
                    'root': 'deduplication',
                    'parsing': 'parsing'
                }
            }
        },
        'PARSING_FILE_NAMES': {
            'articles': 'articles',
            'authors': 'authors',
            'addresses': 'addresses',
            'countries': 'countries',
            'institutions': 'institutions',
            'authors_institutions': 'authorsinst',
            'authors_keywords': 'authorskeywords',
            'indexed_keywords': 'journalkeywords',
            'title_keywords': 'titlekeywords',
            'subjects': 'subjects',
            'sub_subjects': 'subjects2',
            'references': 'references',
            'norm_institutions': 'addressesnorminst',
            'raw_institutions': 'addressesrawinst'
        }
    }


def test__build_demo_effective_config():
    pass


def test__create_folder():
    pass


def test__create_year_demo_folders():
    pass


def test_set_demo_user_config():
    pass


def test_save_demo_parsing_dict():
    pass


def test_save_demo_fails_dict():
    pass


def test_save_demo_db_ids_data():
    pass


def test_save_demo_parsing_dicts():
    pass


def test_set_demo_step_affil_parsing_paths():
    pass


def test_read_demo_parsing_dict():
    pass
