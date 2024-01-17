'''
'''
__all__ = ['DEMO_GLOBAL',]


def get_config_dir():

    """
    Returns a parent directory path
    where persistent application data can be stored.

    # linux: ~/.local/share
    # macOS: ~/Library/Application Support
    # windows: C:/Users/<USER>/AppData/Roaming
    adapted from : https://stackoverflow.com/questions/19078969/python-getting-appdata-folder-in-a-cross-platform-way
    """
    # Standard library imports
    import sys
    from pathlib import Path
    
    home = Path.home()

    if sys.platform == 'win32':
        return home / Path('AppData/Roaming')
    elif sys.platform == 'linux':
        return home / Path('.local/share')
    elif sys.platform == 'darwin':
        return home / Path('Library/Application Support')

def _dump_json(file_path, json_dict):
    # Standard library imports
    import json
    
    with open(file_path, 'w') as file:
        json.dump(json_dict, file, indent=4)
    

def _config_parsing_folders(json_file_name):
    # Standard library imports
    import json
    import os.path
    from pathlib import Path
    
    # Reads the default json_file_name config file
    pck_config_path = Path(__file__).parent / Path('CONFIG') / Path(json_file_name)
    pck_config_date = os.path.getmtime(pck_config_path)
    with open(pck_config_path) as file:
        json_config_dict = json.load(file)
        
    # Sets the json_config_dict according to the status of the local config file        
    local_config_dir_path  = get_config_dir() / Path('BiblioParsing')
    local_config_file_path = local_config_dir_path  / Path(json_file_name)
    
    if os.path.exists(local_config_file_path):
        # Local json_file_name config file exists
        local_config_date = os.path.getmtime(local_config_file_path)
        if local_config_date > pck_config_date:
            # Local config file is more recent than package one
            # thus json_config_dict is defined by the local config file
            with open(local_config_file_path) as file:
                json_config_dict = json.load(file)
        else:
            # Local config file is less recent than package one
            # thus package config file overwrite the local config file 
            # and json_config_dict is kept at default values
            _dump_json(local_config_file_path, json_config_dict)
    else:
        # Local json_file_name config file does not exist
        # thus package config file is used to create a local config file
        # to be filled by the user
        # and json_config_dict is kept at default values
        if not os.path.exists(get_config_dir() / Path('BiblioParsing')):
            os.makedirs(get_config_dir() / Path('BiblioParsing'))
        _dump_json(local_config_file_path, json_config_dict)      
    
    return (json_config_dict, local_config_file_path)


DEMO_GLOBAL, _= _config_parsing_folders('BiblioParsing_config.json')

