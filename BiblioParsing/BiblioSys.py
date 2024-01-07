""" The `BiblioSys` module is a set of system-management functions
for running BiblioAnalysis on different operating systems.
"""

__all__ = ['add_site_packages_path',]


def add_site_packages_path(venv = False):
    ''' The function `add_site_packages_path` adds `site_packages` path to `sys.path`
    for MacOs when not working with virtual environment.
    
    Args:
        os_name (str): name of the operating system ('Darwin' or 'Windows')
        venv (bool): status of virtual environment use.
        
    '''
    # Standard library imports
    import os
    import sys
    
    ## Get the information of current operating system
    mac_packages = None
    if venv == False:
        # Add path of 'site-packages' where useful packages are stored on MacOS;
        # no impact for Windows
        packages_dir = 'site-packages'
        search_path = '/Library/Frameworks/Python.framework/Versions/current/lib'
        for root, dirs, files in os.walk(search_path):
            list_packages = [os.path.abspath(os.path.join(root, name)) for name in dirs if name == packages_dir]
            if list_packages != []: mac_packages = list_packages[0]   
        sys.path.append(mac_packages)        
    return mac_packages
