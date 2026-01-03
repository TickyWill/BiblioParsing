__all__ = ['biblio_parser',
           'merge_database',
           ]


# Local library imports
import BiblioParsing.BiblioSpecificGlobals as bp_sg
from BiblioParsing.BiblioParsingScopus import biblio_parser_scopus
from BiblioParsing.BiblioParsingScopus import read_database_scopus
from BiblioParsing.BiblioParsingWos import biblio_parser_wos
from BiblioParsing.BiblioParsingWos import read_database_wos


def merge_database(database, filename, in_dir, out_dir):
    """Merges several corpus of same database type in one corpus.
    
    Args:
        database (str): database type (scopus or wos).
        filename (str): name of the merged database.
        in_dir (str): name of the folder where the corpuses are saved.
        out_dir (str): name of the folder where the merged corpuses will be saved. 
    """
    rawdata_paths_list = []
    rawdata_list = []
    if database==bp_sg.WOS:
        for path, _, files in os.walk(in_dir):
            rawdata_paths_list.extend(Path(path) / Path(file) for file in files
                                      if file.endswith(".txt"))
        for file_path in rawdata_paths_list:
            rawdata_list.append(read_database_wos(file_path)[0])

    elif database==bp_sg.SCOPUS:
        for path, _, files in os.walk(in_dir):
            rawdata_paths_list.extend(Path(path) / Path(file) for file in files
                                      if file.endswith(".csv"))
        for file_path in rawdata_paths_list:
            rawdata_list.append(read_database_scopus(file_path)[0])
    else:
        raise Exception(f"Sorry, unrecognized database {database}: "
                        f"should be {bp_sg.WOS} or {bp_sg.SCOPUS} ")
        
    result = pd.concat(rawdata_list, ignore_index=True)
    result.to_csv(out_dir / Path(filename), sep='\t')


def biblio_parser(rawdata_path, database, inst_filter_list=None,
                  country_affiliations_file_path=None,
                  inst_types_file_path=None,
                  country_towns_file=None,
                  country_towns_folder_path=None):
    """Parses corpus rawdata using the appropriate parser.
    
    Two parsers are available:
    - `biblio_parser_wos` function imported from `BiblioParsingWos` module; 
    - `biblio_parser_scopus` function imported from `BiblioParsingScopus` module.
    
    Args:
        rawdata_path (path): The full path to the corpus rawdata.
        database (str): The type of the rawdata among Scopus or WoS.
        inst_filter_list (list): The affiliations-filter composed of a list of normalized affiliations (str), \
        optional (default=None).
        country_affiliations_file_path (path): The full path to the data per country of raw affiliations \
        per normalized one, optional (default=None).
        inst_types_file_path (path): The full path to the data of institutions-types used to normalize \
        the affiliations, optional (default=None).
        country_towns_file (str): The name of the file of the data of towns per country, optional (default=None).
        country_towns_folder_path (path): The full path to the folder where the 'country_towns_file' file \
        is available, optional (default=None).
    Returns:
        (tup): The tuple of parsing results returned by the used appropriate parser.
    """
    if database==bp_sg.WOS:
        parsing_tup = biblio_parser_wos(rawdata_path, inst_filter_list=inst_filter_list,
                                        country_affiliations_file_path=country_affiliations_file_path,
                                        inst_types_file_path=inst_types_file_path,
                                        country_towns_file=country_towns_file,
                                        country_towns_folder_path=country_towns_folder_path)
    elif database==bp_sg.SCOPUS:
        parsing_tup = biblio_parser_scopus(rawdata_path, inst_filter_list=inst_filter_list,
                                           country_affiliations_file_path=country_affiliations_file_path,
                                           inst_types_file_path=inst_types_file_path,
                                           country_towns_file=country_towns_file,
                                           country_towns_folder_path=country_towns_folder_path)
    else:
        raise Exception(f"Sorry, unrecognized database {database} : should be {bp_sg.WOS} or {bp_sg.SCOPUS}")
        
    return parsing_tup