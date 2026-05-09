"""Module of functions for reading and cleaning of WoS rawdata.
"""

__all__ = ['read_wos_rawdata']

# Standard library imports
import csv

# 3rd party library imports
import numpy as np
import pandas as pd

# Local libray imports
import BiblioParsing.parsing_cols_globals as bp_pcg
import BiblioParsing.parsing_globals as bp_pg
from BiblioParsing.parsing_utils import build_pub_db_ids
from BiblioParsing.parsing_utils import check_and_drop_columns
from BiblioParsing.parsing_utils import check_and_get_rawdata_file_path
from BiblioParsing.parsing_utils import drop_rawdata
from BiblioParsing.parsing_utils import normalize_journal_names


def _set_wos_rawdata_cols():
    """Builds 2 dict setting selected columns names for the process of getting WoS rawdata.

    Returns:
        (tup): (A dict valued by column names of parsing results defined by the \
        'COL_NAMES' global, A dict valued by column names of rawdata defined \
        by the 'COLUMN_LABEL_WOS' and 'COLUMN_LABEL_WOS_PLUS' globals).
    """
    cols_dic = {'wos_id_col': bp_pcg.COL_NAMES['wos_id'][0],
                'pub_id_col': bp_pcg.COL_NAMES['pub_id'],
               }

    wos_cols_dic = {'init_wos_id_col': bp_pcg.COLUMN_LABEL_WOS_PLUS['wos_id'],
                   }

    return cols_dic, wos_cols_dic


def read_wos_rawdata(rawdata_path, wos_ids=False):
    """Reads the file of WoS rawdata available in the indicated folder.

    The function:
    - Allows to circumvent the error ParserError ('	' expected after '"') generated \
    by the method `pd.read_csv` when reading the raw wos-database file
    - Checks columns and drops unused columns by the parsing process using the \
    `check_and_drop_columns` function imported from `parsing_utils` module.
    - Replaces the unavailable items values by a string set in the global UNKNOWN.
    - Adds an index column.
    - Normalizes the journal names using the `normalize_journal_names` function \
    imported from the `parsing_utils` module.
    Finally, the function can built data of WoS identifiers of the publications.
    The returned data are initialized to empty dataframes.

    Args:
        rawdata_path (path): The full path to the WoS-rawdata file.
        wos_ids (bool): Optional, true for building the data of WoS IDs of \
        publications (dafault=False).
    Returns:
        (tup): (The cleaned corpus data (dataframe), The WoS-IDs data (dataframe)).
    """
    # Setting columns for wos parsing process
    cols_tup = _set_wos_rawdata_cols()
    cols_dic, wos_cols_dic = cols_tup
    wos_id_col = cols_dic['wos_id_col']
    init_wos_id_col = wos_cols_dic['init_wos_id_col']
    wos_ids_cols_list = [wos_id_col, init_wos_id_col]

    # Initializing returned data to empty dataframes
    wos_rawdata_df = pd.DataFrame()
    wos_ids_df = pd.DataFrame()

    # Check if rawdata file is available and get its full path if it is
    rawdata_file_path = check_and_get_rawdata_file_path(rawdata_path, bp_pg.WOS_RAWDATA_EXTENT)

    if rawdata_file_path:
        # Extending the field size limit for reading .txt files
        csv.field_size_limit(bp_pg.FIELD_SIZE_LIMIT)

        with open(rawdata_file_path, 'rt', encoding=bp_pg.ENCODING) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter='\t')
            csv_list = []
            for row in csv_reader:
                csv_list.append(row)
        init_full_wos_rawdata_df = pd.DataFrame(csv_list)

        if len(init_full_wos_rawdata_df):
            # Setting columns name to raw 0
            init_full_wos_rawdata_df.columns = init_full_wos_rawdata_df.iloc[0]
            init_full_wos_rawdata_df = init_full_wos_rawdata_df.drop(0)

            # Trying to drop data by wos identifier given in an XLSX file
            full_wos_rawdata_df = drop_rawdata(rawdata_path, init_full_wos_rawdata_df,
                                               wos_ids_cols_list, bp_pg.WOS)

            # Selecting useful rawdata
            wos_rawdata_df = check_and_drop_columns(bp_pg.WOS, full_wos_rawdata_df)
            wos_rawdata_df = wos_rawdata_df.replace(np.nan, bp_pg.UNKNOWN, regex=True)
            wos_rawdata_df = normalize_journal_names(bp_pg.WOS, wos_rawdata_df)

            if wos_ids:
                # Building the WoS-IDs data
                wos_ids_df = build_pub_db_ids(full_wos_rawdata_df, init_wos_id_col, wos_id_col)
    return_tup = (wos_rawdata_df, wos_ids_df)
    return return_tup
