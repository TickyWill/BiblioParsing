"""Module of functions for parsing of subjects fields of Scopus rawdata.
"""

__all__ = ['build_scopus_subjects_and_sub_subjects',
          ]


# 3rd party library imports
import pandas as pd


def _set_pub_subjects_list(pub_id, codes_df, code_cat_dict, sub_subject):
    """Builds the list of subjects or sub-subjects for a publication ID.

    Args:
        pub_id (int): The publication ID.
        codes_df (dataframe): The codes of the scopus categories \
        of the publication's journal.
        code_cat_dict (dict): The data of subjects or sub-subjects per code \
        of the scopus categories.
        sub_subject (bool): False if only subjects are targeted, True otherwise.
    Returns:
        (list): Composed of tuples of publication ID (int) and subject \
        or sub-subjects (str).
    """
    pub_res = [(pub_id,'')]
    codes_lists = codes_df.tolist()
    # Building subjects list of the publication for the given codes data
    pub_subjects = []
    for codes_list in codes_lists:
        # Dropping empty code
        codes_list = codes_list[:-1]
        for code in codes_list:
            # Selecting only subjects of codes multiple of 100 if sub_subject is False
            # otherwise selecting subjects and sub-subjects
            int_code = int(code)
            if not sub_subject:
                int_code = int(code.strip()[0:2] + "00")
            if int_code in code_cat_dict.keys():
                subject = code_cat_dict[int_code]
                if not sub_subject:
                    subject = subject.replace("General","")
                pub_subjects.append(subject)
    if pub_subjects:
        pub_res = [(pub_id, subject) for subject in pub_subjects]
    return pub_res


def _build_scopus_selected_subjects(corpus_df, scopus_journals_issn_cat_df, code_cat_dict,
                                    fails_dic, sub_subject, cols_list):
    """Builds the data of subjects or sub-subjects per publication ID.

    Args:
        corpus_df (dataframe): The selected rawdata of the corpus..
        scopus_journals_issn_cat_df (dataframe): The data of codes of the scopus \
        categories per journals.
        code_cat_dict (dict): The data of subjects or sub-subjects per code \
        of the scopus categories.
        fails_dic (dict): Parsing success rate data.
        sub_subject (bool): False if only subjects are targeted, True otherwise.
        cols_list (list): The useful column names.
    Returns:
        (dataframe): The built data.
    """
    # Builds the list of tuples [(pub ID, scopus category),...]
    # ex: [(0, 'Applied Mathematics'), (0, 'Materials Chemistry'),...]
    pub_id_col, scopus_journal_col, scopus_issn_col, keywords_col = cols_list
    corpus_series_zip = zip(corpus_df[pub_id_col], corpus_df[scopus_journal_col], corpus_df[scopus_issn_col])
    res = []
    for pub_id, pub_journal, pub_issn in corpus_series_zip:
        # Searching journal by name or by ISSN
        journal_keywords_df = scopus_journals_issn_cat_df.query('journal==@pub_journal')['keyword_id']
        journal_keywords_df = journal_keywords_df[journal_keywords_df!='Undefined']
        if not journal_keywords_df.empty:
            journal_pub_res = _set_pub_subjects_list(pub_id, journal_keywords_df, code_cat_dict, sub_subject)
            res.extend(journal_pub_res)
        else:
            issn_keywords_df = scopus_journals_issn_cat_df.query('issn==@pub_issn')['keyword_id']
            issn_keywords_df = issn_keywords_df[issn_keywords_df!='Undefined']
            if not issn_keywords_df.empty:
                issn_pub_res = _set_pub_subjects_list(pub_id, issn_keywords_df, code_cat_dict, sub_subject)
                res.extend(issn_pub_res)

    # Builds the data of subjects or sub_subjects per publication
    # The duplicated rows are suppressed.
    pub_ids_list, keywords_list = [[]] * 2
    if res:
        pub_ids_list, keywords_list = zip(*res)
    keywords_df = pd.DataFrame.from_dict({pub_id_col  : pub_ids_list,
                                          keywords_col: keywords_list})
    fails_dic[keywords_col] = {'success (%)': 0,
                               pub_id_col   : []}
    if pub_ids_list:
        out_pub_ids_list = keywords_df[keywords_df[keywords_col]==''][pub_id_col].values
        fails_dic[keywords_col] = {'success (%)': 100 * (1-len(out_pub_ids_list) / len(pub_ids_list)),
                                   pub_id_col   : [int(x) for x in out_pub_ids_list]}

    keywords_df = keywords_df.drop_duplicates()
    keywords_df = keywords_df[keywords_df[keywords_col]!='']
    return keywords_df


def build_scopus_subjects_and_sub_subjects(corpus_df, scopus_cat_codes_path,
                                           scopus_journals_issn_cat_path, fails_dic, cols_tup):
    """Builds the data of subjects and sub-subjects per publication of the corpus 
    and updates the parsing success rate data.

    The built data are composed of 2 columns and one row per publication and subject or sub-subjects.
    Ex:
    Pub_id   Subject.
    0       Mathematics.
    0       Engineering.
    1	    Physics and Astronomy.
    1	    Biochemistry, Genetics and Molecular Biology.

    The subjects are attributed using 2 files provided by Elsevier.
    The "scopus_cat_codes.txt" file gives a code per category:
    Category                         Code.
    General Medicine                 2700    => Subject.
    Medicine (miscellaneous)         2701    => Sub-subject.
    Anatomy                          2702.
    Anesthesiology and Pain Medicine 2703.
    Biochemistry, medical            2704.
    etc.

    The "scopus_journals_issn_cat.txt" file give the categories codes attached to a journal:
    Journal               ISSN           Codes.
    21st Century Music   15343219     1210;.
    2D Materials                      2210; 2211; 3104; 2500; 1600;.
    3 Biotech            2190572X     1101; 2301; 1305;.
    etc.

    For "2D Materials journal":
    - The subjects are given by the codes multiple of 100: 2500; 1600.
    - The sub-subjects are given by the other codes: 2210; 2211; 3104.

    Args:
        corpus_df (dataframe): The selected rawdata of the corpus.
        scopus_cat_codes_path (path): The full path to the txt file "scopus_cat_codes".
        scopus_journals_issn_cat_path (path): The full path to the txt file "scopus_journals_issn_cat".
        fails_dic (dict): Parsing success rate data.
        cols_tup (tup): Columns information as built through the `_set_scopus_parsing_cols` internal function.
    Returns:
        (tuple): The subjects data (dataframe) and sub-subjects data (dataframe) built.
    """
    # Setting useful column names
    _, cols_dic, scopus_cols_dic = cols_tup
    cols_keys = ['pub_id_col', 'subject_col', 'sub_subject_col']
    (pub_id_col, subject_col, sub_subject_col) = [cols_dic[key] for key in cols_keys]
    scopus_cols_keys = ['scopus_journal_col', 'scopus_issn_col']
    (scopus_journal_col, scopus_issn_col) = [scopus_cols_dic[key] for key in scopus_cols_keys]

    # Builds the dict "code_cat_dict" {ASJC classification codes:description} out
    # of the file "scopus_cat_codes.txt"
    # ex: {1000: 'Multidisciplinary', 1100: 'General Agricultural',...}
    # -----------------------------------------------------------------------
    scopus_cat_codes_df = pd.read_csv(scopus_cat_codes_path, sep='\t', header=None)
    code_cat_dict = dict(zip(scopus_cat_codes_df[1].fillna(0.0).astype(int), scopus_cat_codes_df[0]))

    # Builds the dataframe "scopus_journals_issn_cat_df" out of the file
    # "scopus_journals_issn_cat.txt"
    # "scopus_journals_issn_cat_df" has 3 columns:
    #       "journal": scopus journal name
    #       "issn": journal issn
    #       "keyword_id": list of keywords IDs associated to the journal or the issn
    # -----------------------------------------------------------------------------
    scopus_journals_issn_cat_df = pd.read_csv(scopus_journals_issn_cat_path, sep='\t',
                                              header=None).fillna(0)
    scopus_journals_issn_cat_df[2] = scopus_journals_issn_cat_df[2].str.split(';')
    scopus_journals_issn_cat_df.columns = ['journal','issn','keyword_id']
    scopus_journals_issn_cat_df['keyword_id'] = scopus_journals_issn_cat_df['keyword_id'].fillna(0)
    scopus_journals_issn_cat_df = scopus_journals_issn_cat_df.replace(0, 'Undefined')

    # Building subjects data
    sub_subject = False
    cols_list = [pub_id_col, scopus_journal_col, scopus_issn_col, subject_col]
    subjects_df = _build_scopus_selected_subjects(corpus_df, scopus_journals_issn_cat_df, code_cat_dict,
                                                  fails_dic, sub_subject, cols_list)

    # Building sub-subjects data
    sub_subject = True
    cols_list = [pub_id_col, scopus_journal_col, scopus_issn_col, sub_subject_col]
    sub_subjects_df = _build_scopus_selected_subjects(corpus_df, scopus_journals_issn_cat_df, code_cat_dict,
                                                      fails_dic, sub_subject, cols_list)

    return subjects_df, sub_subjects_df
