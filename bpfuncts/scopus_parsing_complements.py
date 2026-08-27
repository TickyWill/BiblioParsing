"""Module of functions for parsing of subjects and references fields 
of Scopus rawdata.
"""

__all__ = ['build_scopus_references',
           'build_scopus_subjects_and_sub_subjects',
          ]


# Standard library imports
import re
from collections import namedtuple

# 3rd party library imports
import pandas as pd

# Local libray imports
import bpfuncts.parsing_globals as bp_pg
import bpfuncts.regex_globals as bp_rg


def _set_pub_subjects_list(pub_id, codes_df, code_cat_dict, sub_subject):
    pub_res = [(pub_id,'')]
    codes_lists = codes_df.tolist()
    # Building subjects list of the publication for the given codes data
    pub_subjects = []
    for codes_list in codes_lists:
        # Droping empty code
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
            Pub_id   Subject
              0      Mathematics
              0      Engineering
              1	     Physics and Astronomy
              1	     Biochemistry, Genetics and Molecular Biology

    The subjects are attributed using 2 files provided by Elsevier.
    The "scopus_cat_codes.txt" file gives a code per category:

                         Category            Code
            General Medicine                 2700    => Subject
            Medicine (miscellaneous)         2701    => Sub-subject
            Anatomy                          2702
            Anesthesiology and Pain Medicine 2703
            Biochemistry, medical            2704
            ...

    The "scopus_journals_issn_cat.txt" file give the categories codes attached to a journal:

                Journal            ISSN           Codes
            21st Century Music   15343219     1210;
            2D Materials                      2210; 2211; 3104; 2500; 1600;
            3 Biotech            2190572X     1101; 2301; 1305;
            ...

    For "2D Materials journal":
        - The subjects are given by the codes multiple of 100: 2500; 1600
        - The sub-subjects are given by the other codes: 2210; 2211; 3104

    Args:
        corpus_df (dataframe): The selected rawdata of the corpus.
        scopus_cat_codes_path (path): The full path to the file "scopus_cat_codes.txt".
        scopus_journals_issn_cat_path (path): The full path to the file "scopus_journals_issn_cat.txt".
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
    #       "keyword_id": list of keywords id asociated to the journal or the issn
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


# Functions for parsing cleaned reference
def _try_list_idx(item_idx, value_idx, values_list):
    try:
        value_item_idx, value = item_idx, values_list[value_idx].strip()
    except IndexError:
        value_item_idx, value = 0, bp_pg.UNKNOWN
    return value_item_idx, value


def _find_ref_doi(ref_items_list, verbose):
    item_idx, dois_list = 0, []
    for item_idx, ref_item in enumerate(ref_items_list):
        dois_list = re.findall(bp_rg.RE_SCOPUS_REF_DOI, ref_item)
        if dois_list:
            break
    if verbose:
        print("\n    DOI search:", item_idx, dois_list)
    doi_item_idx, doi = _try_list_idx(item_idx, 0, dois_list)
    return doi_item_idx, doi


def _find_ref_year(ref_items_list, doi_item_idx, doi, ref, verbose):
    item_idx, years_list = 0, []
    for item_idx, ref_item in enumerate(ref_items_list):
        years_list = re.findall(bp_rg.RE_SCOPUS_REF_YEAR, ref_item)
        if years_list:
            break
    if not years_list:
        # Getting possible years in full ref
        all_years_list = re.findall(bp_rg.RE_SCOPUS_REF_YEARS, ref)
        for item_idx, ref_item in enumerate(ref_items_list):
            if item_idx==doi_item_idx and doi!=bp_pg.UNKNOWN:
                years_list = re.findall(bp_rg.RE_SCOPUS_REF_DOI_YEAR, ref_item)
                if years_list:
                    # Droping surrounding dots
                    years_list = [x[1:-1] for x in years_list if x]
                    break
            else:
                years_list = re.findall(bp_rg.RE_SCOPUS_REF_YEARS, ref_item)
                if years_list:
                    break
                if re.findall(bp_rg.RE_SCOPUS_REF_CONF, ref_item):
                    years_list = re.findall(bp_rg.RE_SCOPUS_REF_DIGITS, ref_item)
                    if not all_years_list:
                        break
    if verbose:
        print("\n    Year search:", item_idx, years_list)
    year_item_idx, year = _try_list_idx(item_idx, 0, years_list)
    return year_item_idx, year


def _check_author_in_next_items(init_auth_idx_max, author_step, idx_max, ref_items_list, verbose):
    auth_idx_max = init_auth_idx_max + author_step
    if idx_max>init_auth_idx_max:
        idx = init_auth_idx_max + author_step
        while re.findall(bp_rg.RE_SCOPUS_REF_AUTHOR, ref_items_list[idx]):
            auth_idx_max = idx
            idx += author_step
            if verbose:
                print("\n        idx:", idx)
                print("        auth_idx_max 1:", auth_idx_max)
            if idx>idx_max:
                break
    return auth_idx_max


def _check_authors_case_w_dot(authors_case_base, item, verbose):
    authors_case = authors_case_base + '_w_dot'
    if '.' not in item:
        authors_case = authors_case_base + '_wo_dot'
    if verbose:
        print("        authors_case:", authors_case)
    return authors_case


def _search_authors_in_items(ref_items_list, verbose):
    # Searching for possible author names and specifying the author names structure
    # only in the case of idx_max > 0
    first_author, et_al, authors_case = bp_pg.UNKNOWN, "", 'no_authors'
    idx_max = len(ref_items_list) - 1
    auth_idx_max = 0
    first_item = ref_items_list[0]
    if verbose:
        print("\n    RE_REF_AUTHOR_DROP in first item:",
              re.findall(bp_rg.RE_REF_AUTHOR_DROP, first_item))
    if not re.findall(bp_rg.RE_REF_AUTHOR_DROP, first_item):
        second_item = ref_items_list[1]
        if verbose:
            print("\n    RE_SCOPUS_REF_AUTHOR in first item:",
                  re.findall(bp_rg.RE_SCOPUS_REF_AUTHOR, first_item))

        if re.findall(bp_rg.RE_SCOPUS_REF_AUTHOR, first_item):
            authors_case = 'single_one_item'
            author_step = 1
            first_author = first_item
            # Checking if multiple authors and computing max index of authors' items
            if verbose:
                print("\n    RE_SCOPUS_REF_AUTHOR in second item 1:",
                      re.findall(bp_rg.RE_SCOPUS_REF_AUTHOR, second_item))
            if re.findall(bp_rg.RE_SCOPUS_REF_AUTHOR, second_item):
                authors_case = _check_authors_case_w_dot('multiple_one_item', second_item, verbose)
                et_al = "et al."
                auth_idx_max = _check_author_in_next_items(auth_idx_max, author_step, idx_max,
                                                           ref_items_list, verbose)
        else:
            if verbose:
                print("\n    RE_SCOPUS_REF_AUTHOR in second item 2:",
                      re.findall(bp_rg.RE_SCOPUS_REF_AUTHOR, second_item))
            if re.findall(bp_rg.RE_SCOPUS_REF_AUTHOR, second_item):
                authors_case = 'single_two_items'
                author_step = 2
                first_author = f'{first_item} {second_item}'
                auth_idx_max = 1
                # Checking if multiple authors and computing max index of authors' items
                next_auth_idx_max = auth_idx_max + author_step
                if idx_max>next_auth_idx_max:
                    fourth_item = ref_items_list[next_auth_idx_max]
                    if verbose:
                        print(f"\n    RE_SCOPUS_REF_AUTHOR in {fourth_item}:",
                              re.findall(bp_rg.RE_SCOPUS_REF_AUTHOR, fourth_item))
                    if re.findall(bp_rg.RE_SCOPUS_REF_AUTHOR, fourth_item):
                        authors_case = _check_authors_case_w_dot('multiple_two_items', fourth_item, verbose)
                        et_al = "et al."
                        auth_idx_max = _check_author_in_next_items(auth_idx_max, author_step, idx_max,
                                                                   ref_items_list, verbose)
    if verbose:
        print("\n    Authors search in item:", first_author, et_al, authors_case, auth_idx_max)
    return first_author, et_al, authors_case, auth_idx_max


def _build_authors_attr(ref_items_list, verbose):
    first_author, et_al, authors_case = bp_pg.UNKNOWN, "", 'no_authors'
    idx_max = len(ref_items_list) - 1
    author_max_len = 30
    auth_idx_max = 0
    first_item = ref_items_list[0]
    if re.findall(bp_rg.RE_SCOPUS_REF_ET_AL, first_item):
        authors_case = 'with_et_al'
        first_author = first_item
    else:
        if ":" in first_item or len(first_item)>author_max_len:
            authors_case = 'first_item_too_long'
        if idx_max>0:
            if authors_case=='first_item_too_long':
                # Searching for possible author name in second item
                second_item = ref_items_list[1]
                if re.findall(bp_rg.RE_SCOPUS_REF_AUTHOR, second_item):
                    authors_case = 'partial_one_item'
                    first_author = second_item
                    et_al = "etc."
            else:
                # Searching for possible author names beginning from first item
                first_author, et_al, authors_case, auth_idx_max = _search_authors_in_items(ref_items_list,
                                                                                           verbose)
    if verbose:
        print("\n    Authors search in item:", first_author, et_al, authors_case, auth_idx_max)
    return first_author, et_al, authors_case, auth_idx_max


def _set_dotted_initials(first_author, verbose):
    dotted_first_author = first_author
    initial_dot = '.'
    if first_author!=bp_pg.UNKNOWN and initial_dot not in first_author:
        lastname = ' '.join(first_author.split(" ")[:-1])
        initials = first_author.split(" ")[-1]
        initials_list = [f'{x}{initial_dot}' for x in initials.split("-") if x]
        new_initials = '-'.join(initials_list)
        dotted_first_author = f'{lastname} {new_initials}'
    if verbose:
        print("\n    Dotted first author:", dotted_first_author)
    return dotted_first_author


def _find_ref_authors(ref_items_list, verbose):
    first_author, et_al, authors_case, auth_idx_max = _build_authors_attr(ref_items_list,
                                                                          verbose)
    dotted_first_author = _set_dotted_initials(first_author, verbose)
    authors = dotted_first_author
    if dotted_first_author!=bp_pg.UNKNOWN and "et al." not in dotted_first_author:
        authors = f'{dotted_first_author} {et_al}'
    if verbose:
        print("\n    Authors search:", authors, authors_case, auth_idx_max)
    return authors, authors_case, auth_idx_max


def _found_ref_title(ref_items_list, search_title_params, verbose):
    authors_case, year, year_item_idx, doi, doi_item_idx, auth_idx_max = search_title_params
    mod_ref_items_list = [x.strip() + ", " for x in ref_items_list]
    idx_max = len(ref_items_list)-1
    title_item_idx, title =  0, bp_pg.UNKNOWN
    authors_exclude_cases = ['no_authors', 'first_item_too_long', 'partial_one_item']
    authors_test = all([authors_case not in authors_exclude_cases, auth_idx_max<idx_max])
    if verbose:
        print("        idx_max     :", idx_max)
        print("        auth_idx_max:", auth_idx_max)
        print("        authors_test:", authors_test)
    if authors_test:
        idx = auth_idx_max + 1
        if authors_case=='multiple_two_items_w_dot':
            while re.findall(bp_rg.RE_SCOPUS_REF_DOT, mod_ref_items_list[idx]):
                # Incrementing idx for presence of dot after 1 or 2 characters")
                idx += 1
                if idx>idx_max:
                    break
        if verbose:
            print("\n        Title idx    :", idx)
            print("        doi_item_idx :", doi_item_idx)
            print("        year_item_idx:", year_item_idx)
        if ((idx==doi_item_idx and doi!=bp_pg.UNKNOWN)
            or (year_item_idx==0 and year!=bp_pg.UNKNOWN)):
            # Incrementing idx for conflicts with doi or year indices")
            idx += 1
        if verbose:
            print("        Title idx    :", idx)
            print("        Title        :", title)
        title_item_idx, title = _try_list_idx(idx, idx, ref_items_list)
    if authors_case=='no_authors':
        title = ref_items_list[0]
    if title==f'({year})' and year!=bp_pg.UNKNOWN:
        # Incrementing idx for title equality with (dddd) as year-item
        idx = title_item_idx + 1
        title_item_idx, title = _try_list_idx(idx, idx, ref_items_list)
    if title==ref_items_list[doi_item_idx] and doi!=bp_pg.UNKNOWN:
        # Not keeping DOI as title
        title = bp_pg.UNKNOWN
    if verbose:
        print("\n    Ref title search:", title_item_idx, title)
    return title_item_idx, title


def _split_long_item_by_dot(item_txt):
    item_parts_list = item_txt.split(". ")
    item_txt_start = item_parts_list[0]
    item_txt_end = ". ".join(item_parts_list[1:])
    return item_txt_start, item_txt_end


def _select_journal_part(item_txt, colon, journal, verbose):
    journal_item_part = item_txt
    if colon:
        item_parts_list = item_txt.split(":")
        for part in item_parts_list:
            if re.findall(bp_rg.RE_SCOPUS_REF_JOURNAL, part):
                journal, journal_item_part = part, part
                break
    else:
        if " in " in item_txt:
            if verbose:
                print("    item_txt:", item_txt)
            txt = item_txt
            while txt:
                txt_parts = txt.split(" in ")
                txt_start, txt_end = txt_parts[0], txt_parts[-1]
                txt_start_search = re.findall(bp_rg.RE_SCOPUS_REF_JOURNAL, txt_start)
                txt_end_search = re.findall(bp_rg.RE_SCOPUS_REF_JOURNAL, txt_end)


                if verbose:
                    print("        txt:", txt)
                    print("        txt_parts:", txt_parts)
                    print("        txt_start:", txt_start)
                    print("        txt_start_search:", txt_start_search)
                    print("        txt_end:",txt_end)
                    print("        txt_end_search:", txt_end_search)
                txt_search = any([txt_start_search, txt_end_search])
                if verbose:
                    print("        txt_search:", txt_search)

                if len(txt_parts)==2 and txt_search:
                    if verbose:
                        print("        len=2 and txt_search true")
                    if txt_start_search:
                        journal, journal_item_part = txt, txt
                        txt = ''
                    elif txt_end_search:
                        journal, journal_item_part = f'in {txt_end}', txt_end
                        txt = ''
                elif len(txt_parts)>2:
                    if verbose:
                        print("        len >2")
                    txt = " in ".join(txt_parts[0:-1])
                    if txt_end_search:
                        journal, journal_item_part = f'in {txt_end}', txt_end
                        txt = ''
                else:
                    if verbose:
                        print("        else")
                    journal, journal_item_part = '', ''
                    txt = ''
        years_list = re.findall(bp_rg.RE_SCOPUS_REF_YEAR, item_txt)
        if years_list:
            journal_item_part = item_txt.split(years_list[0])[-1]
            journal = f'{years_list[0]}{journal_item_part}'
    return journal, journal_item_part


def _check_item_date(item_txt):
    item_date_status = False
    if item_txt!=bp_pg.UNKNOWN:
        item_dates_list = re.findall(bp_rg.RE_SCOPUS_REF_YEARS, item_txt)
        if item_dates_list:
            item_date = item_dates_list[0].strip()
            if item_date in item_txt:
                item_date_status = True
    return item_date_status


def _try_next_items(ref_items_list, init_item_idx, doi):
    item_idx_max = len(ref_items_list) - 1
    search_item_idx, search_item = init_item_idx, ref_items_list[init_item_idx]
    check_status = True
    while check_status and search_item_idx<item_idx_max:
        search_item_idx += 1
        search_item_idx, search_item = _try_list_idx(search_item_idx, search_item_idx, ref_items_list)
        check_status = any([_check_item_date(search_item), search_item==doi,
                            re.findall(bp_rg.RE_SCOPUS_REF_ONLY_DIGITS, search_item)])
    if check_status and search_item_idx==item_idx_max:
        search_item = bp_pg.UNKNOWN
    return search_item_idx, search_item


def _clean_journal_and_title(cleaning_params):
    (journal, title_item_idx, title, doi, journal_item_parts_list, ref_items_list) = cleaning_params
    if journal==title and title!=bp_pg.UNKNOWN:
        if journal_item_parts_list:
            journal = ' - '.join(journal_item_parts_list)
        title_item_idx, title = _try_next_items(ref_items_list, title_item_idx, doi)
    if journal==doi and doi!=bp_pg.UNKNOWN:
        journal = bp_pg.UNKNOWN
    if journal!=bp_pg.UNKNOWN:
        if journal in title:
            title = title.replace(journal, "")
            if not title:
                title = bp_pg.UNKNOWN
    else:
        if all("." in title_word for title_word in title.split(" ")):
            journal = title
            title = bp_pg.UNKNOWN
    clean_journal = journal.replace("(", "").replace(")", "")
    clean_title = title.replace("(", "").replace(")", "")
    return clean_journal, clean_title


def _find_ref_journal(ref_items_list, search_journal_params, verbose):
    authors_case, title_item_idx, title_item, doi, auth_idx_max = search_journal_params
    title = title_item
    idx_max = len(ref_items_list) - 1
    journal_item_idx, journal, journal_item_part = 0, bp_pg.UNKNOWN, ''
    journal_item_parts_list = []
    if authors_case in ['first_item_too_long', 'partial_one_item']:
        # First item too long as author names
        title_item_end = title_item
        if ". " in title_item:
            title, title_item_end = _split_long_item_by_dot(title_item)
        if re.findall(bp_rg.RE_SCOPUS_REF_JOURNAL, title_item):
            item_txt, colon = title_item, False
            if ":" in title_item_end:
                item_txt, colon = title_item_end, True
            journal, _ = _select_journal_part(item_txt, colon, journal, verbose)
        else:
            if len(ref_items_list)>1:
                second_item = ref_items_list[1]
                if re.findall(bp_rg.RE_SCOPUS_REF_JOURNAL, second_item):
                    journal, journal_item_idx = second_item, 1
    else:
        journal_item_idx_list, journals_list = [], []
        # First item not too long as author names
        idx_init = auth_idx_max
        if len(ref_items_list)>1:
            idx_init += 1
        search_items_list = ref_items_list[idx_init:]
        if verbose:
            print()
            print("    Journal search_items_list:", search_items_list)
        for search_idx, search_item in enumerate(search_items_list):
            item_idx = search_idx + idx_init
            _journal_item_idx, _journal, journal_item_part = item_idx, '', ''
            check_journal = all([re.findall(bp_rg.RE_SCOPUS_REF_JOURNAL, search_item),
                                 search_item!=doi, item_idx>=idx_init])
            if verbose:
                print()
                print("    search_item:", search_item)
                print("        re.findall(bp_rg.RE_SCOPUS_REF_JOURNAL, search_item):",
                      re.findall(bp_rg.RE_SCOPUS_REF_JOURNAL, search_item))
                print("        check_journal:", check_journal)
            if check_journal:
                colon, _journal_item_idx, _journal = False, item_idx, search_item.strip()
                _journal, journal_item_part = _select_journal_part(search_item, colon, _journal, verbose)

            check_dots = all([len(part)>2 and '.' in part for part in search_item.split(" ")]
                             + [search_item!=doi, item_idx>=idx_init])
            if verbose:
                print("        check_dots:", check_dots)
            if check_dots:
                _journal_item_idx, _journal, journal_item_part = item_idx, search_item.strip(), search_item

            if journal_item_part:
                journal_item_idx_list.append(_journal_item_idx)
                journals_list.append(_journal)
                journal_item_parts_list.append(journal_item_part)

        if journals_list:
            if verbose:
                print("\n    journals_list:", journals_list)
            journal_item_idx, journal = journal_item_idx_list[0], journals_list[0]
        else:
            # No results of journal search in all items
            init_item_idx = max(title_item_idx, auth_idx_max)
            if verbose:
                print("\n\n    init_item_idx:", init_item_idx)
            if init_item_idx<idx_max:
                journal_item_idx, journal = _try_next_items(ref_items_list, init_item_idx, doi)
                journal_item_parts_list = [journal]
    if verbose:
        print("\n    Journal search:", journal, title, journal_item_parts_list)
    cleaning_params = [journal, title_item_idx, title, doi, journal_item_parts_list, ref_items_list]
    clean_journal, clean_title = _clean_journal_and_title(cleaning_params)
    if verbose:
        print("\n    Journal and title clean:", journal_item_idx, clean_journal, clean_title)
    return journal_item_idx, clean_journal, clean_title


# Functions for cleaning raw reference
def _merge_ref_item(ref_item, ref_items_list, new_item_idx, ref_new_item):
    items_idx_max = len(ref_items_list) - 1
    item_end_part = ref_item.split(": ")[1]
    if not re.findall(bp_rg.RE_SCOPUS_REF_AND, item_end_part):
        and_found = False
        while not and_found and new_item_idx<items_idx_max:
            #  if ' and ' not found before searching ref_next_item
            ref_next_item = ref_items_list[new_item_idx + 1]
            ref_new_item += " " + ref_next_item
            new_item_idx += 1
            if re.findall(bp_rg.RE_SCOPUS_REF_AND, ref_next_item):
                #  if ' and ' found after searching ref_next_item
                and_found = True
    return new_item_idx, ref_new_item


def _check_merge_ref_items_colon(item_idx, ref_item, ref_items_list, raw_ref):
    items_idx_max = len(ref_items_list) - 1
    new_item_idx, ref_new_item = item_idx, ref_item
    if ": " in ref_item and item_idx<items_idx_max:
        next_item_idx = item_idx + 1
        next_item = ref_items_list[next_item_idx]
        if not re.findall(bp_rg.RE_SCOPUS_REF_YEAR, next_item):
            ref_end_part = ", ".join(raw_ref.split(", ")[next_item_idx:])
            if re.findall(bp_rg.RE_SCOPUS_REF_AND, ref_end_part):
                # 'and' in ref after ref_item
                new_item_idx, ref_new_item = _merge_ref_item(ref_item, ref_items_list,
                                                             new_item_idx, ref_new_item)
    return new_item_idx, ref_new_item


def _count_upper_len(txt):
    len_max = len(txt)
    upper_len, is_upper = 0, True
    while is_upper and upper_len<len_max:
        if txt[upper_len].isupper():
            upper_len += 1
        else:
            is_upper = False
    return upper_len


def _cases_check(txt):
    upper_len = _count_upper_len(txt)
    firstchar = txt[0]
    firstchar_is_lower = firstchar.islower()
    firstchar_is_upper = firstchar.isupper()
    nextchars_are_upper = all([firstchar_is_upper, upper_len>1,
                               not re.findall(bp_rg.RE_JOURNAL_ACRONYMS, txt)])
    return firstchar_is_lower, nextchars_are_upper


def _check_merge_ref_items_lowercase(item_idx, ref_item, ref_items_list):
    new_item_idx, ref_new_item = item_idx, ref_item
    items_idx_max = len(ref_items_list) - 1
    lowercase = True
    new_item_idx = item_idx
    while lowercase and new_item_idx<items_idx_max:
        ref_next_item = ref_items_list[new_item_idx + 1]
        firstchar_is_lower, nextchars_are_upper = _cases_check(ref_next_item)
        if firstchar_is_lower or nextchars_are_upper:
            ref_new_item += " " + ref_next_item
            new_item_idx += 1
        else:
            lowercase = False
    return new_item_idx, ref_new_item


def _drop_ref_items(value_regex, ref_items_list):
    new_ref_items_list = []
    for ref_item in ref_items_list:
        values_list = re.findall(value_regex, ref_item)
        if values_list:
            ref_item = ''
        new_ref_items_list.append(ref_item)
    ref_items_list = [x for x in new_ref_items_list if x]
    return ref_items_list


def _check_move_first_item(value_regex, ref_items_list):
    values_list = re.findall(value_regex, ref_items_list[0])
    if values_list:
        ref_items_list = ref_items_list[1:] + [ref_items_list[0]]
    return ref_items_list


def _drop_all_items_after(value_regex, ref_items_list):
    new_ref_items_list = []
    for item_idx, ref_item in enumerate(ref_items_list):
        values_list = re.findall(value_regex, ref_item)
        new_ref_items_list.append(ref_item)
        if values_list:
            break
    return new_ref_items_list


def _clean_ref(raw_ref):
    ref = raw_ref.replace(',” ', ', ').replace('”', '').replace(', 0,', ', ')
    init_ref_items_list = ref.split(", ")
    ref_items_list = init_ref_items_list

    # Cleaning first item
    ref_items_list = _check_move_first_item(bp_rg.RE_SCOPUS_REF_YEAR, ref_items_list)
    ref_items_list = _check_move_first_item(bp_rg.RE_SCOPUS_REF_DOI, ref_items_list)

    # Droping useless items
    ref_items_list = _drop_ref_items(bp_rg.RE_SCOPUS_REF_MONTHS_DROP, ref_items_list)
    ref_items_list = _drop_ref_items(bp_rg.RE_SCOPUS_REF_WORDS_DROP, ref_items_list)
    ref_items_list = _drop_ref_items(bp_rg.RE_SCOPUS_REF_PAGES, ref_items_list)
    ref_items_list = _drop_ref_items(bp_rg.RE_SCOPUS_REF_SYMB, ref_items_list)
    ref_items_list = _drop_ref_items(bp_rg.RE_SCOPUS_REF_WORDS_DROP, ref_items_list)
    ref_items_list = _drop_ref_items(bp_rg.RE_SCOPUS_REF_DIGITS_DROP, ref_items_list)

    # Merging items when colon is present
    items_nb = len(ref_items_list)
    new_ref_items_list = []
    item_idx = 0
    while item_idx<items_nb:
        ref_item = ref_items_list[item_idx]
        new_item_idx, ref_new_item = _check_merge_ref_items_colon(item_idx, ref_item,
                                                                   ref_items_list, raw_ref)
        new_ref_items_list.append(ref_new_item)
        item_idx = new_item_idx + 1
    mod_ref_items_list = [x for x in new_ref_items_list if x]

    # Merging items if items are not capitalized
    new_ref_items_list = mod_ref_items_list
    items_idx_max = len(mod_ref_items_list)-1
    if items_idx_max>0:
        new_ref_items_list = [mod_ref_items_list[0]]
        item_idx, ref_item = 1, mod_ref_items_list[1]
        new_item_idx, ref_new_item = _check_merge_ref_items_lowercase(item_idx, ref_item,
                                                                      mod_ref_items_list)
        new_ref_items_list.append(ref_new_item)
        if new_item_idx<items_idx_max:
            new_ref_items_list += mod_ref_items_list[new_item_idx + 1:]

    # Keeping only items up to DOI
    new_ref_items_list = _drop_all_items_after(bp_rg.RE_SCOPUS_REF_DOI, new_ref_items_list)

    clean_ref_items_list = [x for x in new_ref_items_list if x]
    new_ref = ', '.join(clean_ref_items_list)
    return new_ref


def _build_pub_refs_list(pub_id, ref_field, ref_cols_list, pub_verbose, verbose_ref_id):
    # Setting named tuple for the keeping the reference parsing results
    pub_ref_tup = namedtuple('pub_ref', ref_cols_list)

    pub_refs_list =[]
    if isinstance(ref_field, str):
        # If the reference field is not empty and not an URL
        raw_refs_list = [x for x in ref_field.split("; ") if x]
        for ref_idx, raw_ref in enumerate(raw_refs_list):
            ref_verbose = False
            year, authors, journal, doi, title = [bp_pg.UNKNOWN] * 5
            try:
                if pub_verbose:
                    print("\n\n\n\nREF INDEX     :", ref_idx)
                    print("raw_ref       :", raw_ref)
                    if ref_idx==verbose_ref_id:
                        ref_verbose = True
                ref = _clean_ref(raw_ref)
                ref_items_list = ref.split(", ")
                if ref_verbose:
                    print("ref           :", ref)
                    print("ref_items_list:", ref_items_list)

                doi_item_idx, doi = _find_ref_doi(ref_items_list, ref_verbose)
                year_item_idx, year = _find_ref_year(ref_items_list, doi_item_idx, doi, ref, ref_verbose)
                authors, authors_case, auth_idx_max = _find_ref_authors(ref_items_list, ref_verbose)
                search_title_params = [authors_case, year, year_item_idx, doi, doi_item_idx, auth_idx_max]
                title_item_idx, title_item = _found_ref_title(ref_items_list, search_title_params, ref_verbose)
                search_journal_params = [authors_case, title_item_idx, title_item, doi, auth_idx_max]
                _, journal, title = _find_ref_journal(ref_items_list, search_journal_params, ref_verbose)

            except IndexError:
                error_message = (f"\n\nWARNING: Index out of range for"
                                 f"\n    Pub_id       : {pub_id}"
                                 f"\n    Reference index: {ref_idx}"
                                 f"\n    Raw reference: {raw_ref}")
                print(error_message)

            except Exception as err:
                error_message = (f"\n\nWARNING: {err} for"
                                 f"\n    Pub_id       : {pub_id}"
                                 f"\n    Reference index: {ref_idx}"
                                 f"\n    Raw reference: {raw_ref}")
                print(error_message)
                raise

            finally:
                if authors==bp_pg.UNKNOWN:
                    authors = bp_pg.PARTIAL
                    if bp_pg.UNKNOWN not in (journal, title):
                        title = f'{title}, {journal}'
                        journal = bp_pg.UNKNOWN

                if ref_verbose:
                    print("\n\n    raw_ref       :", raw_ref)
                    print("    year          :", year)
                    print("    authors       :", authors)
                    print("    journal       :", journal)
                    print("    doi           :", doi)
                    print("    title         :", title)

                pub_refs_list.append(pub_ref_tup(pub_id, authors, year, journal, doi, title, raw_ref))
    return pub_refs_list


def build_scopus_references(corpus_df, cols_tup, verbose_pub_id=None, verbose_ref_id=None):
    """Builds the data of cited references per publication of the corpus.

    The structure of the built data is composed of 6 columns and one row per reference and per publication.
        Ex:
        Pub_id  Author            Year         Journal           DOI                Title             Full_reference
         0    Bellouard Q et al.  2017   Int. J. Hydrog. Energy 10.23919/...  Thermal management...  Bellouard Q,...
         0    Bellouard Q.        2020   Energy Fuels           unknown       Design and add...      Bellouard Q.,...

    Args:
        corpus_df (dataframe): The selected rawdata of the corpus.
        cols_tup (tup): Columns information as built through the `_set_scopus_parsing_cols` internal function.
        verbose_pub_id (int): Optional publication identifier selected for printing parsing information (default: None).
        verbose_ref_id (int): Optional identifier of the reference of the above publication selected for printing \
        detailed information of parsing steps (default: None).
    Returns:
        (dataframe): The built data.
    """
    # Setting useful column names
    cols_lists_dic, cols_dic, scopus_cols_dic = cols_tup
    ref_cols_list = cols_lists_dic['ref_cols_list']
    pub_id_col = cols_dic['pub_id_col']
    scopus_ref_col = scopus_cols_dic['scopus_ref_col']

    refs_list =[]
    for pub_id, ref_field in zip(list(corpus_df[pub_id_col]), corpus_df[scopus_ref_col]):
        pub_verbose = False
        if pub_id==verbose_pub_id:
            pub_verbose = True
            print("\n\npub_id:", pub_id)
        pub_refs_list = _build_pub_refs_list(pub_id, ref_field, ref_cols_list, pub_verbose, verbose_ref_id)
        refs_list += pub_refs_list
    references_df = pd.DataFrame.from_dict({label:[s[idx] for s in refs_list]
                                            for idx, label in enumerate(ref_cols_list)})
    return references_df
