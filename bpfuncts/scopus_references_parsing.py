"""Module of functions for parsing references fields of Scopus rawdata.
"""

__all__ = ['build_scopus_references',
          ]


# Standard library imports
import re
from collections import namedtuple

# 3rd party library imports
import pandas as pd

# Local libray imports
import bpfuncts.parsing_globals as bp_pg
import bpfuncts.regex_globals as bp_rg
from bpfuncts.parsing_utils import try_list_idx


# ********************************************
# * Internal functions for parsing reference *
# ********************************************

def _find_scopus_ref_doi(ref_items_list, verbose):
    """Searches for the DOI in the items resulting from the split by ', ' of a reference 
    using the regex 'RE_WOS_REF_DOI', a global imported from the `bpfuncts.regex_globals` module.

    Args:
        ref_items_list (list): The items (str) of the reference.
        verbose (bool): True for allowing control prints.
    Returns:
        (tup): Composed of index (int) of DOI item and of the DOI (str).
    """
    item_idx, dois_list = 0, []
    for item_idx, ref_item in enumerate(ref_items_list):
        dois_list = re.findall(bp_rg.RE_SCOPUS_REF_DOI, ref_item)
        if dois_list:
            break
    if verbose:
        print("\n\tDOI search:", item_idx, dois_list)
    doi_item_idx, doi = try_list_idx(item_idx, 0, dois_list)
    return doi_item_idx, doi


def _find_scopus_ref_year(ref_items_list, doi_item_idx, doi, ref, verbose):
    """Searches for the year in the items resulting from the split 
    by ', ' of a reference using several regexes imported from 
    the `bpfuncts.regex_globals` module.

    It also searches for year occurrence in DOI and conference information.

    Args:
        ref_items_list (list): The items (str) of the reference.
        doi_item_idx (int): The index of previously kept DOI item.
        doi (str): The previously kept DOI value.
        ref (str): The full reference.
        verbose (bool): True for allowing control prints.
    Returns:
        (tup): Composed of index (int) of year item and of the year value (str).
    """
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
                    # Dropping surrounding dots
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
        print("\n\tYear search:", item_idx, years_list)
    year_item_idx, year = try_list_idx(item_idx, 0, years_list)
    return year_item_idx, year


def _check_author_in_next_items(init_auth_idx_max, author_step, idx_max, ref_items_list, verbose):
    """Makes sure that all authors' names are found in order to set the maximum index 
    of items where authors' names are found.

    Args:
        init_auth_idx_max (int): The previously set maximum index of items \
        where authors' names are found.
        author_step (int): The increment of items' index (2 if author's name is \
        distributed on two items; 1 otherwise).
        idx_max (int): The maximum index in the items' list of the reference.
        ref_items_list (list): The items (str) of the reference.
        verbose (bool): True for allowing control prints.
    Returns:
        (int): The updated maximum index (int) of items where authors' names are found.
    """
    auth_idx_max = init_auth_idx_max + author_step
    if idx_max>init_auth_idx_max:
        idx = init_auth_idx_max + author_step
        while re.findall(bp_rg.RE_SCOPUS_REF_AUTHOR, ref_items_list[idx]):
            auth_idx_max = idx
            idx += author_step
            if verbose:
                print(f"\n\t\tidx\t: {idx}\n\t\tauth_idx_max 1: {auth_idx_max}")
            if idx>idx_max:
                break
    return auth_idx_max


def _check_authors_case_w_dot(authors_case_base, item, verbose):
    """Checks if the authors' item contains a dot to complete the case 
    of the authors' names.

    Args:
        authors_case_base (str): The base previously set for the case \
        of the authors' names.
        item (str): The authors' item to check.
        verbose (bool): True for allowing control prints.
    Returns:
        (str): The completed case of the authors' names.
    """
    authors_case = authors_case_base + '_w_dot'
    if '.' not in item:
        authors_case = authors_case_base + '_wo_dot'
    if verbose:
        print("\t\tauthors_case:", authors_case)
    return authors_case


def _set_one_item_authors(first_item, second_item, idx_max, ref_items_list, et_al, verbose):
    """Searches for authors' names in the case of each name found in a single item.

    The increment of items for author's search is set to 1. 
    The case of the authors' names is completed through the `_check_authors_case_w_dot` 
    internal function. 
    The function makes sure that all authors' names are found through the 
    `_check_author_in_next_items` internal function in order to set the maximum index 
    of items where authors' names are found.

    Args:
        first_item (str): The first item of the items' list of the reference.
        second_item (str): The second item of the items' list of the reference.
        idx_max (int): The maximum index in the items' list of the reference.
        ref_items_list (list): The items (str) of the reference.
        et_al (str): The previously set value to add to the fist author's name.
        verbose (bool): True for allowing control prints.
    Returns:
        (tup): Composed of the built first author's name (str), of the value \
        to add to the fist author's name, of the case (str) of the authors' names \
        and of the maximum index (int) of items where authors' names are found.
    """
    authors_case = 'single_one_item'
    author_step = 1
    first_author = first_item
    auth_idx_max = 0
    # Checking if multiple authors and computing max index of authors' items
    if verbose:
        print("\n\tRE_SCOPUS_REF_AUTHOR in second item 1:",
              re.findall(bp_rg.RE_SCOPUS_REF_AUTHOR, second_item))
    if re.findall(bp_rg.RE_SCOPUS_REF_AUTHOR, second_item):
        authors_case = _check_authors_case_w_dot('multiple_one_item', second_item, verbose)
        et_al = "et al."
        auth_idx_max = _check_author_in_next_items(auth_idx_max, author_step, idx_max,
                                                   ref_items_list, verbose)
    return first_author, et_al, authors_case, auth_idx_max


def _set_two_item_authors(first_item, second_item, idx_max, ref_items_list, et_al, verbose):
    """Searches for authors' names in the case of each name found in two separate items.

    The increment of items for author's search is set to 2. 
    The case of the authors' names is completed through the `_check_authors_case_w_dot` 
    internal function. 
    The function makes sure that all authors' names are found through the 
    `_check_author_in_next_items` internal function in order to set the maximum index 
    of items where authors' names are found.

    Args:
        first_item (str): The first item of the items' list of the reference.
        second_item (str): The second item of the items' list of the reference.
        idx_max (int): The maximum index in the items' list of the reference.
        ref_items_list (list): The items (str) of the reference.
        et_al (str): The previously set value to add to the fist author's name.
        verbose (bool): True for allowing control prints.
    Returns:
        (tup): Composed of the built first author's name (str), of the value \
        to add to the fist author's name, of the case (str) of the authors' names \
        and of the maximum index (int) of items where authors' names are found.
    """
    authors_case = 'single_two_items'
    author_step = 2
    first_author = f'{first_item} {second_item}'
    auth_idx_max = 1
    # Checking if multiple authors and computing max index of authors' items
    next_auth_idx_max = auth_idx_max + author_step
    if idx_max>next_auth_idx_max:
        fourth_item = ref_items_list[next_auth_idx_max]
        if verbose:
            print(f"\n\tRE_SCOPUS_REF_AUTHOR in {fourth_item}:",
                  re.findall(bp_rg.RE_SCOPUS_REF_AUTHOR, fourth_item))
        if re.findall(bp_rg.RE_SCOPUS_REF_AUTHOR, fourth_item):
            authors_case = _check_authors_case_w_dot('multiple_two_items', fourth_item, verbose)
            et_al = "et al."
            auth_idx_max = _check_author_in_next_items(auth_idx_max, author_step, idx_max,
                                                       ref_items_list, verbose)
    return first_author, et_al, authors_case, auth_idx_max


def _search_authors_in_items(ref_items_list, verbose):
    """Searches for possible author names and specifies the case of the authors' names.

    The search is performed only if the items resulting from the split  by ', ' 
    of a reference are not limited to one item. 
    The search is performed using the regex 'RE_SCOPUS_REF_AUTHOR' global 
    imported from the `bpfuncts.regex_globals` module. 
    If each author's name is distributed on two items (last name and first name 
    found in separate items), the increment of items for author's search is set to 2.
    Otherwise it is set to 1. 
    The case of the authors' names is completed through the `_check_authors_case_w_dot` 
    internal function. 
    The function makes sure that all authors' names are found through the 
    `_check_author_in_next_items` internal function in order to set the maximum index 
    of items where authors' names are found.

    Args:
        ref_items_list (list): The items (str) of the reference.
        verbose (bool): True for allowing control prints.
    Returns:
        (tup): Composed of the built first author's name (str), of the value \
        to add to the fist author's name, of the case (str) of the authors' names \
        and of the maximum index (int) of items where authors' names are found.
    """
    first_author, et_al, authors_case = bp_pg.UNKNOWN, "", 'no_authors'
    idx_max = len(ref_items_list) - 1
    auth_idx_max = 0
    first_item = ref_items_list[0]
    if verbose:
        print("\n\tRE_REF_AUTHOR_DROP in first item:",
              re.findall(bp_rg.RE_REF_AUTHOR_DROP, first_item))
    if not re.findall(bp_rg.RE_REF_AUTHOR_DROP, first_item):
        second_item = ref_items_list[1]
        if verbose:
            print("\n\tRE_SCOPUS_REF_AUTHOR in first item:",
                  re.findall(bp_rg.RE_SCOPUS_REF_AUTHOR, first_item))

        if re.findall(bp_rg.RE_SCOPUS_REF_AUTHOR, first_item):
            return_tup = _set_one_item_authors(first_item, second_item, idx_max,
                                               ref_items_list, et_al, verbose)
            first_author, et_al, authors_case, auth_idx_max = return_tup
        else:
            if verbose:
                print("\n\tRE_SCOPUS_REF_AUTHOR in second item 2:",
                      re.findall(bp_rg.RE_SCOPUS_REF_AUTHOR, second_item))
            if re.findall(bp_rg.RE_SCOPUS_REF_AUTHOR, second_item):
                return_tup = _set_two_item_authors(first_item, second_item, idx_max,
                                               ref_items_list, et_al, verbose)
                first_author, et_al, authors_case, auth_idx_max = return_tup
    if verbose:
        print("\n\tAuthors search in item:", first_author, et_al, authors_case, auth_idx_max)
    return first_author, et_al, authors_case, auth_idx_max


def _build_authors_attr(ref_items_list, verbose):
    """Search for the authors from the items resulting from the split 
    by ', ' of a reference using the regex 'RE_SCOPUS_REF_AUTHOR' global 
    imported from the `bpfuncts.regex_globals` module.

    If the first item is not too long, the search is performed 
    through the `_search_authors_in_items` internal function.

    Args:
        ref_items_list (list): The items (str) of the reference.
        verbose (bool): True for allowing control prints.
    Returns:
        (tup): Composed of the built first author's name (str), of the value \
        to add to the fist author's name, of the case (str) of the authors' names \
        and of the maximum index (int) of items where authors' names are found.
    """
    first_author, et_al, authors_case = bp_pg.UNKNOWN, "", 'no_authors'
    idx_max = len(ref_items_list) - 1
    author_max_len = 30
    auth_idx_max = 0
    first_item = ref_items_list[0]
    if re.findall(bp_rg.RE_SCOPUS_REF_EL_AL, first_item):
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
                first_author, et_al, authors_case, auth_idx_max = _search_authors_in_items(ref_items_list, verbose)
    if verbose:
        print("\n    Authors search in item:", first_author, et_al, authors_case, auth_idx_max)
    return first_author, et_al, authors_case, auth_idx_max


def _set_scopus_dotted_initials(first_author, verbose):
    """Modifies the first author's name in order to set dotted initials.

    Args:
        first_author (str): The author's name of the reference.
        verbose (bool): True for allowing control prints.
    Returns:
        (str): The modified first author's name.
    """
    dotted_first_author = first_author
    initial_dot = '.'
    if first_author!=bp_pg.UNKNOWN and initial_dot not in first_author:
        lastname = ' '.join(first_author.split(" ")[:-1])
        initials = first_author.split(" ")[-1]
        initials_list = [f'{x}{initial_dot}' for x in initials.split("-") if x]
        new_initials = '-'.join(initials_list)
        dotted_first_author = f'{lastname} {new_initials}'
    if verbose:
        print("\n\tDotted first author:", dotted_first_author)
    return dotted_first_author


def _find_scopus_ref_authors(ref_items_list, verbose):
    """Search for the authors from the items resulting 
    from the split by ', ' of a reference.

    First, it builds the first author and authors attributes through 
    the `_build_authors_attr` internal function. 
    Then, it sets the first author's name with dotted initials through
    the `_set_scopus_dotted_initials` internal function. 
    Finally, it adds to the first author's name the 'et_al' value 
    if it doesn't include 'et al.' string.

    Args:
        ref_items_list (list): The items (str) of the reference.
        verbose (bool): True for allowing control prints.
    Returns:
        (tup): Composed of the built authors (str), of the case (str) of \
        the authors' names and of the maximum index (int) of items \
        where authors' names are found.
    """
    first_author, et_al, authors_case, auth_idx_max = _build_authors_attr(ref_items_list,
                                                                          verbose)
    dotted_first_author = _set_scopus_dotted_initials(first_author, verbose)
    authors = dotted_first_author
    if dotted_first_author!=bp_pg.UNKNOWN and "et al." not in dotted_first_author:
        authors = f'{dotted_first_author} {et_al}'
    if verbose:
        print("\n    Authors search:", authors, authors_case, auth_idx_max)
    return authors, authors_case, auth_idx_max


def _find_scopus_ref_title(ref_items_list, search_title_params, verbose):
    """Searches for the title in the items resulting from the split by ', ' of a reference.

    The search takes into account the case of authors, the previously kept year, 
    the previously kept DOI and last author's item.

    Args:
        ref_items_list (list): The items (str) of the reference.
        search_title_params (list): Composed of the case of authors (str), \
        of the kept year value (str), of the index (int) of the kept-year item, \
        of the kept DOI value (str), of the index (int) of the kept-DOI item \
        and of the maximum index (int) of authors' items.
        verbose (bool): True for allowing control prints.
    Returns:
        (tup): Composed of the index (int) of the kept item as title \
        and the kept item (str) as title.
    """
    authors_case, year, year_item_idx, doi, doi_item_idx, auth_idx_max = search_title_params
    mod_ref_items_list = [x.strip() + ", " for x in ref_items_list]
    idx_max = len(ref_items_list)-1
    title_item_idx, title =  0, bp_pg.UNKNOWN
    authors_exclude_cases = ['no_authors', 'first_item_too_long', 'partial_one_item']
    authors_test = all([authors_case not in authors_exclude_cases, auth_idx_max<idx_max])
    if verbose:
        print(f"\t\tidx_max\t: {idx_max}\n\t\tauth_idx_max: {auth_idx_max}"
              f"\n\t\tauthors_test: {authors_test}")
    if authors_test:
        idx = auth_idx_max + 1
        if authors_case=='multiple_two_items_w_dot':
            while re.findall(bp_rg.RE_SCOPUS_REF_DOT, mod_ref_items_list[idx]):
                # Incrementing idx for presence of dot after 1 or 2 characters")
                idx += 1
                if idx>idx_max:
                    break
        if verbose:
            print(f"\n\t\tTitle idx\t: {idx}\n\t\tdoi_item_idx: {doi_item_idx}"
                  f"\n\t\tyear_item_idx: {year_item_idx}")
        if ((idx==doi_item_idx and doi!=bp_pg.UNKNOWN)
            or (year_item_idx==0 and year!=bp_pg.UNKNOWN)):
            # Incrementing idx for conflicts with doi or year indices")
            idx += 1
        if verbose:
            print(f"\n\t\tTitle idx\t: {idx}\n\t\tTitle: {title}")
        title_item_idx, title = try_list_idx(idx, idx, ref_items_list)
    if authors_case=='no_authors':
        title = ref_items_list[0]
    if title==f'({year})' and year!=bp_pg.UNKNOWN:
        # Incrementing idx for title equality with (dddd) as year-item
        idx = title_item_idx + 1
        title_item_idx, title = try_list_idx(idx, idx, ref_items_list)
    if title==ref_items_list[doi_item_idx] and doi!=bp_pg.UNKNOWN:
        # Not keeping DOI as title
        title = bp_pg.UNKNOWN
    if verbose:
        print("\n\tRef title search:", title_item_idx, title)
    return title_item_idx, title


def _split_long_item_by_dot(item_txt):
    """Builds the start part and the end part of an item after split by '. '.

    Args:
        item_txt (txt): The long item to split.
    Returns:
        (tup): Composed of the built item start (str) and of the built item end.
    """
    item_parts_list = item_txt.split(". ")
    item_txt_start = item_parts_list[0]
    item_txt_end = ". ".join(item_parts_list[1:])
    return item_txt_start, item_txt_end


def _select_journal_part(item_txt, colon, journal, verbose):
    """Selects the part of an item that provides journal information.

    The search of journal information is based on the regex 'RE_SCOPUS_REF_JOURNAL' 
    global imported from the `bpfuncts.regex_globals` module.

    Args:
        item_txt (txt): The item where journal information is searched.
        colon (bool): True if item should be split by ':' before journal search.
        journal (str): The initially selected journal information.
        verbose (bool): True for allowing control prints.
    Returns:
        (tup): Composed of the journal name (str) and of the part of the item \
        giving the journal information.
    """
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
                print("\titem_txt:", item_txt)
            txt = item_txt
            while txt:
                txt_parts = txt.split(" in ")
                txt_start, txt_end = txt_parts[0], txt_parts[-1]
                txt_start_search = re.findall(bp_rg.RE_SCOPUS_REF_JOURNAL, txt_start)
                txt_end_search = re.findall(bp_rg.RE_SCOPUS_REF_JOURNAL, txt_end)
                if verbose:
                    print(f"\t\ttxt: {txt}\n\t\ttxt_parts: {txt_parts}"
                          f"\n\t\ttxt_start: {txt_start}\n\t\ttxt_start_search: {txt_start_search}"
                          f"\n\t\ttxt_end: {txt_end}\n\t\ttxt_end_search: {txt_end_search}")
                txt_search = any([txt_start_search, txt_end_search])
                if verbose:
                    print("\t\ttxt_search:", txt_search)
                if len(txt_parts)==2 and txt_search:
                    if verbose:
                        print("\t\tlen=2 and txt_search true")
                    if txt_start_search:
                        journal, journal_item_part = txt, txt
                        txt = ''
                    elif txt_end_search:
                        journal, journal_item_part = f'in {txt_end}', txt_end
                        txt = ''
                elif len(txt_parts)>2:
                    if verbose:
                        print("\t\tlen >2")
                    txt = " in ".join(txt_parts[0:-1])
                    if txt_end_search:
                        journal, journal_item_part = f'in {txt_end}', txt_end
                        txt = ''
                else:
                    if verbose:
                        print("\t\telse")
                    journal, journal_item_part = '', ''
                    txt = ''
        years_list = re.findall(bp_rg.RE_SCOPUS_REF_YEAR, item_txt)
        if years_list:
            journal_item_part = item_txt.split(years_list[0])[-1]
            journal = f'{years_list[0]}{journal_item_part}'
    return journal, journal_item_part


def _check_item_date(item_txt):
    """Checks if an item contains a date value through the regex 
    'RE_SCOPUS_REF_YEARS' global imported from the `bpfuncts.regex_globals` module.

    Args:
        item_txt (txt): The item where date information is searched.
    Returns:
        (bool): The status of date value in the item.
    """
    item_date_status = False
    if item_txt!=bp_pg.UNKNOWN:
        item_dates_list = re.findall(bp_rg.RE_SCOPUS_REF_YEARS, item_txt)
        if item_dates_list:
            item_date = item_dates_list[0].strip()
            if item_date in item_txt:
                item_date_status = True
    return item_date_status


def _try_next_items(ref_items_list, init_item_idx, doi):
    """Checks the next items of an item and selects the first one that 
    doesn't contain date information, is not equal to the kept DOI 
    and not only composed of digits.

    The item for which next items are checked is set through 
    the `try_list_idx` function imported from the `bpfuncts.parsing_utils` module. 
    The check of date-information content is performed through 
    the `_check_item_date` internal function. 
    The check of only-digits item is performed using the regex 
    'RE_SCOPUS_REF_ONLY_DIGITS' global imported from 
    the `bpfuncts.regex_globals` module.

    It also searches for year occurrence in DOI and conference information.

    Args:
        ref_items_list (list): The items (str) of the reference.
        init_item_idx (int): The index of the item for which next items are checked.
        doi (str): The kept DOI value.
    Returns:
        (tup): Composed of index (int) of the kept next item and of the kept item (str).
    """
    item_idx_max = len(ref_items_list) - 1
    search_item_idx, search_item = init_item_idx, ref_items_list[init_item_idx]
    check_status = True
    while check_status and search_item_idx<item_idx_max:
        search_item_idx += 1
        search_item_idx, search_item = try_list_idx(search_item_idx, search_item_idx, ref_items_list)
        check_status = any([_check_item_date(search_item), search_item==doi,
                            re.findall(bp_rg.RE_SCOPUS_REF_ONLY_DIGITS, search_item)])
    if check_status and search_item_idx==item_idx_max:
        search_item = bp_pg.UNKNOWN
    return search_item_idx, search_item


def _clean_journal_and_title(cleaning_params):
    """Cleans the kept journal information and the kept title information.

    The cleaning takes into account the previously kept journal information, the previously kept title item, 
    the previously kept DOI and the parts of the item providing journal information. 
    These parameters are provided by the `_find_scopus_ref_journal` internal function.

    Args:
        cleaning_params (list): Composed of the previously kept journal information (str), \
        the index (int) of the previously kept-title item, the previously kept title, \
        The kept DOI (str), the parts of the item providing journal information \
        and the list of items resulting from the split by ', ' of a reference.
    Returns:
        (tup): Composed of the cleaned kept journal and of the cleaned kept title.
    """
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


def _find_scopus_ref_journal(ref_items_list, search_journal_params, verbose):
    """Searches for the journal in the items resulting from the split by ', ' of a reference.

    The search takes into account the case of authors, the previously kept title item, 
    the previously kept DOI and last author's item. 
    Depending on these parameters, several internal functions are used:
    `_split_long_item_by_dot`, `_select_journal_part` and `_try_next_items`.
    The kept journal and title are cleaned through the `_clean_journal_and_title` 
    internal function.

    Args:
        ref_items_list (list): The items (str) of the reference.
        search_journal_params (list): Composed of the case of authors (str), \
        of the index (int) of the previously kept-title item, \
        of the previously kept-title item (str), \
        of the kept DOI value (str), and of the maximum index (int) of authors' items.
        verbose (bool): True for allowing control prints.
    Returns:
        (tup): Composed of the index (int) of the kept item as journal information, \
        the kept clean journal (str) and the kept clean title (str).
    """
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
            print("\n\tJournal search_items_list:", search_items_list)
        for search_idx, search_item in enumerate(search_items_list):
            item_idx = search_idx + idx_init
            _journal_item_idx, _journal, journal_item_part = item_idx, '', ''
            check_journal = all([re.findall(bp_rg.RE_SCOPUS_REF_JOURNAL, search_item),
                                 search_item!=doi, item_idx>=idx_init])
            if verbose:
                print("\n\tsearch_item:", search_item)
                print("\t\tre.findall(bp_rg.RE_SCOPUS_REF_JOURNAL, search_item):",
                      re.findall(bp_rg.RE_SCOPUS_REF_JOURNAL, search_item))
                print("\t\tcheck_journal:", check_journal)
            if check_journal:
                colon, _journal_item_idx, _journal = False, item_idx, search_item.strip()
                _journal, journal_item_part = _select_journal_part(search_item, colon, _journal, verbose)

            check_dots = all([len(part)>2 and '.' in part for part in search_item.split(" ")]
                             + [search_item!=doi, item_idx>=idx_init])
            if verbose:
                print("\t\tcheck_dots:", check_dots)
            if check_dots:
                _journal_item_idx, _journal, journal_item_part = item_idx, search_item.strip(), search_item

            if journal_item_part:
                journal_item_idx_list.append(_journal_item_idx)
                journals_list.append(_journal)
                journal_item_parts_list.append(journal_item_part)

        if journals_list:
            if verbose:
                print("\n\tjournals_list:", journals_list)
            journal_item_idx, journal = journal_item_idx_list[0], journals_list[0]
        else:
            # No results of journal search in all items
            init_item_idx = max(title_item_idx, auth_idx_max)
            if verbose:
                print("\n\n\tinit_item_idx:", init_item_idx)
            if init_item_idx<idx_max:
                journal_item_idx, journal = _try_next_items(ref_items_list, init_item_idx, doi)
                journal_item_parts_list = [journal]
    if verbose:
        print("\n\tJournal search:", journal, title, journal_item_parts_list)
    cleaning_params = [journal, title_item_idx, title, doi, journal_item_parts_list, ref_items_list]
    clean_journal, clean_title = _clean_journal_and_title(cleaning_params)
    if verbose:
        print("\n\tJournal and title clean:", journal_item_idx, clean_journal, clean_title)
    return journal_item_idx, clean_journal, clean_title


# *************************************************
# * Internal functions for cleaning raw reference *
# *************************************************

def _merge_ref_item(ref_item, ref_items_list, new_item_idx, ref_new_item):
    """Merges an item with its next items until the item that contains ' and '.

    The item of which the merge id performed contains colon.

    Args:
        ref_item, ref_items_list, new_item_idx, ref_new_item
    Returns:
        (tup): Composed of the index (int) of the last item merged \
        and of the item resulting from the merge (str).
    """
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
    """Tries to an merge item, where colon is present, with its next items until ' and ' is found.

    When ' and ' is found in the ref part after the item of which the merge is checked, 
    the merge is performed through the `_merge_ref_item` internal function.

    Args:
       item_idx (int): The index of the item of which the merge is checked.
       ref_item (str): The item of which the merge is checked.
       ref_items_list (list): The items resulting from the split by ', ' of the reference.
       raw_ref (str): The reference before cleaning.
    Returns:
        (tup): Composed of the index (int) of the last item merged \
        and of the item resulting from the merge (str).
    """
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
    """Counts the number of characters in uppercase in a string 
    until a character in lower case is found or the end of 
    the string is reached.

    Args:
        txt (str): The string to check.
    Returns:
        (int): The number of consecutive characters in upper case.
    """
    len_max = len(txt)
    upper_len, is_upper = 0, True
    while is_upper and upper_len<len_max:
        if txt[upper_len].isupper():
            upper_len += 1
        else:
            is_upper = False
    return upper_len


def _cases_check(txt):
    """Sets the case status of the first character of a string.

    Also, it builds a complementary boolean which is True if the first character 
    is in uppercase, the number of consecutive characters in uppercase 
    is greater than 1 and no journal acronym is found in the string.

    Args:
        txt (str): The string to check.
    Returns:
        (tup): Composed of the lowercase status (bool) of the first character \
        and of the complementary built boolean.
    """
    upper_len = _count_upper_len(txt)
    firstchar = txt[0]
    firstchar_is_lower = firstchar.islower()
    firstchar_is_upper = firstchar.isupper()
    nextchars_are_upper = all([firstchar_is_upper, upper_len>1,
                               not re.findall(bp_rg.RE_JOURNAL_ACRONYMS, txt)])
    return firstchar_is_lower, nextchars_are_upper


def _check_merge_ref_items_lowercase(item_idx, ref_item, ref_items_list):
    """Tries to merge an item with next items until an item characters 
    in upper-case is found.

    The check of lower case or upper case of item is performed 
    through the `_cases_check` internal function.

    Args:
       item_idx (int): The index of the item of which the merge is checked.
       ref_item (str): The item of which the merge is checked.
       ref_items_list (list): The items resulting from the split \
       by ', ' of the reference.
    Returns:
        (tup): Composed of the index (int) of the last item merged \
        and of the item resulting from the merge (str).
    """
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
    """Drops items in the list of items resulting 
    from the split by ', ' of the reference.

    The items are dropped if values are found in this item
    for the search using the specified regex.

    Args:
        value_regex (regex): The specified regex.
        ref_items_list (list): The items resulting from \
        the split by ', ' of the reference.
    Returns:
        (list): The modified list of items (str).
    """
    new_ref_items_list = []
    for ref_item in ref_items_list:
        values_list = re.findall(value_regex, ref_item)
        if values_list:
            ref_item = ''
        new_ref_items_list.append(ref_item)
    ref_items_list = [x for x in new_ref_items_list if x]
    return ref_items_list


def _check_move_first_item(value_regex, ref_items_list):
    """Moves to the end of the list, the first item of the list 
    of items resulting from the split by ', ' of the reference.

    The items are moved if values are found in this item 
    for the search using the specified regex.

    Args:
        value_regex (regex): The specified regex.
        ref_items_list (list): The items resulting from \
        the split by ', ' of the reference.
    Returns:
        (list): The modified list of items (str).
    """
    ref_item_to_move = ref_items_list[0]
    values_list = re.findall(value_regex, ref_item_to_move)
    if values_list:
        ref_items_list = ref_items_list[1:] + [ref_item_to_move]
    return ref_items_list


def _drop_all_items_after(value_regex, ref_items_list):
    """Drops all items after an item in the list of items resulting 
    from the split by ', ' of the reference.

    The item after which all items are dropped is the one
    where values are found for the search using the specified regex.

    Args:
        value_regex (regex): The specified regex.
        ref_items_list (list): The items resulting from \
        the split by ', ' of the reference.
    Returns:
        (list): The modified list of items (str).
    """
    new_ref_items_list = []
    for ref_item in ref_items_list:
        values_list = re.findall(value_regex, ref_item)
        new_ref_items_list.append(ref_item)
        if values_list:
            break
    return new_ref_items_list


def _clean_ref(raw_ref):
    """Cleans a reference before parsing.

    The cleaning is, in particular, performed through several internal functions: 
    `_check_move_first_item`, `_drop_ref_items`, `_check_merge_ref_items_colon`, 
    `_check_merge_ref_items_lowercase` and `_drop_all_items_after`.

    Args:
        raw_ref (str): The reference as given by the references field \
        of a publication in corpus rawdata.
    Returns:
        (str): The cleaned reference.
    """
    ref = raw_ref.replace(',” ', ', ').replace('”', '').replace(', 0,', ', ')
    init_ref_items_list = ref.split(", ")
    ref_items_list = init_ref_items_list

    # Cleaning first item
    ref_items_list = _check_move_first_item(bp_rg.RE_SCOPUS_REF_YEAR, ref_items_list)
    ref_items_list = _check_move_first_item(bp_rg.RE_SCOPUS_REF_DOI, ref_items_list)

    # Dropping useless items
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


# *****************************************
# * Main functions for parsing references *
# *****************************************

def _build_scopus_pub_refs_list(pub_id, ref_field, ref_cols_list, pub_verbose, verbose_ref_id):
    """Builds the list of key items of the references of a publication as named-tuples.

    Args:
        pub_id (int): The publication ID.
        ref_field (str): The reference field giving the references of the publication.
        ref_cols_list (list): The column names to be used for the named-tuples.
        pub_verbose (bool): True allows control prints.
        verbose_ref_id (int): Identifier of the reference of the above publication selected for printing \
        detailed information of parsing steps.
    Returns:
        (list): The built named-tuples.
    """
    # Setting named tuple for keeping the reference parsing results
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
                    print(f"\n\n\n\nREF INDEX\t: {ref_idx}\nraw_ref\t\t: {raw_ref}")
                    if ref_idx==verbose_ref_id:
                        ref_verbose = True
                ref = _clean_ref(raw_ref)
                ref_items_list = ref.split(", ")
                if ref_verbose:
                    print(f"ref\t\t\t: {ref}\nref_items_list : {ref_items_list}")

                doi_item_idx, doi = _find_scopus_ref_doi(ref_items_list, ref_verbose)
                year_item_idx, year = _find_scopus_ref_year(ref_items_list, doi_item_idx, doi, ref, ref_verbose)
                authors, authors_case, auth_idx_max = _find_scopus_ref_authors(ref_items_list, ref_verbose)
                search_title_params = [authors_case, year, year_item_idx, doi, doi_item_idx, auth_idx_max]
                title_item_idx, title_item = _find_scopus_ref_title(ref_items_list, search_title_params, ref_verbose)
                search_journal_params = [authors_case, title_item_idx, title_item, doi, auth_idx_max]
                _, journal, title = _find_scopus_ref_journal(ref_items_list, search_journal_params, ref_verbose)

            except IndexError:
                print(f"\n\nWARNING: Index out of range for\n\tPub_id\t\t: {pub_id}"
                      f"\n\tReference index: {ref_idx}\n\tRaw reference: {raw_ref}")
            except Exception as err:
                print(f"\n\nWARNING: {err} for\n\tPub_id\t\t: {pub_id}"
                      f"\n\tReference index: {ref_idx}\n\tRaw reference: {raw_ref}")
                raise
            finally:
                if authors==bp_pg.UNKNOWN:
                    authors = bp_pg.PARTIAL
                    if bp_pg.UNKNOWN not in (journal, title):
                        title = f'{title}, {journal}'
                        journal = bp_pg.UNKNOWN
                if ref_verbose:
                    print(f"\n\n\traw_ref: {raw_ref}\n\tyear: {year}\n\tauthors: {authors}"
                          f"\n\tjournal: {journal}\n\tdoi: {doi}\n\ttitle: {title}")
                pub_refs_list.append(pub_ref_tup(pub_id, authors, year, journal, doi, title, raw_ref))
    return pub_refs_list


def build_scopus_references(corpus_df, cols_tup, verbose_pub_id=None, verbose_ref_id=None):
    """Builds the data of cited references per publication of the corpus.

    The structure of the built data is composed of 6 columns and one row per reference and per publication.
    Ex:
    Pub_id  Author           Year         Journal           DOI                Title             Full_reference.
    0    Bellouard Q et al.  2017   Int. J. Hydrog. Energy 10.23919/etc Thermal management etc  Bellouard Q, etc.
    0    Bellouard Q.        2020   Energy Fuels           unknown          Design and add etc  Bellouard Q., etc.

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
        pub_refs_list = _build_scopus_pub_refs_list(pub_id, ref_field, ref_cols_list,
                                                    pub_verbose, verbose_ref_id)
        refs_list += pub_refs_list
    references_df = pd.DataFrame.from_dict({label:[s[idx] for s in refs_list]
                                            for idx, label in enumerate(ref_cols_list)})
    return references_df
