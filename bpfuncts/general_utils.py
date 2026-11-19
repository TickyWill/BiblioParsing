"""Module of functions for general use.
"""

__all__ = ['dict_print',
           'print_final_text',
           'print_temp_text',
           'remove_special_symbol',
          ]


# Standard library imports
import functools
import unicodedata

# Local library imports
import bpfuncts.general_globals as bp_gg


def remove_special_symbol(text, only_ascii=True, strip=True):
    """The function `remove_special_symbol` removes accentuated characters in the string 'text'
    and ignore non-ascii characters if 'only_ascii' is true.

    Finally, spaces at the ends of 'text' are removed if strip is true.

    Args:
        text (str): The text where to remove special symbols.
        only_ascii (boolean): If True, non-ascii characters are removed from 'text' (default: True).
        strip (boolean): If True, spaces at the ends of 'text' are removed (default: True).
    Returns:
        (str): The modified string 'text'.
    """
    if only_ascii:
        nfc = functools.partial(unicodedata.normalize,'NFD')
        text = nfc(text). \
                   encode('ascii', 'ignore'). \
                   decode('utf-8')
    else:
        nfkd_form = unicodedata.normalize('NFKD',text)
        text = ''.join([c for c in nfkd_form if not unicodedata.combining(c)])

    if strip:
        text = text.strip()
    return text


def print_temp_text(txt):
    """Prints to console the text with cleaning at next print.

    Args:
        txt (str): The text to print.
    Returns:
        (int): Length of the printed text.
    """
    print(txt, end="\r")
    return len(txt)


def print_final_text(step_txt, prev_txt_len=None):
    """Prints to console the step text.

    If 'prev_txt' is set, it first clean the previous printed line to console.

    Args:
        step_txt (str)= The text to print.
        prev_txt_len (int): Optional length of the previously print text (default: None).
    """
    if prev_txt_len:
        print(" " * prev_txt_len, end="\r")
    print(step_txt)


def dict_print(dic):
    """Prints dict items line by line.

    Args:
        (dict): The data to print.
    """
    for k,v in dic.items():
        print(f"{bp_gg.TAB}{k}: {v}")
