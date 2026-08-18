"""Module of functions for general use.
"""

__all__ = ['remove_special_symbol',
          ]


# Standard library imports
import functools
import unicodedata


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
