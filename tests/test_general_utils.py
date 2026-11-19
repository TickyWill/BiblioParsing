"""

"""

from bpfuncts.general_utils import *

def test_remove_special_symbol_only_ascii():
    assert remove_special_symbol("Kraków") == 'Krakow'


def test_remove_special_symbol_not_only_ascii():
    assert remove_special_symbol("Kraków", only_ascii=False) == 'Krakow'
