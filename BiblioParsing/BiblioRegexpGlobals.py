'''The BiblioRegexpGlobals module defines regexp as globals  
   used in other BiblioParsing modules.
   Created on 25/12/2023.

'''

__all__ = ['RE_ADDRESS',
           'RE_AWA',
           'RE_ADDS_JOURNAL',
           'RE_AUTHOR',
           'RE_DETECT_SCOPUS_NEW',
           'RE_NUM_CONF',
           'RE_REF_AUTHOR_SCOPUS',
           'RE_REF_AUTHOR_SCOPUS_NEW',
           'RE_REF_AUTHOR_WOS',
           'RE_REF_JOURNAL_SCOPUS',
           'RE_REF_JOURNAL_SCOPUS_NEW',
           'RE_REF_JOURNAL_WOS',
           'RE_REF_PAGE_SCOPUS',
           'RE_REF_PAGE_SCOPUS_NEW',
           'RE_REF_PAGE_WOS',
           'RE_REF_PROC_SCOPUS',
           'RE_REF_VOL_SCOPUS',
           'RE_REF_VOL_WOS',
           'RE_REF_YEAR_SCOPUS',
           'RE_REF_YEAR_WOS',
           'RE_SUB',
           'RE_SUB_FIRST',
           'RE_YEAR',
           'RE_YEAR_JOURNAL',
           'RE_ZIP_CODE',
          ]

##################
# Parsing regexp #
##################

# Standard library imports
import re

RE_ADDRESS = re.compile('''(?<=\]\s)                                             # Captures: "xxxxx" in string between "]" and "["
                        [^;]*                                                    # or  between "]" and end of string or ";"
                        (?=; | $ )''',re.X)

RE_ADDS_JOURNAL = re.compile(r'\([^\)]+\)')                                      # Captures string between "()" in journal name   (unused)

RE_AUTHOR = re.compile('''(?<=\[)
                      [a-zA-Z,;\s\.\-']*(?=, | \s )
                      [a-zA-Z,;\s\.\-']*
                      (?=\])''',re.X)                                            # Captures: "xxxx, xxx" or "xxxx xxx" in string between "[" and "]"

RE_NUM_CONF = re.compile(r'\s\d+th\s|\s\d+nd\s')                                 # Captures: " d...dth " or " d...dnd " in string

RE_DETECT_SCOPUS_NEW = re.compile("\(\d{4}\)(\s)?$")                             # find (dddd); at the end of a string

RE_REF_AUTHOR_SCOPUS = re.compile(r'^[^,0123456789:]*,'                          # Captures: "ccccc, ccccc,"
                                  '[^,0123456789:]*,')

RE_REF_AUTHOR_SCOPUS_NEW = re.compile(r'^[^,0123456789:]*,')                     # Captures: "ccccc," (since 07-2023)

RE_REF_AUTHOR_WOS = re.compile(r'^[^,0123456789:]*,')                            # Captures: "ccccc ccccc,"  ; To Do: to be converted to explicite list

RE_REF_JOURNAL_SCOPUS = re.compile('''\(\d{4}\)\s+[^,]*,                         # Capures "(dddd) cccccc," c not a comma
                                   |\(\d{4}\)\s+[^,]*$''',re.X)                  # or "(dddd) cccccc" at the end

RE_REF_JOURNAL_SCOPUS_NEW = re.compile('''(?<=,\s)[^,]*,\s+\d+,''')              # (since 07-2023)

RE_REF_JOURNAL_WOS = re.compile('''(?<=,)\s[A-Z]{2}[0-9A-Z&\s\-\.\[\]]+(?=,)     # Captures ", Science & Dev.[3],"
                                |(?<=,)\s[A-Z]{2}[0-9A-Z&\s\-\.\[\]]+$''',re.X)

RE_REF_PAGE_SCOPUS = re.compile(r'\s+[p]{1,2}\.\s+[a-zA-Z0-9]{1,9}')             # Captures: "pp. ddd" or "p. ddd"

RE_REF_PAGE_SCOPUS_NEW = re.compile(r'\s+[p]{1,2}\.\s+[a-zA-Z0-9]{1,9}'
                                     '-[a-zA-Z0-9]{1,9}')                        # Captures: "pp. ddd-ddd" (since 07-2023)
RE_REF_PAGE_WOS = re.compile(r',\s+P\d{1,6}')                                    # Captures: ", Pdddd"

RE_REF_PROC_SCOPUS = re.compile(r"[Pp]roceedings.*")                             # Captures alias of Proceedings surrounded by texts

RE_REF_VOL_SCOPUS = re.compile(''',\s+\d{1,6},                                   # Capture: ", dddd,"
                               |,\s+\d{1,6}\s\(                                  # or: ", dddd ("
                               |,\s+\d{1,6}$''',re.X)                            # or: ", dddd" at the string end

RE_REF_VOL_WOS = re.compile(r',\s+V\d{1,6}')                                     # Captures: ", Vdddd"

RE_REF_YEAR_SCOPUS = re.compile(r'(?<=\()\d{4}(?=\))')                           # Captures: "dddd" within parenthesis in scopus references

RE_REF_YEAR_WOS = re.compile(r',\s\d{4},')                                       # Captures: ", dddd," in wos references

RE_SUB = re.compile('''[a-z]?Univ[\.a-zé]{0,6}\s                                 # Captures alias of University surrounded by texts
                    |[a-z]?Univ[\.a-zé]{0,6}$''',re.X)

RE_SUB_FIRST = re.compile('''[a-z]?Univ[,]\s ''',re.X)                           # Captures alias of University before a coma

RE_YEAR = re.compile(r'\d{4}')                                                   # Captures "dddd" as the string giving the year

RE_YEAR_JOURNAL = re.compile(r'\s\d{4}\s')                                       # Captures " dddd " as the year in journal name

RE_ZIP_CODE = re.compile(',\s[a-zA-Z]?[\-]?\d+.*',)                              # Captures text begining with ', '
                                                                                 # and that possibly contains letters and hyphen-minus
RE_AWA = re.compile('\w+;,\s\w+|\w+;\w+')                                        # Captures ';, ' or ';' surrounded by letters
