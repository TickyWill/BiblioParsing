# Application Release Backlog

## Release 3.1.0 - 2026/11/22
* **Summary**: This release corresponds to a deep refactoring of the code and to the upgrade of the python version from 3.9 to 3.14.
* **Features**:
  - 1: Deep refactoring based on rename of modules to fullfill the convention rules.
  - 2: Split of Scopus parsings and WoS parsings in several modules after code review of subjects and references parsing.
  - 3: Split of specific globals in several modules of globals.
  - 4: Upgrade of python-version from 3.9.7 to 3.14.
  - 5: Update of the requirements' files and complementary libraries of "nltk" package for python-version upgrade to 3.14.
  - 6: Update of the usage-demonstration notebook accordingly to the code refactoring.
* **Improvements**:
  - 1: Creation of unit-tests and related useful data-files.
  - 2: Reduction of complexity of several functions.
  - 3: Completion of all docstrings.
* **Bug Fixes**:
  - Bug 1: Inversion of file paths when calling "add_if" function of "bmfuncts.add_ifs" module in the "bmgui.update_if_page" module.
  - Bug 2: Partial update of IFs database with data of missing ISSNs and missing IFs set by the user fixed by creation of "\_clean_journals_data" and "\_clean_and_save_if_db" internal functions to "bmfuncts.update_impact_factors" module.
* **Known Issues**:
  - None.
* **API Changes**:
  - None.
* **Deprecated Features**:
  - None
* **Contributors**: Amal Chabli.
* **Acknowledgments**:
  - Thanks to Baptiste Refalo for the unit-tests creation and for the code reviews, which are essential for consolidation.
  - Thanks to François Bertin for continued insight-full advice and the initial development of the package.
  - Thanks to Iona Brenac for the code specifications and her real-world test during his master's program.

---
```
(Repeat the above structure for each release, with the most recent releases at the top of the file.)
```

