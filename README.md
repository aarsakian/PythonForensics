##vcardParser

Parses Vcard format files and exports to a xlsx and html file as well. 
Requires xlsxwriter, vobject, jinja2

##HashMatcher

This script searches recursively a tree of files and creates a list of their ed2k hashes. Then, it compares the list with the ed2k hashes of records of eMule Known.met. Known.met must be transformed to xls file, this can be carried out by MetViewer or Internet Evidence Finder. If xls is exported by IEF, please remove first column.

##KeywordSearcher

Use your Encase keyword file to search for matches in the filenames of known.MET records. The first matched keyword is displayed. Known.met must be transformed to xls file, this can be carried out by [MetViewer](http://www.gaijin.at/en/dlemmetview.php), or Internet Evidence Finder. Your keyword file must be saved in UTF8 encoding. This script tries its best to remove punctutatio characters, brackets etc.

##extractDB

Run this script to extract tables from an Microsoft SQL database. Usage python extractDB.py  "_WINDOWS SERVER NAME_"  "_Database Name_". External dependencies are **xlsxwriter** and **pyodbc**. There are additional arguments such as enabling threads.
