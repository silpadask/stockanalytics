#!/usr/bin/env python3
# coding: utf-8
# packages
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import warnings
warnings.filterwarnings("ignore")
import os
import subprocess
import conf_reader
import sys

if sys.version_info[0] == 3:
    from importlib import abc
else:
    from importlib2 import abc

config = conf_reader.get_config()
binfolderpath = config.get("binfolder", None)
os.chdir(binfolderpath)

files=['conf_reader.py','package_installation.py','create_new_user.py','check_list_sql.py','load_data.py','bhav_download.py','index_download.py',\
'corporate_action_download.py','corporate_action_new.py','corporate_action_upload.py','bhav_upload.py','index_upload.py','corpact_clean.py',\
'adjust_price.py','get_average.py','get_pivotsmonthly.py','get_pivotsbiweekly.py','get_signals.py','get_symbols.py']

for execute in files:
	try:
		subprocess.call(['python3',execute])		
	except Exception as E:
		sys.exit(str(execute)+ " Errror occured " + str(E))
