#!/usr/bin/env python3
# coding: utf-8
# packages
import warnings
warnings.filterwarnings("ignore")
import subprocess
import sys
import os
import conf_reader
import logging
import datetime as dt
from pkg_resources import WorkingSet , DistributionNotFound
from setuptools.command.easy_install import main as install
import subprocess

def create_logger():
    logger = logging.getLogger(__name__)
    if not os.path.exists('../logs'):
        os.makedirs('../logs')
    dt_str = str(dt.datetime.now()).replace(' ', '_' ).replace('-','').replace(':', '').split('.')[0]
    logging.basicConfig(filename='../logs/packageinstallation'+ dt_str+'.log', filemode='a', format='%(process)d  %(asctime)s %(levelname)s %(funcName)s %(lineno)d ::: %(message)s', level=logging.WARNING)
    return logger

def package_installation(logger):
	try:
		reqs = subprocess.check_output([sys.executable, '-m', 'pip', 'freeze'])	
	except:
		try:
			reqs = subprocess.check_output([sys.executable, '-m', 'pip3', 'freeze'])
		except:print("Please ensure that pip or pip3 is installed on your laptop and redo the setup")
	installed_packages = [r.decode().split('==')[0] for r in reqs.split()]

	config = conf_reader.get_config()
	requirements = config.get("requirements", None)

	print('packages execution started...')
	packages=[]

	try:
		with open(requirements, "rt") as f:
			for line in f:
				l = line.strip()
				package = l.split(',')
				package=package[0]
				packages.append(package)

		for i in packages:
			if i not in installed_packages:
				working_set = WorkingSet()
				try:
					dep = working_set.require('paramiko>=1.0')
				except DistributionNotFound:
					pass
				whoami=os.getlogin()
				if whoami =='root':
					install([i])
				if whoami !='root':
					try:
						subprocess.check_call(["pip", "install","--user", i])
					except:
						try:
							subprocess.check_call(["pip3", "install","--user", i])
						except:
							print("Check whether this user has admin privileges for installing package")

	
	except Exception as e:
		logger.exception('ERROR:: Some issue in reading the Config...check config_reader.py script in bin Folder....')
		raise e

def main():
    logger = create_logger()
    packageinstallation = package_installation(logger)
    logging.warning('package_installation == %s', packageinstallation)

if __name__ == "__main__":
        main()