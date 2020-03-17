#!/usr/bin/env python3
# coding: utf-8
# packages
import warnings
warnings.filterwarnings("ignore")
import os
import re 

used_properties_keys = ['inputdbprop', 'sql_old_username','sql_old_userpassword','sqltablename','inputstockdatapath','latestdatafromnsepath','olddatafromnsepath','requirements','binfolder']
def get_config():
    try:
        config_dict=dict()     
        with open('../etc/stockdata.conf', "rt") as f:
            for line in f:
                if not line.startswith('#'):
                    l = line.strip()
                    key_value = l.split('=')
                    key = key_value[0].strip()
                    key_value = l.replace(' ','').split(key+'=')
                    config_dict[key] = ' '.join(key_value[1:]).strip(' "')              
            config_dict = {k:(int(v) if v.isnumeric() else v ) for k,v in config_dict.items() if k in used_properties_keys}
            if 'inputdbprop' in config_dict and config_dict['inputdbprop']:
                config_dict['inputdbprop'] = eval(config_dict['inputdbprop'])
            
            f.close()
            return config_dict
    except Exception as e:
        print('Exception in readConfig as:: ', e)


if __name__ == '__main__':
    config = get_config()
    print(str(config)+' execute sucessfully...' )
    # print('config==', config)