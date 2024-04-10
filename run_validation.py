#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import json
import multiprocessing
from datetime import datetime
import logging
import math
import pandas as pd
#import nltk
#nltk.download('stopwords', quiet=True)

from modules.validation_helper import validation_helper
import modules.nltk_modules
# import ipdb
# ipdb.set_trace(context=20)

def initialize_logging(config, start_time):

    run_name = config['run_name']
    log_path = config['log_path']
    log_level = config['log_level']

    # If not exists, create
    if not os.path.exists(log_path):
        os.makedirs(log_path)

    str_date = start_time.strftime("%Y%m%d%H%M%S")
    log_file = os.path.join(log_path, f'{str_date}_{run_name}_validation.log')
    #log_file = f'{log_path}\\{str_date}_{run_name}_validation.log'

    set_level = logging.INFO
    if log_level == 'debug':
        set_level = logging.DEBUG
    elif log_level == 'info':
        set_level = logging.INFO
    elif log_level == 'warning':
        set_level = logging.WARNING
    elif log_level == 'error':
        set_level = logging.ERROR
    elif log_level == 'critical':
        set_level = logging.CRITICAL

    logging.basicConfig(
        level=set_level,
        format="%(asctime)s - [%(levelname)s] - %(message)s",
        handlers=[
            logging.FileHandler(log_file, 'a'),
            logging.StreamHandler()
        ]
    )

    return log_file, set_level

def check_config(config_name):

    try:
        config_success = True
        with open(config_name) as f:
            config = json.load(f)  

        # run_name
        if 'run_name' in config:
            if config['run_name'].strip() == '':
                print('Config Error: run_name is blank')
                config_success = False
        else:
            print('Config Error: run_name not present')
            config_success = False

        # input_data_path
        if 'input_data_path' in config:
            if config['input_data_path'].strip() == '':
                print('Config Error: input_data_path is blank')
                config_success = False
            if not os.path.isdir(config['input_data_path']):
                print('Config Error: input_data_path does not exist')
                config_success = False
        else:
            print('Config Error: input_data_path not present')
            config_success = False

        # output_data_path
        if 'output_data_path' in config:
            if config['output_data_path'].strip() == '':
                print('Config Error: output_data_path is blank')
                config_success = False
        else:
            print('Config Error: output_data_path not present')
            config_success = False

        # answer_db_file
        if 'answer_db_file' in config:
            if config['answer_db_file'].strip() == '':
                print('Config Error: answer_db_file is blank')
                config_success = False
            if not os.path.isfile(config['answer_db_file']):
                print('Config Error: answer_db_file does not exist')
                config_success = False
        else:
            print('Config Error: answer_db_file not present')
            config_success = False

        # uid_mapping_file
        if 'uid_mapping_file' in config:
            if config['uid_mapping_file'].strip() == '':
                print('Config Error: uid_mapping_file is blank')
                config_success = False
            if not os.path.isfile(config['uid_mapping_file']):
                print('Config Error: uid_mapping_file does not exist')
                config_success = False
        else:
            print('Config Error: uid_mapping_file not present')
            config_success = False
            
        # patid_mapping_file
        if 'patid_mapping_file' in config:
            if config['patid_mapping_file'].strip() == '':
                print('Config Error: patid_mapping_file is blank')
                config_success = False
            if not os.path.isfile(config['patid_mapping_file']):
                print('Config Error: patid_mapping_file does not exist')
                config_success = False
        else:
            print('Config Error: patid_mapping_file not present')
            config_success = False

        # multiprocessing
        if 'multiprocessing' in config:
            if config['multiprocessing'].strip() == '':
                print('Config Error: multiprocessing is blank')
                config_success = False

            if eval(config['multiprocessing']):
                # multiprocessing_cpus
                if 'multiprocessing_cpus' in config:
                    if config['multiprocessing_cpus'].strip() == '':
                        print('Config Error: multiprocessing_cpus is blank')
                        config_success = False
                else:
                    print('Config Error: multiprocessing_cpus not present')
                    config_success = False
        else:
            print('Config Error: multiprocessing not present')
            config_success = False


        # log_path
        if 'log_path' in config:
            if config['log_path'].strip() == '':
                print('Config Error: log_path is blank')
                config_success = False
        else:
            print('Config Error: log_path not present')
            config_success = False

        # log_level
        if 'log_level' in config:
            if config['log_level'].strip() == '':
                print('Config Error: log_level is blank')
                config_success = False
            if config['log_level'] not in ["debug","info","warning","error","critical"]:
                print('Config Error: log_level is invalid. Valid values: ["debug","info","warning","error","critical"]')
                config_success = False
        else:
            print('Config Error: log_level not present')
            config_success = False
            
        # report_series
        if 'report_series' in config:
            if config['report_series'].strip() == '':
                print('Config Error: report_series is blank')
                config_success = False
        else:
            print('Config Error: report_series not present')
            config_success = False

    except Exception as e:
        config_success = False
        print(f'Error Confirming Config: {e}')

    return config, config_success

def main(argv):

    start_time = datetime.now()

    if len(argv) > 0:
        config_name = argv[0]

        #------------------------------------------
        # Check and Load Config
        #------------------------------------------
        config, config_success = check_config(config_name)

        if config_success:
            #------------------------------------------
            # Initialize Logging
            #------------------------------------------
            log_path, log_level = initialize_logging(config, start_time)

            logging.info('Run Started')

            #------------------------------------------
            # Run Validation
            #------------------------------------------

            try:
                helper = validation_helper(config, log_path, log_level)
                helper.run_validation()
            except Exception as e:
                logging.error('Error:', exc_info=e)

            #------------------------------------------
            # Calculate Duration
            #------------------------------------------
            end_time = datetime.now()
            elapsed_time = end_time - start_time
            seconds_in_day = 24 * 60 * 60
            duration = divmod(elapsed_time.days * seconds_in_day + elapsed_time.seconds, 60)

            logging.info(f'Run Complete - Duration: {duration}')

    else:
        print('Please enter path to config file')

        return None


if __name__ == "__main__":

    multiprocessing.set_start_method("spawn", True)

    main(sys.argv[1:])
