#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import json
from datetime import datetime
import logging
import math
import pandas as pd
import subprocess
import re
from pydicom import dcmread
import concurrent.futures as futures

from modules.dciodvfy_runner import dciodvfy_runner

def initialize_logging(config, start_time):

    run_name = config['run_name']
    log_path = config['log_path']
    log_level = config['log_level']

    # If not exists, create
    if not os.path.exists(log_path):
        os.makedirs(log_path)

    str_date = start_time.strftime("%Y%m%d%H%M%S")
    log_file = os.path.join(log_path, f'{str_date}_{run_name}_dciodvfy.log')
    #log_file = f'{log_path}\\{str_date}_{run_name}_reports.log'

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

def main(argv):

    start_time = datetime.now()

    if len(argv) > 0:
        config_name = argv[0]

        #------------------------------------------
        # Load Config
        #------------------------------------------
        with open(config_name) as f:
            config = json.load(f)  

        #------------------------------------------
        # Initialize Logging
        #------------------------------------------
        log_path, log_level = initialize_logging(config, start_time)

        logging.info('Dciodvfy Run Started')

        #------------------------------------------
        # Run Validation
        #------------------------------------------

        runner = dciodvfy_runner(config, log_path, log_level)

        if os.name =='nt':
            software_path = 'software/dicom3tools_winexe_1.00.snapshot.20220618093127/dciodvfy'
        elif os.name =='mac':
            software_path = 'software/dicom3tools_macexe_1.00.snapshot.20220618093127/dciodvfy'
        else:
            #linux - need to run "sudo apt install dicom3tools"
            software_path = 'dciodvfy'

        data_path = config['input_data_path']
        results_path = os.path.join(config['output_data_path'], config['run_name'])
        multiproc = eval(config['multiprocessing'])
        multiproc_cpus = int(config['multiprocessing_cpus']) if 'multiprocessing_cpus' in config else 0

        runner.check_directory(software_path, data_path, results_path, multiproc, multiproc_cpus, log_path, log_level)

        #------------------------------------------
        # Calculate Duration
        #------------------------------------------
        end_time = datetime.now()
        elapsed_time = end_time - start_time
        seconds_in_day = 24 * 60 * 60
        duration = divmod(elapsed_time.days * seconds_in_day + elapsed_time.seconds, 60)

        logging.info(f'Dciodvfy Complete - Duration: {duration}')

    else:
        print('Please enter path to config file')

        return None

if __name__ == "__main__":

    main(sys.argv[1:])