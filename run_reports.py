#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import json
from datetime import datetime
import logging
import math
import pandas as pd

from modules.reports_helper import reports_helper

def initialize_logging(config, start_time):

    run_name = config['run_name']
    log_path = config['log_path']
    log_level = config['log_level']

    # If not exists, create
    if not os.path.exists(log_path):
        os.makedirs(log_path)

    str_date = start_time.strftime("%Y%m%d%H%M%S")
    log_file = os.path.join(log_path, f'{str_date}_{run_name}_reports.log')

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

        #config_name = "config_local_test.json"

        #------------------------------------------
        # Load Config
        #------------------------------------------
        with open(config_name) as f:
            config = json.load(f)  

        #------------------------------------------
        # Initialize Logging
        #------------------------------------------
        log_path, log_level = initialize_logging(config, start_time)

        logging.info('Report Generation Started')

        #------------------------------------------
        # Run Validation
        #------------------------------------------

        helper = reports_helper(config)
        helper.run_reports()

        #------------------------------------------
        # Calculate Duration
        #------------------------------------------
        end_time = datetime.now()
        elapsed_time = end_time - start_time
        seconds_in_day = 24 * 60 * 60
        duration = divmod(elapsed_time.days * seconds_in_day + elapsed_time.seconds, 60)

        logging.info(f'Reports Generation Complete - Duration: {duration}')

    else:
        print('Please enter path to config file')

        return None

if __name__ == "__main__":

    main(sys.argv[1:])
