#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This module is used to import the burn-in validation spreadsheet

"""

import os
import pandas as pd
import sqlite3 as sql
import logging


class import_helper(object):

    def __init__(self, config, log_path, log_level):

        logging.info('Initialization Started')

        run_name = config['run_name']
        input_data_path = config['input_data_path']
        output_data_path = config['output_data_path']
        answer_db_file = config['answer_db_file']
        uid_mapping_file = config['uid_mapping_file']
        multiproc = eval(config['multiprocessing'])
        multiproc_cpus = config['multiprocessing_cpus']

        # output path
        # ---------------------------
        self.output_path = os.path.join(output_data_path, run_name)

        # validation file
        # ---------------------------
        validation_path = os.path.join(self.output_path, "pixel_validation.xlsx")
        self.validation_df = pd.read_excel(validation_path, index_col=0)

        # validation db
        # ---------------------------
        validation_db_path = os.path.join(self.output_path, "validation_results.db")
        self.validation_db_conn = sql.connect(validation_db_path)

        # logging
        # ---------------------------
        self.log_path = log_path
        self.log_level = log_level

        logging.info('Initialization Complete')

    def run_import(self):

        #-------------------------------------
        # Import File
        #-------------------------------------
        logging.info('File Import Started')

        cursor = self.validation_db_conn.cursor()

        with self.validation_db_conn:
            for index, row in self.validation_df.iterrows():
                sql_query = """UPDATE validation_results 
                                  SET check_passed = @check_passed,  
                                      check_score = @check_score 
                                WHERE [index] = @index
                            """
                if pd.isna(row.check_passed):
                    sql_values = [row.check_passed, float(row.check_score), index]
                else:
                    sql_values = [int(row.check_passed), float(row.check_score), index]
                cursor.execute(sql_query, sql_values)

        #self.validation_db_conn.commit()

        logging.info(f'File Import Complete')


