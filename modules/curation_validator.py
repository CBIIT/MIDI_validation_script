#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This module is used to validate the curation process

"""

import os
import pandas as pd
import numpy as np
import json
import logging
import string
import traceback
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords

import concurrent.futures as futures


class curation_validator(object):

    def __init__(self):
        
        self.stopwords = stopwords.words('english')
        self.punctuation = list(string.punctuation) + ['“','”','‘','’','``','•']      

    # ---------------------------------
    # Main functions
    # ---------------------------------

    def get_validation_data(self, file_data, answer_data, multiproc, multiproc_cpus, log_path, log_level):

        error_dicts = []

        cpu_count = os.cpu_count()

        if multiproc and len(file_data) > (multiproc_cpus * 100):           
            workers = 60 if multiproc_cpus > 60 else multiproc_cpus if multiproc_cpus >= 1 else 1
            file_lists = np.array_split(file_data, workers)
            
            with futures.ProcessPoolExecutor(max_workers=workers) as executor:

                futures_list = []

                for file_list in file_lists:
                    futures_list.append(executor.submit(self.validate_files, file_list, answer_data, log_path, log_level))

                for future in futures.as_completed(futures_list):
                    result = future.result()
                    error_dicts.append(result)

        else:
            result = self.validate_files(file_data, answer_data, log_path, log_level)
            error_dicts.append(result)

        #---------------------------

        error_df = pd.concat((pd.DataFrame.from_dict(error_dict, 'index') for error_dict in error_dicts))

        return error_df

    def validate_files(self, file_list, answer_data, log_path, log_level):

        def initialize_logging(log_path, log_level):

            logging.basicConfig(
                level=log_level,
                format="%(asctime)s - [%(levelname)s] - %(message)s",
                handlers=[
                    logging.FileHandler(log_path, 'a'),
                    logging.StreamHandler()
                ]
            )

        initialize_logging(log_path, log_level)

        error_dict = {}
        error_iter = 0

        for file_index, file_row in file_list.iterrows():

            try:
                #raise Exception("Testing Exception")

                if 'scope' in answer_data.columns:

                    file_answer_data = \
                        answer_data[((answer_data.scope == '<Instance>') & (answer_data.new_instance == file_row.instance)) | 
                                    ((answer_data.scope == '<Series>') & (answer_data.new_series == file_row.series)) | 
                                    ((answer_data.scope.isin(['<Study>','<Patient>'])) & (answer_data.new_study == file_row.study))]

                else:
                    file_answer_data = answer_data[(answer_data.new_instance == file_row.instance)]

                if not file_answer_data.empty:
                    answer_df = self.flatten_answer_data(file_answer_data)

                    # tag_retained
                    # --------------------------------------------------------------
                    tag_retain_check = answer_df[answer_df.action == '<tag_retained>']
                    error_iter, error_dict = self.validate_tag_retained(tag_retain_check, file_index, file_row, error_dict, error_iter)

                    # text_notnull
                    # --------------------------------------------------------------
                    text_notnull_check = answer_df[answer_df.action == '<text_notnull>']
                    error_iter, error_dict = self.validate_text_notnull(text_notnull_check, file_index, file_row, error_dict, error_iter)

                    # text_retained
                    # --------------------------------------------------------------
                    text_retained_check = answer_df[answer_df.action == '<text_retained>']
                    error_iter, error_dict = self.validate_text_retained(text_retained_check, file_index, file_row, error_dict, error_iter)

                    # text_removed
                    # --------------------------------------------------------------
                    text_removed_check = answer_df[answer_df.action == '<text_removed>']
                    error_iter, error_dict = self.validate_text_removed(text_removed_check, file_index, file_row, error_dict, error_iter)

                    # date_shifted
                    # --------------------------------------------------------------
                    date_shifted_check = answer_df[answer_df.action == '<date_shifted>']
                    error_iter, error_dict = self.validate_date_shifted(date_shifted_check, file_index, file_row, error_dict, error_iter)

                    # uid_changed
                    # --------------------------------------------------------------
                    uid_changed_check = answer_df[(answer_df.action == '<uid_changed>')]
                    error_iter, error_dict = self.validate_uid_changed(uid_changed_check, file_index, file_row, error_dict, error_iter)

                    # pixels_hidden
                    # --------------------------------------------------------------
                    pixels_hidden_check = answer_df[answer_df.action == '<pixels_hidden>']
                    error_iter, error_dict = self.validate_pixels_hidden(pixels_hidden_check, file_index, file_row, error_dict, error_iter)

            except:
                error = traceback.format_exc()
                logging.error(f'action: validate_files | file_path: {file_row.file_path} | instance: {file_row.instance} | tag: None \n{error}')
                a='a'

        return error_dict

    # ---------------------------------
    # Helper functions
    # ---------------------------------

    def flatten_answer_data(self, answer_data):

        answer_dicts = []

        for index, row in answer_data.iterrows():

            answer_dict = json.loads(row.AnswerData)
            answer_dicts.append(answer_dict)

        answer_df = pd.concat((pd.DataFrame.from_dict(answer_dict, 'index') for answer_dict in answer_dicts))

        return answer_df

    def log_error(self, error_dict, error_iter, file_index, file_row, check_index, check_row, file_value, passed, score):

        # log errors found in validation
        error_dict[error_iter] = {}
        error_dict[error_iter]['file_index'] = file_index
        error_dict[error_iter]['check_index'] = check_index
        error_dict[error_iter]['check_passed'] = passed
        error_dict[error_iter]['check_score'] = score
        error_dict[error_iter]['action'] = check_row.action
        error_dict[error_iter]['action_text'] = check_row.action_text
        error_dict[error_iter]['answer_category'] = f'<{str(check_row.answer_category)}>'

        error_dict[error_iter]['file_value'] = file_value
        error_dict[error_iter]['answer_value'] = check_row.value        
        
        error_dict[error_iter]['tag'] = check_row.tag
        error_dict[error_iter]['tag_ds'] = check_row.tag_ds
        #error_dict[error_iter]['tag_keyword'] = check_row.tag_keyword
        error_dict[error_iter]['tag_name'] = check_row.tag_name

        error_dict[error_iter]['modality'] = file_row.modality        
        error_dict[error_iter]['class'] = file_row['class']         
        error_dict[error_iter]['patient'] = file_row.patient
        error_dict[error_iter]['study'] = file_row.study
        error_dict[error_iter]['series'] = file_row.series
        error_dict[error_iter]['instance'] = file_row.instance
        error_dict[error_iter]['file_name'] = file_row.file_name
        error_dict[error_iter]['file_path'] = file_row.file_path

        error_iter+=1

        return error_iter, error_dict

    # ---------------------------------
    # Validation functions
    # ---------------------------------

    def validate_tag_retained(self, data_check, file_index, file_row, error_dict, error_iter):

        for check_index, check_row in data_check.iterrows():

            file_value = None
            check_pass = None
            check_score = None

            try:

                if check_row.tag_ds in file_row.keys():
                    file_value = file_row[check_row.tag_ds]
                    check_pass = True
                    check_score = 1
                else:
                    check_pass = False
                    check_score = 0

            except:
                error = traceback.format_exc()
                logging.error(f'action: tag_retained | file_path: {file_row.file_path} | instance: {file_row.instance} | tag: {check_row.tag_ds} \n{error}')
                a='a'

            error_iter, error_dict = self.log_error(error_dict, error_iter, file_index, file_row, check_index, check_row, file_value, check_pass, check_score)

        return error_iter, error_dict

    def validate_text_notnull(self, data_check, file_index, file_row, error_dict, error_iter):

        for check_index, check_row in data_check.iterrows():

            file_value = None
            check_pass = None
            check_score = None

            try:

                if check_row.tag_ds in file_row.keys():
                    file_value = file_row[check_row.tag_ds]
                    if file_value in ['<>']:
                        check_pass = False
                        check_score = 0
                    else:
                        check_pass = True
                        check_score = 1
                else:
                    check_pass = False
                    check_score = 0
                        
            except:
                error = traceback.format_exc()
                logging.error(f'action: text_notnull | file_path: {file_row.file_path} | instance: {file_row.instance} | tag: {check_row.tag_ds} \n{error}')
                a='a'

            error_iter, error_dict = self.log_error(error_dict, error_iter, file_index, file_row, check_index, check_row, file_value, check_pass, check_score)

        return error_iter, error_dict

    def validate_text_retained(self, data_check, file_index, file_row, error_dict, error_iter):

        for check_index, check_row in data_check.iterrows():

            file_value = None
            check_pass = None
            check_score = None

            try:
                if check_row.tag_ds in file_row.keys():
                    file_value = file_row[check_row.tag_ds] if not pd.isnull(file_row[check_row.tag_ds]) else None
                    if file_value:
                        check_value = check_row.action_text if not pd.isnull(check_row.action_text) else None
                        check_pass, check_score = self.validate_text(file_value, check_value, 'retain')
                    else:
                        check_pass = False
                        check_score = 0
                else:
                    check_pass = False
                    check_score = 0

            except:
                error = traceback.format_exc()
                logging.error(f'action: text_retained | file_path: {file_row.file_path} | instance: {file_row.instance} | tag: {check_row.tag_ds} \n{error}')
                a='a'

            error_iter, error_dict = self.log_error(error_dict, error_iter, file_index, file_row, check_index, check_row, file_value, check_pass, check_score)

        return error_iter, error_dict

    def validate_text_removed(self, data_check, file_index, file_row, error_dict, error_iter):

        for check_index, check_row in data_check.iterrows():
                
            file_value = None
            check_pass = None
            check_score = None

            try:

                if check_row.tag_ds in file_row.keys():
                    file_value = file_row[check_row.tag_ds] if not pd.isnull(file_row[check_row.tag_ds]) else None
                    file_value = '<>' if file_value == '<REMOVED>' else file_value
                    if file_value:
                        check_value = check_row.action_text if not pd.isnull(check_row.action_text) else None
                        check_pass, check_score = self.validate_text(file_value, check_value, 'remove')
                    else:
                        check_pass = True
                        check_score = 1
                else:
                    check_pass = True
                    check_score = 1

            except:
                error = traceback.format_exc()
                logging.error(f'action: text_removed | file_path: {file_row.file_path} | instance: {file_row.instance} | tag: {check_row.tag_ds} \n{error}')
                a='a'

            error_iter, error_dict = self.log_error(error_dict, error_iter, file_index, file_row, check_index, check_row, file_value, check_pass, check_score)

        return error_iter, error_dict

    def validate_date_shifted(self, data_check, file_index, file_row, error_dict, error_iter):

        for check_index, check_row in data_check.iterrows():

            file_value = None
            check_pass = None
            check_score = None

            try:

                if check_row.tag_ds in file_row.keys():

                    file_value = file_row[check_row.tag_ds] if not pd.isnull(file_row[check_row.tag_ds]) else ''
                    check_value = check_row.value.replace('<','').replace('>','')

                    if check_value.replace('\\','') in file_value.replace('\\',''):
                        check_pass = False
                        check_score = 0
                    else:
                        check_pass = True
                        check_score = 1
                else:
                    check_pass = True
                    check_score = 1

            except:
                error = traceback.format_exc()
                logging.error(f'action: date_shifted | file_path: {file_row.file_path} | instance: {file_row.instance} | tag: {check_row.tag_ds} \n{error}')
                a='a'

            error_iter, error_dict = self.log_error(error_dict, error_iter, file_index, file_row, check_index, check_row, file_value, check_pass, check_score)

        return error_iter, error_dict

    def validate_uid_changed(self, data_check, file_index, file_row, error_dict, error_iter):

        for check_index, check_row in data_check.iterrows():

            file_value = None
            check_pass = None
            check_score = None

            try:

                if check_row.tag_ds in file_row.keys():

                    file_value = file_row[check_row.tag_ds] if not pd.isnull(file_row[check_row.tag_ds]) else ''
                    check_value = check_row.value.replace('<','').replace('>','')

                    if check_value.replace('\\','') in file_value.replace('\\',''):
                        check_pass = False
                        check_score = 0
                    else:
                        check_pass = True
                        check_score = 1
                else:
                    check_pass = True
                    check_score = 1

            except:
                error = traceback.format_exc()
                logging.error(f'action: uid_changed | file_path: {file_row.file_path} | instance: {file_row.instance} | tag: {check_row.tag_ds} \n{error}')
                a='a'

            error_iter, error_dict = self.log_error(error_dict, error_iter, file_index, file_row, check_index, check_row, file_value, check_pass, check_score)

        return error_iter, error_dict

    def validate_pixels_hidden(self, data_check, file_index, file_row, error_dict, error_iter):

        for check_index, check_row in data_check.iterrows():

            try:                    
                error_iter, error_dict = self.log_error(error_dict, error_iter, file_index, file_row, check_index, check_row, None, None, None)

            except:
                error = traceback.format_exc()
                logging.error(f'action: pixels_hidden | file_path: {file_row.file_path} | instance: {file_row.instance} | tag: {check_row.tag_ds} \n{error}')

        return error_iter, error_dict

    def validate_text(self, file_value, answer_value, method):

        #-----------------------------
        # Test variables
        #-----------------------------
        
        #file_value = "<The patient's address is 1261 AR 72223>"
        #answer_value = '<1261 Leawood Street, Little Rock AR 72223>'

        #file_value = "<12>"
        #answer_value = "<12.0>"
        
        #-----------------------------
        check_pass = False
        check_score = 0

        file_value = file_value.replace('<','').replace('>','').lower()
        answer_value = answer_value.replace('<','').replace('>','').lower()

        #-----------------------------

        def validate_number(file_value, answer_value, method):

            file_value_fl = float(file_value)
            answer_value_fl = float(answer_value)

            retain = True if method == 'retain' else False

            if answer_value_fl == file_value_fl:
                check_pass, check_score = (True, 1.0) if retain else (False, 0.0)
            else:
                check_pass, check_score = (False, 0.0) if retain else (True, 1.0)

            return check_pass, check_score

        def tokenize_and_check(file_value, answer_value, method):

            retain = True if method == 'retain' else False

            answer_tokens = [token for token in word_tokenize(answer_value) if token not in (self.stopwords + self.punctuation)]

            total = len(answer_tokens)
            retained = 0
            removed = 0

            for token in answer_tokens:
                if token in file_value:
                    retained += 1
                else:
                    removed += 1

            if retain:
                check_pass = True if retained == total else False
                check_score = (retained / total)
            else:
                check_pass = True if removed == total else False
                check_score = (removed / total)

            return check_pass, check_score

        #-----------------------------

        if file_value.replace('.', '', 1).isdigit() and answer_value.replace('.', '', 1).isdigit():

            check_pass, check_score = validate_number(file_value, answer_value, method)

        else:           

            if str(answer_value) in file_value:
                if method == 'retain':
                    check_pass = True
                    check_score = 1.0
                elif method == 'remove':
                    check_pass = False
                    check_score = 0.0
            else:
                check_pass, check_score = tokenize_and_check(file_value, answer_value, method)

        return check_pass, check_score




