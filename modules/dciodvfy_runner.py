#!/usr/bin/env python
# -*- coding: utf-8 -*-

#Example using dciodvfy
# https://gist.github.com/fedorov/8ae49c844889d3525ec79a57b99f79c5

import sys
import os
import subprocess
import re
from pydicom import dcmread, errors as dcm_errors
import pandas as pd
import concurrent.futures as futures
import logging
from tqdm import tqdm


class dciodvfy_runner(object):

    def __init__(self, config, log_path, log_level):

        logging.info('Initialization Started')

        run_name = config['run_name']

    def index_path(self, path, index_type):

        #index_types
        # 1 - files
        # 2 - paths with files

        logging.info(f'Indexing Path: {path}')

        index_list = []

        if index_type == 1:
            for root, dirs, files in os.walk(path):
                for file in files:
                    index_list.append([root, [file]])
        elif index_type == 2:
            for root, dirs, files in os.walk(path):
                if files:
                    index_list.append([root, files])

        return index_list

    def check_file(self, root, file, software_path, log_path, log_level):

        # def initialize_logging(log_path, log_level):

        #     logging.basicConfig(
        #         level=log_level,
        #         format="%(asctime)s - [%(levelname)s] - %(message)s",
        #         handlers=[
        #             logging.FileHandler(log_path, 'a'),
        #             logging.StreamHandler()
        #         ]
        #     )

        # initialize_logging(log_path, log_level)

        errors = {}
        error_iter = 0

        file_path = os.path.join(root, file)

        #logging.info(f'Checking {file_path}')

        with open(file_path, 'rb') as dcm:
            #dataset = dcmread(dcm, force=True)
            try:
                dataset = dcmread(dcm, force=False)
            except dcm_errors.InvalidDicomError:
                dataset = None            

        if dataset:
            dcm_modality = dataset.Modality if 'Modality' in dataset else None
            dcm_class = dataset.SOPClassUID if 'SOPClassUID' in dataset else None
            dcm_patient = dataset.PatientID if 'PatientID' in dataset else None
            dcm_study = dataset.StudyInstanceUID if 'StudyInstanceUID' in dataset else None
            dcm_series = dataset.SeriesInstanceUID if 'SeriesInstanceUID' in dataset else None
            dcm_instance = dataset.SOPInstanceUID if 'SOPInstanceUID' in dataset else None
            dcm_file_name = f'<{file}>'
            dcm_file_path = f'<{file_path}>'
                
            proc = subprocess.Popen([software_path, '-new', file_path], stderr=subprocess.PIPE)
            allMessages = proc.communicate()[1].decode().split('\n')
            for message in allMessages:
                if re.search('^Error|Warning', message):
                    errors[error_iter] = {}

                    msg_type = re.search('^Error|Warning', message)
                    if msg_type:
                        errors[error_iter]['type'] = f'<{msg_type.group(0)}>'
                    else:
                        errors[error_iter]['type'] = message
                    msg_tag = re.search('(?<=\<)(.*?)(?=\>)', message)
                    if msg_tag:
                        errors[error_iter]['tag'] = f'<{msg_tag.group(0)}>'
                    else:
                        errors[error_iter]['tag'] = message
                    msg_msg = re.search('(?<=\> - ).*$', message)
                    if msg_msg:
                        errors[error_iter]['message'] = f'<{msg_msg.group(0)}>'
                    else:
                        errors[error_iter]['message'] = message
                    #errors[error_iter]['full'] = message
                    errors[error_iter]['modality'] = '<' + dcm_modality + '>'
                    errors[error_iter]['class'] = '<' + dcm_class + '>'
                    errors[error_iter]['patient'] = '<' + dcm_patient + '>'
                    errors[error_iter]['study'] = '<' + dcm_study + '>'
                    errors[error_iter]['series'] = '<' + dcm_series + '>'
                    errors[error_iter]['instance'] = '<' + dcm_instance + '>'
                    errors[error_iter]['file_name'] = dcm_file_name
                    errors[error_iter]['file_path'] = dcm_file_path
                    error_iter += 1

        return errors

    def check_directory_files(self, root, files, software_path, log_path, log_level):

        file_error_dicts = []

        with futures.ThreadPoolExecutor() as executor:

            futures_list = []

            for file in files:
                futures_list.append(executor.submit(self.check_file, root, file, software_path, log_path, log_level))

            for future in futures.as_completed(futures_list):
                error_dict = future.result()
                file_error_dicts.append(error_dict)

        return file_error_dicts

    def check_directory(self, software_path, data_path, results_path, multiproc, multiproc_cpus, log_path, log_level):

        directory_error_dicts = []

        file_list = self.index_path(data_path, 2)

        if multiproc:
            
            workers = 60 if multiproc_cpus > 60 else multiproc_cpus if multiproc_cpus >= 1 else 1

            with futures.ProcessPoolExecutor(max_workers=workers) as executor:

                futures_list = []

                for root, files in file_list:
                    futures_list.append(executor.submit(self.check_directory_files, root, files, software_path, log_path, log_level))

                for future in tqdm(futures.as_completed(futures_list), total=len(futures_list), desc="Checking Directories"):
                    error_dict = future.result()
                    directory_error_dicts.extend(error_dict)

        else:
            for root, files in tqdm(file_list, desc="Checking Directories"):
                error_dict = self.check_directory_files(root, files, software_path, log_path, log_level)
                directory_error_dicts.extend(error_dict)

        error_df = pd.concat((pd.DataFrame.from_dict(error_dict, 'index') for error_dict in directory_error_dicts))

        if not os.path.exists(results_path):
            os.makedirs(results_path)

        #writer = pd.ExcelWriter(os.path.join(results_path, 'dciodvfy_report.xlsx'))
        #error_df.to_excel(writer, 'Errors', index=False)
        #warning_df.to_excel(writer, 'Warnings', index=False)
        #writer.save()

        error_df.to_csv(os.path.join(results_path, 'dciodvfy_report.csv'), index=False)


        return None


