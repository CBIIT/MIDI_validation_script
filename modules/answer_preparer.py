#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This module is used to prepare the answer data for validation

"""

import os
import pandas as pd
import numpy as np
import logging

import concurrent.futures as futures

class answer_preparer(object):

    #def __init__(self):

    def get_prepared_data(self, answer_data, uids_old_to_new, multiproc, multiproc_cpus, log_path, log_level):

        return_files = []

        cpu_count = os.cpu_count()

        if multiproc and len(answer_data) > (multiproc_cpus * 100):
            workers = max(1, min(multiproc_cpus, os.cpu_count(), 60))
            
            answer_files = np.array_split(answer_data, workers)

            with futures.ProcessPoolExecutor(max_workers=workers) as executor:

                futures_list = []

                for answer_file in answer_files:
                    futures_list.append(executor.submit(self.prepare_answer_data, answer_file, uids_old_to_new, log_path, log_level))

                for future in futures.as_completed(futures_list):
                    result = future.result()
                    return_files.append(result)

        else:
            result = self.prepare_answer_data(answer_data, uids_old_to_new, log_path, log_level)
            return_files.append(result)

        return_df = pd.concat(return_file for return_file in return_files)

        return return_df

    def prepare_answer_data(self, answer_data, uids_old_to_new, log_path, log_level):

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

        converted_data = self.convert_ids(answer_data, uids_old_to_new)

        return converted_data

    def convert_ids(self, answer_data, uids_old_to_new):

        return_data = answer_data.copy()

        return_data['new_study'] = ''
        return_data['new_series'] = ''
        return_data['new_instance'] = ''

        for index, row in return_data.iterrows():

            study_uid = f'<{row.StudyInstanceUID}>'
            if study_uid in uids_old_to_new.keys():
                return_data.at[index,'new_study'] = uids_old_to_new.get(study_uid) if study_uid else ''
            
            series_uid = f'<{row.SeriesInstanceUID}>'
            if series_uid in uids_old_to_new.keys():
                return_data.at[index,'new_series'] = uids_old_to_new.get(series_uid) if series_uid else ''

            instance_uid = f'<{row.SOPInstanceUID}>'
            if instance_uid in uids_old_to_new.keys():
                return_data.at[index,'new_instance'] = uids_old_to_new.get(instance_uid) if instance_uid else ''

        return return_data




