#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This module is used to validate the curation process

"""

import os
import pandas as pd
import logging

from modules.file_indexer import file_indexer
from modules.answer_preparer import answer_preparer
from modules.curation_validator import curation_validator

import concurrent.futures as futures

class study_organizer(object):

    def __init__(self):
        self.study_answer_df = []

    def run_validation(self, dir_df, output_path, answer_df, uids_old_to_new, multiproc, multiproc_cpus, log_path, log_level):

        #-------------------------------------
        # Get list of modalities and loop
        #-------------------------------------
        
        validation_dfs = []
        
        studies = dir_df['study'].unique()

        study_count = len(studies)
        logging.debug(f'{str(study_count)} Studies to Process')

        if multiproc:
            workers = 60 if multiproc_cpus > 60 else multiproc_cpus if multiproc_cpus >= 1 else 1
            workers = os.cpu_count() if workers > os.cpu_count() else workers
            #sub_workers = ((workers - modality_count) / modality_count) // 1
            #sub_workers = 1 if int(sub_workers) == 0 else int(sub_workers)
            
            with futures.ProcessPoolExecutor(max_workers=workers) as executor:

                futures_list = []

                for study in studies:

                    logging.info(f'Study:{study} - Started')

                    study_df = dir_df[dir_df['study'] == study]
                    
                    futures_list.append(executor.submit(self.validation_runner, output_path, study_df, answer_df, uids_old_to_new, study, log_path, log_level))

                for future in futures.as_completed(futures_list):
                    result, study = future.result()
                    validation_dfs.append(result)

                    logging.info(f'Study:{study} - Complete')

        else:
            for study in studies:
                logging.info(f'Study:{study} - Started')

                study_df = dir_df[dir_df['study'] == study]
                #mod_answer_df = answer_df[answer_df['Modality'] == modality]

                result, study = self.validation_runner(output_path, study_df, answer_df, uids_old_to_new, study, log_path, log_level)
                validation_dfs.append(result)

                logging.info(f'Study:{study} - Complete')

        full_validation_df = pd.concat(validation_df for validation_df in validation_dfs)
        #full_validation_df.to_csv(os.path.join(self.output_path, "validation_results.csv"))

        return full_validation_df

    def validation_runner(self, output_path, data_df, answer_df, uids_old_to_new, study, log_path, log_level):

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

        multiproc = False
        multiproc_cpus = 1

        #-------------------------------------
        # Index files
        #-------------------------------------
        logging.info(f'Study:{study} - File Indexing Started')
        indexer = file_indexer()
        study_table_df = indexer.get_file_table(data_df, multiproc, multiproc_cpus, log_path, log_level)

        #pat_table_df.to_csv(os.path.join(self.output_path, "file_table_listing.csv"))
        logging.debug(f'Study:{study} - Files Indexed: {len(study_table_df)}')
        logging.info(f'Study:{study} - File Indexing Complete')

        #-------------------------------------
        # Prep Answer Data
        #-------------------------------------
        logging.info(f'Study:{study} - Answer Data Prep Started')
        if len(self.study_answer_df)==0:
            preparer = answer_preparer()
            study_answer_df = preparer.get_prepared_data(answer_df, uids_old_to_new, multiproc, multiproc_cpus, log_path, log_level)
            self.study_answer_df = study_answer_df
        logging.debug(f'Study:{study} - Answer Records: {len(self.study_answer_df)}')
        logging.info(f'Study:{study} - Answer Data Prep Complete')

        #-------------------------------------
        # Validate Data
        #-------------------------------------
        logging.info(f'Study:{study} - Validation Started')
        validator = curation_validator()
        study_validation_df = validator.get_validation_data(study_table_df, self.study_answer_df, multiproc, multiproc_cpus, log_path, log_level)
        logging.debug(f'Study:{study} - Validation Records: {len(study_validation_df)}')
        logging.info(f'Study:{study} - Validation Complete')

        return study_validation_df, study
