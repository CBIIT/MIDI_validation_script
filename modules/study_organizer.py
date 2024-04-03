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
from tqdm import tqdm

class study_organizer(object):

    def run_validation(self, dir_df, output_path, answer_df, uids_old_to_new, uids_new_to_old, multiproc, multiproc_cpus, log_path, log_level):

        #-------------------------------------
        # Get list of studies and loop
        #-------------------------------------
        
        validation_dfs = []
        
        #studies = dir_df['study'].unique()
        
        study_counts = dir_df['study'].value_counts()
        studies = study_counts.index.tolist()
        
        logging.info(f'{len(study_counts)} Studies to Process')

        #multiproc=False

        if multiproc:
            workers = max(1, min(multiproc_cpus, os.cpu_count(), 60))
            
            with futures.ProcessPoolExecutor(max_workers=workers) as executor:

                futures_list = []

                for study in studies:

                    logging.debug(f'Study:{study} - Started')

                    study_df = dir_df[dir_df['study'] == study]
                    study_answer_df = answer_df[answer_df['StudyInstanceUID'] == uids_new_to_old[study]]
                    
                    futures_list.append(executor.submit(self.validation_runner, output_path, study_df, study_answer_df, uids_old_to_new, study, log_path, log_level))

                for future in tqdm(futures.as_completed(futures_list), total=len(futures_list), desc="Validating Studies"):
                    result, study = future.result()
                    validation_dfs.append(result)

                    logging.debug(f'Study:{study} - Complete')

        else:
            for study in tqdm(studies, desc="Validating Studies"):
                logging.debug(f'Study:{study} - Started')

                study_df = dir_df[dir_df['study'] == study]
                study_answer_df = answer_df[answer_df['StudyInstanceUID'] == uids_new_to_old[study]]

                result, study = self.validation_runner(output_path, study_df, study_answer_df, uids_old_to_new, study, log_path, log_level)
                validation_dfs.append(result)

                logging.debug(f'Study:{study} - Complete')

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
        logging.debug(f'Study:{study} - File Indexing Started')
        indexer = file_indexer()
        study_table_df = indexer.get_file_table(data_df, multiproc, multiproc_cpus, log_path, log_level)

        #pat_table_df.to_csv(os.path.join(self.output_path, "file_table_listing.csv"))
        logging.debug(f'Study:{study} - Files Indexed: {len(study_table_df)}')
        logging.debug(f'Study:{study} - File Indexing Complete')

        #-------------------------------------
        # Prep Answer Data
        #-------------------------------------
        logging.debug(f'Study:{study} - Answer Data Prep Started')
        preparer = answer_preparer()
        study_answer_df = preparer.get_prepared_data(answer_df, uids_old_to_new, multiproc, multiproc_cpus, log_path, log_level)
        logging.debug(f'Study:{study} - Answer Records: {len(study_answer_df)}')
        logging.debug(f'Study:{study} - Answer Data Prep Complete')

        #-------------------------------------
        # Validate Data
        #-------------------------------------
        logging.debug(f'Study:{study} - Validation Started')
        validator = curation_validator()
        study_validation_df = validator.get_validation_data(study_table_df, study_answer_df, multiproc, multiproc_cpus, log_path, log_level)
        logging.debug(f'Study:{study} - Validation Records: {len(study_validation_df)}')
        logging.debug(f'Study:{study} - Validation Complete')

        return study_validation_df, study
