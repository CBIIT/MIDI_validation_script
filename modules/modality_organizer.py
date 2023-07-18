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

class modality_organizer(object):

    def __init__(self):

        a = 'a'

    def run_validation(self, dir_df, output_path, answer_df, uids_old_to_new, multiproc, multiproc_cpus, log_path, log_level):

        #-------------------------------------
        # Get list of modalities and loop
        #-------------------------------------
        
        validation_dfs = []
        
        modalities = dir_df['modality'].unique()
        #modalities = ['CT']

        modality_count = len(modalities)
        logging.info(f'{str(modality_count)} Modalities to Process')

        if multiproc:
            workers = 60 if multiproc_cpus > 60 else multiproc_cpus if multiproc_cpus >= 1 else 1
            workers = os.cpu_count() if workers > os.cpu_count() else workers
            sub_workers = ((workers - modality_count) / modality_count) // 1
            sub_workers = 1 if int(sub_workers) == 0 else int(sub_workers)
            
            with futures.ProcessPoolExecutor(max_workers=workers) as executor:

                futures_list = []

                for modality in modalities:

                    logging.info(f'{modality} - Started')

                    mod_df = dir_df[dir_df['modality'] == modality]
                    mod_answer_df = answer_df[answer_df['Modality'] == modality]
                    
                    futures_list.append(executor.submit(self.validation_runner, output_path, mod_df, mod_answer_df, uids_old_to_new, multiproc, sub_workers, modality, log_path, log_level))

                for future in futures.as_completed(futures_list):
                    result, modality = future.result()
                    validation_dfs.append(result)

                    logging.info(f'{modality} - Complete')

        else:
            for modality in modalities:
                logging.info(f'{modality} - Started')

                mod_df = dir_df[dir_df['modality'] == modality]
                mod_answer_df = answer_df[answer_df['Modality'] == modality]

                result, modality = self.validation_runner(output_path, mod_df, mod_answer_df, uids_old_to_new, multiproc, 1, modality, log_path, log_level)
                validation_dfs.append(result)

                logging.info(f'{modality} - Complete')

        full_validation_df = pd.concat(validation_df for validation_df in validation_dfs)
        #full_validation_df.to_csv(os.path.join(self.output_path, "validation_results.csv"))

        return full_validation_df

    def validation_runner(self, output_path, data_df, answer_df, uids_old_to_new, multiproc, multiproc_cpus, modality, log_path, log_level):

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

        #-------------------------------------
        # Index files
        #-------------------------------------
        logging.info(f'{modality} - File Indexing Started')
        indexer = file_indexer()
        mod_table_df = indexer.get_file_table(data_df, multiproc, multiproc_cpus, log_path, log_level)
        #mod_table_df.to_csv(os.path.join(self.output_path, "file_table_listing.csv"))
        logging.info(f'{modality} - File Indexing Complete')

        #-------------------------------------
        # Prep Answer Data
        #-------------------------------------
        logging.info(f'{modality} - Answer Data Prep Started')
        preparer = answer_preparer()
        mod_answer_df = preparer.get_prepared_data(answer_df, uids_old_to_new, multiproc, multiproc_cpus, log_path, log_level)
        logging.info(f'{modality} - Answer Data Prep Complete')

        #-------------------------------------
        # Validate Data
        #-------------------------------------
        logging.info(f'{modality} - Validation Started')
        validator = curation_validator()
        mod_validation_df = validator.get_validation_data(mod_table_df, mod_answer_df, multiproc, multiproc_cpus, log_path, log_level)
        logging.info(f'{modality} - Validation Complete')

        return mod_validation_df, modality









