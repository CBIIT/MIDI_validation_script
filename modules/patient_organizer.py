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

class patient_organizer(object):

    #def __init__(self):

    def run_validation(self, dir_df, output_path, answer_df, uids_old_to_new, multiproc, multiproc_cpus, log_path, log_level):

        #-------------------------------------
        # Get list of modalities and loop
        #-------------------------------------
        
        validation_dfs = []
        
        patients = dir_df['patient'].unique()

        patient_count = len(patients)
        logging.debug(f'{str(patient_count)} Patients to Process')

        if multiproc:
            workers = 60 if multiproc_cpus > 60 else multiproc_cpus if multiproc_cpus >= 1 else 1
            workers = os.cpu_count() if workers > os.cpu_count() else workers
            #sub_workers = ((workers - modality_count) / modality_count) // 1
            #sub_workers = 1 if int(sub_workers) == 0 else int(sub_workers)
            
            with futures.ProcessPoolExecutor(max_workers=workers) as executor:

                futures_list = []

                for patient in patients:

                    logging.info(f'Patient:{patient} - Started')

                    pat_df = dir_df[dir_df['patient'] == patient]
                    #mod_answer_df = answer_df[answer_df['Modality'] == modality]
                    
                    futures_list.append(executor.submit(self.validation_runner, output_path, pat_df, answer_df, uids_old_to_new, patient, log_path, log_level))

                for future in futures.as_completed(futures_list):
                    result, patient = future.result()
                    validation_dfs.append(result)

                    logging.info(f'Patient:{patient} - Complete')

        else:
            for patient in patients:
                logging.info(f'Patient:{patient} - Started')

                pat_df = dir_df[dir_df['patient'] == patient]
                #mod_answer_df = answer_df[answer_df['Modality'] == modality]

                result, patient = self.validation_runner(output_path, pat_df, answer_df, uids_old_to_new, patient, log_path, log_level)
                validation_dfs.append(result)

                logging.info(f'Patient:{patient} - Complete')

        full_validation_df = pd.concat(validation_df for validation_df in validation_dfs)
        #full_validation_df.to_csv(os.path.join(self.output_path, "validation_results.csv"))

        return full_validation_df

    def validation_runner(self, output_path, data_df, answer_df, uids_old_to_new, patient, log_path, log_level):

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
        logging.info(f'Patient:{patient} - File Indexing Started')
        indexer = file_indexer()
        pat_table_df = indexer.get_file_table(data_df, multiproc, multiproc_cpus, log_path, log_level)
        #pat_table_df.to_csv(os.path.join(self.output_path, "file_table_listing.csv"))
        logging.debug(f'Patient:{patient} - Files Indexed: {len(pat_table_df)}')
        logging.info(f'Patient:{patient} - File Indexing Complete')

        #-------------------------------------
        # Prep Answer Data
        #-------------------------------------
        logging.info(f'Patient:{patient} - Answer Data Prep Started')
        preparer = answer_preparer()
        pat_answer_df = preparer.get_prepared_data(answer_df, uids_old_to_new, multiproc, multiproc_cpus, log_path, log_level)
        logging.debug(f'Patient:{patient} - Answer Records: {len(pat_answer_df)}')
        logging.info(f'Patient:{patient} - Answer Data Prep Complete')

        #-------------------------------------
        # Validate Data
        #-------------------------------------
        logging.info(f'Patient:{patient} - Validation Started')
        validator = curation_validator()
        pat_validation_df = validator.get_validation_data(pat_table_df, pat_answer_df, multiproc, multiproc_cpus, log_path, log_level)
        logging.debug(f'Patient:{patient} - Validation Records: {len(pat_validation_df)}')
        logging.info(f'Patient:{patient} - Validation Complete')

        return pat_validation_df, patient










