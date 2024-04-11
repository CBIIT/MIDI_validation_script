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

class file_organizer(object):

    def run_validation(self, dir_df, output_path, answer_df, uids_old_to_new, uids_new_to_old, patids_old_to_new, multiproc, multiproc_cpus, log_path, log_level):

        #-------------------------------------
        # Get list of series and loop
        #-------------------------------------
        
        validation_dfs = []
        
        files = dir_df['instance'].unique()
        batch_size = len(files) // (multiproc_cpus * 10) + (1 if len(files) % multiproc_cpus > 0 else 0)
        file_batches = [files[i:i + batch_size] for i in range(0, len(files), batch_size)]        
        
        logging.info(f'{len(file_batches)} File Batches to Validate')

        #multiproc=False

        if multiproc:
            workers = max(1, min(multiproc_cpus, os.cpu_count(), 60))
            
            with futures.ProcessPoolExecutor(max_workers=workers) as executor:

                futures_list = []

                for batch in file_batches:

                    lookup_uids = [uids_new_to_old[instance] for instance in batch]

                    file_df = dir_df[dir_df['instance'].isin(batch)]
                    file_answer_df = answer_df[answer_df['SOPInstanceUID'].isin(lookup_uids)]
                    
                    futures_list.append(executor.submit(self.validation_runner, output_path, file_df, file_answer_df, uids_old_to_new, patids_old_to_new, log_path, log_level))

                for future in tqdm(futures.as_completed(futures_list), total=len(futures_list), desc="Validating File Batches"):
                    result = future.result()
                    validation_dfs.append(result)

        else:
            for batch in tqdm(file_batches, desc="Validating File Batches"):

                lookup_uids = [uids_new_to_old[instance] for instance in batch]
                
                file_df = dir_df[dir_df['instance'].isin(batch)]
                file_answer_df = answer_df[answer_df['SOPInstanceUID'].isin(lookup_uids)]

                result = self.validation_runner(output_path, file_df, file_answer_df, uids_old_to_new, patids_old_to_new, log_path, log_level)
                validation_dfs.append(result)

        full_validation_df = pd.concat(validation_df for validation_df in validation_dfs)
        #full_validation_df.to_csv(os.path.join(self.output_path, "validation_results.csv"))

        return full_validation_df

    def validation_runner(self, output_path, data_df, answer_df, uids_old_to_new, patids_old_to_new, log_path, log_level):

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
        indexer = file_indexer()
        file_table_df = indexer.get_file_table(data_df, multiproc, multiproc_cpus, log_path, log_level)

        #-------------------------------------
        # Prep Answer Data
        #-------------------------------------
        preparer = answer_preparer()
        file_answer_df = preparer.get_prepared_data(answer_df, uids_old_to_new, multiproc, multiproc_cpus, log_path, log_level)

        #-------------------------------------
        # Validate Data
        #-------------------------------------
        validator = curation_validator()
        file_validation_df = validator.get_validation_data(file_table_df, file_answer_df, uids_old_to_new, patids_old_to_new, multiproc, multiproc_cpus, log_path, log_level)

        return file_validation_df
