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

class series_organizer(object):

    def run_validation(self, dir_df, output_path, answer_df, uids_old_to_new, uids_new_to_old, multiproc, multiproc_cpus, log_path, log_level):

        #-------------------------------------
        # Get list of series and loop
        #-------------------------------------
        
        validation_dfs = []
        
        series_counts = dir_df['series'].value_counts()
        series = series_counts.index.tolist()
        
        logging.info(f'{len(series_counts)} Series to Process')

        #multiproc=False

        if multiproc:
            workers = max(1, min(multiproc_cpus, os.cpu_count(), 60))
            
            with futures.ProcessPoolExecutor(max_workers=workers) as executor:

                futures_list = []

                for ser in series:

                    logging.debug(f'Series:{ser} - Started')

                    series_df = dir_df[dir_df['series'] == ser]
                    series_answer_df = answer_df[answer_df['SeriesInstanceUID'] == uids_new_to_old[ser]]
                    
                    futures_list.append(executor.submit(self.validation_runner, output_path, series_df, series_answer_df, uids_old_to_new, ser, log_path, log_level))

                for future in tqdm(futures.as_completed(futures_list), total=len(futures_list), desc="Validating Series"):
                    result, ser = future.result()
                    validation_dfs.append(result)

                    logging.debug(f'Series:{ser} - Complete')

        else:
            for ser in tqdm(series, desc="Validating Series"):
                logging.debug(f'Series:{series} - Started')

                series_df = dir_df[dir_df['series'] == series]
                series_answer_df = answer_df[answer_df['SeriesInstanceUID'] == uids_new_to_old[series]]

                result, series = self.validation_runner(output_path, series_df, series_answer_df, uids_old_to_new, series, log_path, log_level)
                validation_dfs.append(result)

                logging.debug(f'Series:{series} - Complete')

        full_validation_df = pd.concat(validation_df for validation_df in validation_dfs)
        #full_validation_df.to_csv(os.path.join(self.output_path, "validation_results.csv"))

        return full_validation_df

    def validation_runner(self, output_path, data_df, answer_df, uids_old_to_new, series, log_path, log_level):

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
        logging.debug(f'Series:{series} - File Indexing Started')
        indexer = file_indexer()
        series_table_df = indexer.get_file_table(data_df, multiproc, multiproc_cpus, log_path, log_level)

        #pat_table_df.to_csv(os.path.join(self.output_path, "file_table_listing.csv"))
        logging.debug(f'Series:{series} - Files Indexed: {len(series_table_df)}')
        logging.debug(f'Series:{series} - File Indexing Complete')

        #-------------------------------------
        # Prep Answer Data
        #-------------------------------------
        logging.debug(f'Series:{series} - Answer Data Prep Started')
        preparer = answer_preparer()
        series_answer_df = preparer.get_prepared_data(answer_df, uids_old_to_new, multiproc, multiproc_cpus, log_path, log_level)
        logging.debug(f'Series:{series} - Answer Records: {len(series_answer_df)}')
        logging.debug(f'Series:{series} - Answer Data Prep Complete')

        #-------------------------------------
        # Validate Data
        #-------------------------------------
        logging.debug(f'Series:{series} - Validation Started')
        validator = curation_validator()
        series_validation_df = validator.get_validation_data(series_table_df, series_answer_df, multiproc, multiproc_cpus, log_path, log_level)
        logging.debug(f'Series:{series} - Validation Records: {len(series_validation_df)}')
        logging.debug(f'Series:{series} - Validation Complete')

        return series_validation_df, series
