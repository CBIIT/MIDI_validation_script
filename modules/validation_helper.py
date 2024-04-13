#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This module is used to validate the curation process

"""

import os
import pandas as pd
import sqlite3 as sql
import logging
import shutil
import json
from tqdm import tqdm
import time


from modules.directory_indexer import directory_indexer
#from modules.modality_organizer import modality_organizer
#from modules.patient_organizer import patient_organizer
#from modules.study_organizer import study_organizer
#from modules.series_organizer import series_organizer
from modules.file_organizer import file_organizer

class validation_helper(object):

    def __init__(self, config, log_path, log_level):

        logging.info('Initialization Started')

        run_name = config['run_name']
        input_data_path = config['input_data_path']
        output_data_path = config['output_data_path']
        answer_db_file = config['answer_db_file']
        uid_mapping_file = config['uid_mapping_file']
        patid_mapping_file = config['patid_mapping_file']
        multiproc = eval(config['multiprocessing'])
        multiproc_cpus = config['multiprocessing_cpus'] if 'multiprocessing_cpus' in config else 0

        # input_path
        # ---------------------------
        self.input_path = input_data_path

        # output path
        # ---------------------------
        self.output_path = os.path.join(output_data_path, run_name)

        # If not exists, create
        if not os.path.exists(self.output_path):
            os.makedirs(self.output_path)
        else:
            shutil.rmtree(self.output_path)
            os.makedirs(self.output_path)
            

        # validation db
        # ---------------------------
        self.validation_db_conn = sql.connect(os.path.join(self.output_path, "validation_results.db"))

        logging.info('Validation Result DB Created')

        # answer data
        # ---------------------------
        answer_db_conn = sql.connect(answer_db_file)
        answer_query = "SELECT * FROM answer_data"
        self.answer_df = pd.read_sql(answer_query, answer_db_conn)
        answer_db_conn.close()

        logging.info(f'Answer Key Imported: {len(self.answer_df)} Records')

        # uid mapping
        # ---------------------------
        self.uids_old_to_new = {}
        uid_file = pd.read_csv(uid_mapping_file, na_values=[], keep_default_na=False, converters={'id_old':str,'id_new':str})
        uid_file.drop_duplicates(subset='id_old', keep='first', inplace=True)
        self.uids_new_to_old = uid_file[['id_new','id_old']].set_index('id_new')['id_old'].to_dict()
        uid_file = uid_file.applymap(lambda x: f'<{x}>')
        self.uids_old_to_new = uid_file[['id_old','id_new']].set_index('id_old')['id_new'].to_dict()

        logging.info(f'UID Mapping Imported: {len(self.uids_old_to_new)} Records')
        
        # patid mapping
        # ---------------------------
        self.patids_old_to_new = {}
        patid_file = pd.read_csv(patid_mapping_file, na_values=[], keep_default_na=False, converters={'id_old':str,'id_new':str})
        patid_file.drop_duplicates(subset='id_old', keep='first', inplace=True)
        patid_file = patid_file.applymap(lambda x: f'<{x}>')
        self.patids_old_to_new = patid_file[['id_old','id_new']].set_index('id_old')['id_new'].to_dict()

        logging.info(f'PatID Mapping Imported: {len(self.patids_old_to_new)} Records')

        # multiprocessing
        # ---------------------------
        # self.multiproc = multiproc in ['True','true','1']
        self.multiproc = multiproc 
        self.multiproc_cpus = 0 if multiproc_cpus == '' else int(multiproc_cpus)

        # logging
        # ---------------------------
        self.log_path = log_path
        self.log_level = log_level

        logging.info('Initialization Complete')

    def run_validation(self):

        #-------------------------------------
        # Index directory
        #-------------------------------------
        logging.info('Directory Indexing Started')
        dir_indexer = directory_indexer()
        dir_df = dir_indexer.get_directory_listing(self.input_path, self.multiproc, self.multiproc_cpus)
        logging.debug(f'Directory Listing: {len(dir_df)} Files Indexed')
        logging.info(f'Directory Indexing Complete')

        #-------------------------------------
        # Send data to organizer to run validation
        #-------------------------------------        
        logging.info('Validation Started')
        
        #mod_organizer = modality_organizer()
        #validation_df = mod_organizer.run_validation(dir_df, self.output_path, self.answer_df, self.uids_old_to_new, self.multiproc, self.multiproc_cpus, self.log_path, self.log_level)

        #pat_organizer = patient_organizer()
        #validation_df = pat_organizer.run_validation(dir_df, self.output_path, self.answer_df, self.uids_old_to_new, self.multiproc, self.multiproc_cpus, self.log_path, self.log_level)

        #stu_organizer = study_organizer()
        #validation_df = stu_organizer.run_validation(dir_df, self.output_path, self.answer_df, self.uids_old_to_new, self.uids_new_to_old, self.multiproc, self.multiproc_cpus, self.log_path, self.log_level)
        
        #ser_organizer = series_organizer()
        #validation_df = ser_organizer.run_validation(dir_df, self.output_path, self.answer_df, self.uids_old_to_new, self.uids_new_to_old, self.multiproc, self.multiproc_cpus, self.log_path, self.log_level)        
        
        f_organizer = file_organizer()
        validation_df = f_organizer.run_validation(dir_df, self.output_path, self.answer_df, self.uids_old_to_new, self.uids_new_to_old, self.patids_old_to_new, self.multiproc, self.multiproc_cpus, self.log_path, self.log_level)        
        
        validation_df = validation_df.reset_index(drop=True)
        
        logging.info('Validation Complete')

        logging.info('Generating Categories')
        
        tqdm.pandas(desc="Parsing JSON")        
        validation_df['category_json'] = validation_df['answer_category_v2'].progress_apply(lambda x: json.loads(x.strip('<>')))
               
        # progress_bar = tqdm(total=1, desc="Normalizing JSON")       
        #progress_bar.set_description("Normalizing JSON")
        logging.info('Normalizing JSON')
        json_df = pd.json_normalize(validation_df['category_json'])        
        json_df.index = validation_df.index
        json_df.rename(columns={'hipaa.z':'hipaa_z','hipaa.m':'hipaa_m','dicom.p15':'dicom_p15','dicom.iod':'dicom_iod',
                                'dicom.safe':'dicom_safe','tcia.ptkb':'tcia_ptkb','tcia.p15':'tcia_p15','tcia.rev':'tcia_rev'}, inplace=True)        
        json_df['prev_cat'] = json_df['prev_cat'].astype(str)      
        # progress_bar.update(1)        
        # progress_bar.close()

    
        # progress_bar = tqdm(total=1, desc="Combining JSON")
        #progress_bar.set_description("Combining JSON")
        logging.info('Combining JSON')
        combined_df = pd.concat([validation_df.drop(columns=['category_json', 'answer_category_v2']), json_df], axis=1)
        # progress_bar.update(1)
        # progress_bar.close()

        logging.info('Categories Complete')        
        
        logging.info('Writing Results')
        combined_df.to_sql('validation_results', self.validation_db_conn, if_exists='replace')
        


        if len(validation_df) != 0:
            #-------------------------------------
            # Create Burn-in validation spreadsheet
            #-------------------------------------        
            pixel_val_df = validation_df[validation_df.action == '<pixels_hidden>']
            pixel_val_df = pixel_val_df[['check_passed','check_score','action','action_text','file_path','modality','class','patient','study','series','instance']]        
            pixel_val_df['file_path'] = pixel_val_df['file_path'].apply(lambda x: str(x).replace('<','').replace('>',''))            
            pixel_val_df.to_excel(os.path.join(self.output_path, "pixel_validation.xlsx"))
        else:
            logging.error('Zero results returned. Check UID Mapping File and/or ensure you are using correct Answer Key.')


