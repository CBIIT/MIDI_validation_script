#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This module is used to run validation reports (and scoring)

"""

import os
import pandas as pd
import numpy as np
import sqlite3 as sql
import logging
import concurrent.futures as futures
import json
from tqdm import tqdm


class reports_helper(object):

    def __init__(self, config):

        logging.info('Initialization Started')

        self.config = config

        # input_data_path = config['input_data_path']
        # answer_db_file = config['answer_db_file']
        # uid_mapping_file = config['uid_mapping_file']
        
        # multiprocessing
        # self.multiproc = eval(config['multiprocessing'])
        # self.multiproc_cpus = 0 if config['multiprocessing_cpus'] == '' else int(config['multiprocessing_cpus'])
        self.multiproc = False
        self.multiproc_cpus = 1        

        # output path
        # ---------------------------
        run_name = config['run_name']
        output_data_path = config['output_data_path']
        self.output_path = os.path.join(output_data_path, run_name)

        # validation data
        # ---------------------------
        validation_db_path = os.path.join(self.output_path, "validation_results.db")
        validation_db_conn = sql.connect(validation_db_path)
        validation_query = "select * from validation_results"
        self.validation_df = pd.read_sql(validation_query, validation_db_conn, index_col='index')
        validation_db_conn.close()

        # logging
        # ---------------------------
        self.log_path = config['log_path']
        self.log_level = config['log_level']    
        
        self.series_based = eval(config['report_series'])

        logging.info('Initialization Complete')

    def run_reports(self):

        #-------------------------------------
        # Run Reports
        #-------------------------------------

        logging.info('Report Generation Started')

        report_tasks = {'Discrepancy Report': self.discrepancy_report,
                        'Scoring Report': self.scoring_report,
                        'Action Report': self.action_report,
                        'Category Report': self.category_report} #,
                        #'Category Scoring Report': self.category_scoring_report}        

        report_results = {}

        if self.multiproc:
            workers = max(1, min(self.multiproc_cpus, os.cpu_count(), 60))
            
            with futures.ProcessPoolExecutor(max_workers=workers) as executor:
                
                futures_list = {executor.submit(task): name for name, task in report_tasks.items()}
                
                progress_bar = tqdm(futures.as_completed(futures_list.keys()), total=len(report_tasks), desc='Generating Reports')
                
                for future in progress_bar:
                    name = futures_list[future]
                    try:
                        report_results[name] = future.result()
                        progress_bar.set_postfix_str(f"{name} Complete")
                            
                    except Exception as e:
                        logging.error(f"Error generating {name}: {e}")
                        
        else:
            progress_bar = tqdm(report_tasks.items(), total=len(report_tasks), desc='Generating Reports')
            
            for name, task in progress_bar:
                try:
                    result = task()
                    report_results[name] = result
                    progress_bar.set_postfix_str(f"{name} Complete")
                    
                except Exception as e:
                    logging.error(f"Error generating {name}: {e}")

        output_file = os.path.join(self.output_path, 'scoring_report_series.xlsx' if self.series_based else 'scoring_report_instance.xlsx')
        with pd.ExcelWriter(output_file, engine='openpyxl', mode='w') as report_writer:
            
            for name, _ in tqdm(report_tasks.items(), total=len(report_results), desc='Writing Reports'):
                report = report_results.get(name)
                if report is not None:
                    if name == 'Discrepancy Report':
                        internal_df, participant_df = report
                        internal_path = os.path.join(self.output_path, 'discrepancy_report_internal.csv')
                        internal_df.to_csv(internal_path)
                        participant_path = os.path.join(self.output_path, 'discrepancy_report_participant.csv')
                        participant_df.to_csv(participant_path)
                    elif name == 'Action Report':
                        action_df, action_pivot = report
                        if not action_pivot.empty:
                            action_pivot.to_excel(report_writer, 'Actions', index=True) 
                    elif name == 'Category Report':
                        category_df, category_pivot = report
                        if not category_pivot.empty:
                            category_pivot.to_excel(report_writer, 'Categories', index=True)
                    elif name == 'Scoring Report':
                        scoring_df, scoring_pivot = report
                        if not scoring_pivot.empty:
                            scoring_pivot.to_excel(report_writer, 'Scoring', index=False)
                    elif name == 'Category Scoring Report':
                        scoring_df, scoring_pivot = report
                        if not scoring_pivot.empty:
                            scoring_pivot.to_excel(report_writer, 'Category Scoring', index=True)                            

        logging.info('Report Generation Complete')

        return None

    def discrepancy_report(self):

        total_df = self.validation_df[self.validation_df['check_passed'].isin([0, np.nan])].copy()        
        # validation_db_conn = sql.connect(self.validation_db_path)
        # validation_query = "select * from validation_results where check_passed = 0 or check_passed is null"
        # total_df = pd.read_sql(validation_query, validation_db_conn, index_col='index')
        # validation_db_conn.close()

        total_df.loc[total_df.tag_name == '<LUT Data>', 'file_value'] = '<Removed>'
        total_df.loc[total_df.tag_name == '<LUT Data>', 'answer_value'] = '<Removed>'
        total_df.loc[total_df.tag_name == '<LUT Data>', 'action_text'] = '<Removed>'
        
        category_tuples = total_df.apply(self.check_category, axis=1)
        total_df['category'] = category_tuples.apply(lambda x: x[0] if x != 0 else 'unknown')
        total_df['subcategory'] = category_tuples.apply(lambda x: x[1] if x != 0 else 'unknown')

        internal_df = total_df[['check_passed','check_score','tag_ds','tag_name','file_value','answer_value','action','action_text',
                                'category', 'subcategory', 'hipaa_z', 'hipaa_m', 'dicom_p15', 'dicom_iod', 'dicom_safe', 'tcia_ptkb', 
                                'tcia_p15', 'tcia_rev', 'prev_cat', 'modality','class','patient','study','series','instance','file_name','file_path']]
        
        participant_df = total_df[['check_passed','check_score','tag_ds','tag_name','file_value','answer_value','action','action_text',
                                   'category', 'subcategory', 'modality','class','patient','study','series','instance','file_name']]
        
        return internal_df, participant_df

    def action_report(self):

        action_df = self.validation_df.copy()
        # validation_db_conn = sql.connect(self.validation_db_path)
        # validation_query = "select * from validation_results"
        # action_df = pd.read_sql(validation_query, validation_db_conn, index_col='index')
        # validation_db_conn.close()        

        if self.series_based:
            #action_df.drop(columns=['file_index','check_index','instance','file_name','file_path'], errors='ignore', inplace=True)
            action_df.sort_values('check_passed', inplace=True)
            action_df.drop_duplicates(subset=['action', 'tag_ds', 'patient', 'study', 'series'], keep='first', inplace=True)
        
        action_df = action_df[['action','check_passed']].copy()
        action_df['check_passed'] = action_df['check_passed'].fillna(-1)
        
        action_pivot = pd.pivot_table(action_df, index=['action'], columns=['check_passed'], aggfunc=len, fill_value=0, dropna=False)
        action_pivot = action_pivot.rename(columns={'action':'Action',-1:'Blank',0:'Fail',1:'Pass'})
        action_pivot = action_pivot.reset_index()
        action_pivot.loc['Total']= action_pivot.sum(numeric_only=True, axis=0)
        action_pivot.loc[:,'Total'] = action_pivot.sum(numeric_only=True, axis=1)
        
        return action_df, action_pivot

    def check_category(self, row):
        if row.action in ['<tag_retained>','<text_notnull>']:
            return ('dicom', row['dicom_iod'])
        elif row.action in ['<date_shifted>']:
            return ('hipaa', 'HIPAA-C')
        elif row.action in ['<uid_changed>']:
            return ('hipaa', 'HIPAA-R')
        elif row.action in ['<pixels_hidden>']:
            return ('hipaa', 'HIPAA-A')
        elif row.action in ['<patid_consistent>']:
            return ('dicom', 'DICOM-P15-BASIC-C')
        elif row.action in ['<uid_consistent>']:
            return ('dicom', 'DICOM-P15-BASIC-U')
        elif row.action in ['<pixels_retained>']:
            return ('tcia', 'TCIA-P15-PIX-K')
        elif row.action in ['<text_removed>']:
            if row['hipaa_m']:
                return ('hipaa', row['hipaa_m'])
            elif row['hipaa_z']:
                return ('hipaa', row['hipaa_z'])
            elif row['tcia_p15']:
                return ('tcia', row['tcia_p15'])
            elif row['tcia_ptkb']:
                return ('tcia', row['tcia_ptkb'])
            elif row['tcia_rev']:
                return ('tcia', row['tcia_rev'])
        elif row.action in ['<text_retained>']:
            if row['tcia_p15']:
                return ('tcia', row['tcia_p15'])
            elif row['tcia_ptkb']:
                return ('tcia', row['tcia_ptkb'])
            elif row['tcia_rev']:
                return ('tcia', row['tcia_rev'])                
        else:
            return 0

    def category_report(self):

        total_df = self.validation_df.copy()
        # validation_db_conn = sql.connect(self.validation_db_path)
        # validation_query = "select * from validation_results"
        # total_df = pd.read_sql(validation_query, validation_db_conn, index_col='index')
        # validation_db_conn.close() 

        if self.series_based:
            #total_df.drop(columns=['file_index','check_index','instance','file_name','file_path'], errors='ignore', inplace=True)
            total_df.sort_values('check_passed', inplace=True)
            total_df.drop_duplicates(subset=['action', 'tag_ds', 'patient', 'study', 'series'], keep='first', inplace=True)
        
        category_tuples = total_df.apply(self.check_category, axis=1)

        total_df['category'] = category_tuples.apply(lambda x: x[0] if x != 0 else 'unknown')
        total_df['subcategory'] = category_tuples.apply(lambda x: x[1] if x != 0 else 'unknown')

        category_df = total_df[['category','subcategory','check_passed']].copy()
        category_df['check_passed'] = category_df['check_passed'].fillna(-1)
        
        category_pivot = pd.pivot_table(category_df, index=['category','subcategory'], columns=['check_passed'], aggfunc=len, fill_value=0) #, dropna=False)
        category_pivot = category_pivot.rename(columns={'category':'Category','subcategory':'Subcategory', -1:'Blank',0:'Fail',1:'Pass'})
        
        category_pivot = category_pivot.reset_index()
        
        category_pivot.loc['Total']= category_pivot.sum(numeric_only=True, axis=0)
        category_pivot.loc[:,'Total'] = category_pivot.sum(numeric_only=True, axis=1)

        return category_df, category_pivot

    def scoring_report(self):
        
        scoring_df = self.validation_df.copy()
        # validation_db_conn = sql.connect(self.validation_db_path)
        # validation_query = "select * from validation_results"
        # scoring_df = pd.read_sql(validation_query, validation_db_conn, index_col='index')
        # validation_db_conn.close()         

        if self.series_based:
            #scoring_df.drop(columns=['file_index','check_index','instance','file_name','file_path', 'action_text', 'file_value', 'answer_value'], errors='ignore', inplace=True)
            scoring_df.sort_values('check_passed', inplace=True)
            scoring_df.drop_duplicates(subset=['action', 'tag_ds', 'patient', 'study', 'series'], keep='first', inplace=True)

        scoring_df['score_cat'] = "All"
        scoring_df = scoring_df[['check_passed','score_cat']].copy()
        scoring_df['check_passed'] = scoring_df['check_passed'].fillna(-1)

        scoring_pivot = pd.pivot_table(scoring_df, index=['score_cat'], columns=['check_passed'], aggfunc=len, fill_value=0, dropna=False)
        scoring_pivot = scoring_pivot.rename(columns={'score_cat':'Category', -1:'Blank',0:'Fail',1:'Pass'})
        scoring_pivot.index.names = ['Category']
        scoring_pivot = scoring_pivot.reset_index()
        scoring_pivot.loc['Total']= scoring_pivot.sum(numeric_only=True, axis=0)
        scoring_pivot.loc[:,'Total'] = scoring_pivot.sum(numeric_only=True, axis=1)  

        scores = []
        final_score = 0.0

        pivot_iter = scoring_pivot.copy()

        for index, row in pivot_iter.iterrows():

            # if index == 'Total':
            #     final_score_str = '{percent:.2%}'.format(percent=final_score)
            #     scoring_pivot.at[index, 'Weighted_score'] = final_score_str
            # else:
            failed = row.Fail if 'Fail' in row else 0
            passed = row.Pass  if 'Pass' in row else 0
            total = row.Total if 'Total' in row else 0

            cat_score = (passed/total)
            cat_score_str = '{percent:.2%}'.format(percent=cat_score)

            scoring_pivot.at[index, 'Score'] = cat_score_str

        scoring_pivot.drop('Total', inplace=True)

        return scoring_df, scoring_pivot

    def category_scoring_report(self):
        
        scoring_df = self.validation_df.copy()
        # validation_db_conn = sql.connect(self.validation_db_path)
        # validation_query = "select * from validation_results"
        # scoring_df = pd.read_sql(validation_query, validation_db_conn, index_col='index')
        # validation_db_conn.close()          

        if self.series_based:
            #scoring_df.drop(columns=['file_index','check_index','instance','file_name','file_path', 'action_text', 'file_value', 'answer_value'], errors='ignore', inplace=True)
            scoring_df.sort_values('check_passed', inplace=True)
            scoring_df.drop_duplicates(subset=['action', 'tag_ds', 'patient', 'study', 'series'], keep='first', inplace=True)
            
        category_tuples = scoring_df.apply(self.check_category, axis=1)
        scoring_df['score_cat'] = category_tuples.apply(lambda x: x[0] if x != 0 else 'unknown')
        
        category_map = {
            'hipaa': 'Category 1 - HIPAA',
            'dicom': 'Category 2 - DICOM Standard',
            'tcia': 'Category 3 - Best Practice'
        }
        scoring_df['score_cat'] = scoring_df['score_cat'].map(category_map)
        
        scoring_df = scoring_df[['check_passed','score_cat']].copy()
        scoring_df['check_passed'] = scoring_df['check_passed'].fillna(-1)
        
        scoring_pivot = pd.pivot_table(scoring_df, index=['score_cat'], columns=['check_passed'], aggfunc=len, fill_value=0, dropna=False)
        scoring_pivot = scoring_pivot.rename(columns={'score_cat':'Category', -1:'Blank',0:'Fail',1:'Pass'})
        scoring_pivot.index.names = ['Category']
        scoring_pivot = scoring_pivot.reset_index()
        scoring_pivot.loc['Total']= scoring_pivot.sum(numeric_only=True, axis=0)
        scoring_pivot.loc[:,'Total'] = scoring_pivot.sum(numeric_only=True, axis=1)        

        scoring_weights = {'Category 1 - HIPAA':70, 'Category 2 - DICOM Standard':20, 'Category 3 - Best Practice':10, }

        scores = []
        final_score = 0.0

        pivot_iter = scoring_pivot.copy()

        for index, row in pivot_iter.iterrows():

            if index == 'Total':
                # final_score = sum(scores)
                final_score_str = '{percent:.2%}'.format(percent=final_score)
                scoring_pivot.at[index, 'Weighted Score'] = final_score_str
            else:
                weight = scoring_weights[row.Category]
                failed = row.Fail if 'Fail' in row else 0
                passed = row.Pass  if 'Pass' in row else 0
                total = row.Total if 'Total' in row else 0

                cat_score = (passed/total)
                cat_score_str = '{percent:.2%}'.format(percent=cat_score)
                add_score = (cat_score*(weight/100))
                final_score += add_score
                add_score_str = '{percent:.2%}'.format(percent=add_score)
                scores.append(add_score_str)

                scoring_pivot.at[index, 'Weight'] = weight
                scoring_pivot.at[index, 'Score'] = cat_score_str
                scoring_pivot.at[index, 'Weighted Score'] = add_score_str

        return scoring_df, scoring_pivot
