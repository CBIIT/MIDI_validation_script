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
import copy


class reports_helper(object):

    def __init__(self, config, log_path, log_level):

        logging.info('Initialization Started')

        run_name = config['run_name']
        input_data_path = config['input_data_path']
        output_data_path = config['output_data_path']
        answer_db_file = config['answer_db_file']
        uid_mapping_file = config['uid_mapping_file']
        multiproc = eval(config['multiprocessing'])
        multiproc_cpus = config['multiprocessing_cpus']

        # output path
        # ---------------------------
        self.output_path = os.path.join(output_data_path, run_name)

        # validation db
        # ---------------------------
        validation_db_path = os.path.join(self.output_path, "validation_results.db")
        validation_db_conn = sql.connect(validation_db_path)

        # validation file
        # ---------------------------
        validation_query = "select * from validation_results"
        self.validation_df = pd.read_sql(validation_query, validation_db_conn, index_col='index')

        validation_db_conn.close()

        # logging
        # ---------------------------
        self.log_path = log_path
        self.log_level = log_level

        logging.info('Initialization Complete')

    def run_reports(self):

        #-------------------------------------
        # Run Reports
        #-------------------------------------
        logging.info('Report Generation Started')

        logging.info('Discrepancy Report Started')
        discrepancy_df = self.discrepancy_report()
        if not discrepancy_df.empty:
            #discrepancy_df.to_excel(report_writer, 'Discrepancies', index=True)
            discrepancy_path = os.path.join(self.output_path, 'discrepancy_report.csv')
            discrepancy_df.to_csv(discrepancy_path)
        logging.info('Discrepancy Report Complete')

        report_path = os.path.join(self.output_path, 'scoring_report.xlsx')
        report_writer = pd.ExcelWriter(report_path)

        logging.info('Action Report Started')        
        action_df, action_pivot = self.action_report()
        if not action_pivot.empty:
            action_pivot.to_excel(report_writer, 'Actions', index=True)
            #action_path = os.path.join(self.output_path, 'action_report.csv')
            #action_df.to_csv(action_path)
        logging.info('Action Report Complete')

        logging.info('Category Report Started')
        category_df, category_pivot = self.category_report()
        if not category_pivot.empty:
            category_pivot.to_excel(report_writer, 'Categories', index=True)
            #category_path = os.path.join(self.output_path, 'category_report.csv')
            #category_df.to_csv(category_path)
        logging.info('Category Report Complete')

        logging.info('Scoring Report Started')
        scoring_df, scoring_pivot = self.scoring_report(category_df)
        if not scoring_pivot.empty:
            scoring_pivot.to_excel(report_writer, 'Scoring', index=True)
            #scoring_path = os.path.join(self.output_path, 'scoring_report.csv')
            #scoring_df.to_csv(scoring_path)
        logging.info('Scoring Report Complete')

        report_writer.save()

        logging.info('Report Generation Complete')

        return None

    def discrepancy_report(self):

        total_df = self.validation_df[self.validation_df['check_passed'].isin([0, np.nan])].copy()

        total_df.loc[total_df.tag_name == '<LUT Data>', 'file_value'] = '<Removed>'
        total_df.loc[total_df.tag_name == '<LUT Data>', 'answer_value'] = '<Removed>'
        total_df.loc[total_df.tag_name == '<LUT Data>', 'action_text'] = '<Removed>'

        total_df = total_df[['check_passed','check_score','tag_ds','tag_name','file_value','answer_value','action','action_text','answer_category','modality','class','patient','study','series','instance','file_name','file_path']]

        #if tag_name = '<LUT Data>'
        #   file_value = '<Removed>'
        #   answer_value = '<Removed>'
        #   action_text = '<Removed>'

        #total_df.to_csv(os.path.join(self.output_path, "total_report_test.csv"))

        return total_df

    def action_report(self):

        action_df = self.validation_df[['action','check_passed']].copy()
        action_df['check_passed'] = action_df['check_passed'].fillna(-1)
        action_pivot = pd.pivot_table(action_df, index=['action'], columns=['check_passed'], aggfunc=len, fill_value=0, dropna=False)
        action_pivot = action_pivot.rename(columns={'action':'Action',-1:'Blank',0:'Fail',1:'Pass'})
        action_pivot = action_pivot.reset_index()
        action_pivot.loc['Total']= action_pivot.sum(numeric_only=True, axis=0)
        action_pivot.loc[:,'Total'] = action_pivot.sum(numeric_only=True, axis=1)
        
        #action_pivot['total'] = action_pivot.sum(axis=1)
        #action_pivot.to_csv(os.path.join(self.output_path, "action_report_test.csv"))

        return action_df, action_pivot

    def category_report(self):

        initial_df = self.validation_df[['action','check_passed','check_score','answer_category']].copy()
        initial_df['answer_category'] = initial_df['answer_category'].apply(lambda x: str(x).replace('<','').replace('>','').replace('[]',"['tcia_standard']"))
        
        category_dict = {}
        category_iter = 0

        for index, row in initial_df.iterrows():
            cat_list = eval(row.answer_category)
            cat_list = list(dict.fromkeys(cat_list))

            for category in cat_list:            
            
                category_dict[category_iter] = {}
                category_dict[category_iter]['check_passed'] = row.check_passed
                category_dict[category_iter]['answer_category'] = category

                category_iter += 1

        category_df = pd.DataFrame.from_dict(category_dict, orient='index')
        category_df['check_passed'] = category_df['check_passed'].fillna(-1)
        category_pivot = pd.pivot_table(category_df, index=['answer_category'], columns=['check_passed'], aggfunc=len, fill_value=0, dropna=False)
        category_pivot = category_pivot.rename(columns={'answer_category':'Category',-1:'Blank',0:'Fail',1:'Pass'})
        category_pivot.index.names = ['Category']
        category_pivot = category_pivot.reset_index()
        category_pivot.loc['Total']= category_pivot.sum(numeric_only=True, axis=0)
        category_pivot.loc[:,'Total'] = category_pivot.sum(numeric_only=True, axis=1)

        #category_pivot.to_csv(os.path.join(self.output_path, "category_report_test.csv"))

        return category_df, category_pivot

    def scoring_report(self, category_df):

        def get_score_cat(category):

            hipaa_cats = ['patient_name','patient_mrn','patient_birth_date',
                          'patient_ssn','patient_address','patient_telephone']

            if category == 'dicom_standard':
                return 'Category 2 - DICOM Standard'
            elif category in hipaa_cats:
                return 'Category 1 - HIPAA'
            else:
                return 'Category 3 - Best Practice'

        scoring_df = category_df.copy()
        scoring_df['score_cat'] = scoring_df['answer_category'].apply(lambda x: get_score_cat(x))
        scoring_df = scoring_df[['check_passed','score_cat']]

        scoring_pivot = pd.pivot_table(scoring_df, index=['score_cat'], columns=['check_passed'], aggfunc=len, fill_value=0, dropna=False)
        scoring_pivot = scoring_pivot.rename(columns={'score_cat':'Category',-1:'Blank',0:'Fail',1:'Pass'})
        scoring_pivot.index.names = ['Category']
        scoring_pivot = scoring_pivot.reset_index()
        scoring_pivot.loc['Total']= scoring_pivot.sum(numeric_only=True, axis=0)
        scoring_pivot.loc[:,'Total'] = scoring_pivot.sum(numeric_only=True, axis=1)

        scoring_weights = {'Category 1 - HIPAA':50, 'Category 2 - DICOM Standard':30, 'Category 3 - Best Practice':20, }

        scores = []

        pivot_iter = scoring_pivot.copy()

        for index, row in pivot_iter.iterrows():

            if index == 'Total':
                final_score = sum(scores)
                final_score_str = '{percent:.2%}'.format(percent=final_score)
                scoring_pivot.at[index, 'Score'] = final_score_str
            else:
                weight = scoring_weights[row.Category]
                failed = row.Fail if 'Fail' in row else 0
                passed = row.Pass  if 'Pass' in row else 0
                total = row.Total if 'Total' in row else 0

                cat_score = (passed/total)
                cat_score_str = '{percent:.2%}'.format(percent=cat_score)
                add_score = (cat_score*(weight/100))
                scores.append(add_score)

                scoring_pivot.at[index, 'Weight'] = weight
                scoring_pivot.at[index, 'Score'] = cat_score_str

        return scoring_df, scoring_pivot