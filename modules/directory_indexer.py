#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This module is used to index directories

"""

import os
from pydicom import dcmread
import pandas as pd
import logging

import concurrent.futures as futures

class directory_indexer(object):

    def __init__(self):
        a='a'

    def get_directory_listing(self, path, multiproc, multiproc_cpus):

        dir_dicts = []

        if multiproc:
            
            workers = 60 if multiproc_cpus > 60 else multiproc_cpus if multiproc_cpus >= 1 else 1

            with futures.ProcessPoolExecutor(max_workers=workers) as executor:

                futures_list = []

                for root, dirs, files in os.walk(path):
                    futures_list.append(executor.submit(self.index_directory, root, dirs, files))

                for future in futures.as_completed(futures_list):
                    dir_dicts.append(future.result())
        else:
            
            for root, dirs, files in os.walk(path):
               
                dir_dicts.append(self.index_directory(root, dirs, files))

        dir_df = pd.concat((pd.DataFrame.from_dict(dir_dict, 'index') for dir_dict in dir_dicts))
        dir_df.reset_index(drop=True, inplace=True)
        files_found = len(dir_df)
        logging.debug(f'{str(files_found)} Files Found')

        return dir_df

    def index_directory(self, root, dirs, files):
        
        sop_class = ''
        modality = ''
        patient = ''
        study = ''
        series = ''
        instance = ''
        instance_num = ''
        file_name = ''
        file_path = ''

        dir_dict = {}
        dir_iter = 0

        for file in files:
            dir_dict[dir_iter] = {}
                
            file_path = os.path.join(root, file)
            logging.debug(file_path)

            with open(file_path, 'rb') as dcm:
                dataset = dcmread(dcm, force=True)
                
                sop_class = dataset.SOPClassUID if 'SOPClassUID' in dataset else None
                modality = dataset.Modality if 'Modality' in dataset else None
                patient = dataset.PatientID if 'PatientID' in dataset else None
                study = dataset.StudyInstanceUID if 'StudyInstanceUID' in dataset else None
                series = dataset.SeriesInstanceUID if 'SeriesInstanceUID' in dataset else None
                instance = dataset.SOPInstanceUID if 'SOPInstanceUID' in dataset else None
                instance_num = dataset.InstanceNumber if 'InstanceNumber' in dataset else None

            dir_dict[dir_iter]['class'] = sop_class
            dir_dict[dir_iter]['modality'] = modality
            dir_dict[dir_iter]['patient'] = patient
            dir_dict[dir_iter]['study'] = study
            dir_dict[dir_iter]['series'] = series
            dir_dict[dir_iter]['instance'] = instance
            dir_dict[dir_iter]['instance_num'] = instance_num
            dir_dict[dir_iter]['file_name'] = file 
            dir_dict[dir_iter]['file_path'] = file_path

            dir_iter += 1

        return dir_dict