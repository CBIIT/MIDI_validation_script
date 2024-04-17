#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This module is used to index directories

"""

import os
from pydicom import dcmread, errors
import pandas as pd
import logging
from glob import glob
import concurrent.futures as futures
from tqdm import tqdm
import warnings
import hashlib
import math

class directory_indexer(object):

    def get_directory_listing(self, path, multiproc, multiproc_cpus):

        files = self.get_directory_files(path)
        batch_size = max(1, min(50, math.ceil(len(files) / multiproc_cpus))) # min 1, max 250 files in a batch
        #batch_size = len(dir_files) // (multiproc_cpus * 5) + (1 if len(dir_files) % multiproc_cpus > 0 else 0)
        batches = [files[i:i + batch_size] for i in range(0, len(files), batch_size)]
        
        file_dicts = []
        
        if multiproc:
            workers = max(1, min(multiproc_cpus, os.cpu_count(), 60))
            with futures.ProcessPoolExecutor(max_workers=workers) as executor:
                
                futures_list = [executor.submit(self.index_files, batch) for batch in batches]
                
                for future in tqdm(futures.as_completed(futures_list), total=len(futures_list), desc="Indexing File Batches"):
                    result = future.result()
                    file_dicts.extend(result)
                    
        else:
            for batch in tqdm(batches, desc="Indexing File Batches"):
                result = self.index_files(batch)
                file_dicts.extend(result)            

        dir_df = pd.DataFrame(file_dicts)
        
        #dir_df.to_excel(r'C:\data\midi\validation_test\test\new_dir_index.xlsx')
        
        return dir_df

    def get_directory_files(self, path):
        """List all files in the given directory."""
        all_paths = glob(os.path.join(path, '**', '*'), recursive=True)
        return [p for p in all_paths if os.path.isfile(p)]

    def md5sum_data(self, data):
        """Calculate the md5sum of data, return (size, digest)"""
        if isinstance(data, str):
            # Encode the string to bytes
            data = data.encode()

        data_hash = hashlib.md5(data)
        data_size = len(data)
        data_digest = data_hash.hexdigest()
    
        return data_size, data_digest

    def index_files(self, file_paths):
        
        file_dicts = []
        
        for file_path in file_paths:

            file_dict = {}

            try:
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore", message="Unknown encoding")
                    
                    with open(file_path, 'rb') as dcm:
                        try:
                            dataset = dcmread(dcm, force=False)
                        except errors.InvalidDicomError:
                            continue
                        
                        pixel_digest = None
                        if 'PixelData' in dataset:
                            pixel_size, pixel_digest = self.md5sum_data(dataset.PixelData)
                        
                        file_dict = {
                            'class': getattr(dataset, 'SOPClassUID', None),
                            'modality': getattr(dataset, 'Modality', None),
                            'patient': getattr(dataset, 'PatientID', None),
                            'study': getattr(dataset, 'StudyInstanceUID', None),
                            'series': getattr(dataset, 'SeriesInstanceUID', None),
                            'instance': getattr(dataset, 'SOPInstanceUID', None),
                            'instance_num': getattr(dataset, 'InstanceNumber', None),
                            'file_name': os.path.basename(file_path),
                            'file_path': file_path,
                            'file_digest': pixel_digest
                        }
                file_dicts.append(file_dict)
                
            except Exception as e:
                logging.error(f"Error reading {file_path}: {e}")

        return file_dicts



    # def __init__(self):
    #     a='a'

    # def get_directory_listing(self, path, multiproc, multiproc_cpus):

    #     dir_dicts = []

    #     if multiproc:
            
    #         workers = 60 if multiproc_cpus > 60 else multiproc_cpus if multiproc_cpus >= 1 else 1

    #         with futures.ProcessPoolExecutor(max_workers=workers) as executor:

    #             futures_list = []

    #             for root, dirs, files in os.walk(path):
    #                 futures_list.append(executor.submit(self.index_directory, root, dirs, files))

    #             for future in futures.as_completed(futures_list):
    #                 dir_dicts.append(future.result())
    #     else:
            
    #         for root, dirs, files in os.walk(path):
               
    #             dir_dicts.append(self.index_directory(root, dirs, files))

    #     dir_df = pd.concat((pd.DataFrame.from_dict(dir_dict, 'index') for dir_dict in dir_dicts))
    #     dir_df.reset_index(drop=True, inplace=True)
    #     files_found = len(dir_df)
    #     logging.debug(f'{str(files_found)} Files Found')
        
    #     #dir_df.to_excel(r'C:\data\midi\validation_test\test\old_dir_index.xlsx')

    #     return dir_df



    # def index_directory(self, root, dirs, files):
        
    #     sop_class = ''
    #     modality = ''
    #     patient = ''
    #     study = ''
    #     series = ''
    #     instance = ''
    #     instance_num = ''
    #     file_name = ''
    #     file_path = ''

    #     dir_dict = {}
    #     dir_iter = 0

    #     for file in files:
    #         dir_dict[dir_iter] = {}
                
    #         file_path = os.path.join(root, file)
    #         logging.debug(file_path)

    #         with open(file_path, 'rb') as dcm:
    #             dataset = dcmread(dcm, force=True)
                
    #             sop_class = dataset.SOPClassUID if 'SOPClassUID' in dataset else None
    #             modality = dataset.Modality if 'Modality' in dataset else None
    #             patient = dataset.PatientID if 'PatientID' in dataset else None
    #             study = dataset.StudyInstanceUID if 'StudyInstanceUID' in dataset else None
    #             series = dataset.SeriesInstanceUID if 'SeriesInstanceUID' in dataset else None
    #             instance = dataset.SOPInstanceUID if 'SOPInstanceUID' in dataset else None
    #             instance_num = dataset.InstanceNumber if 'InstanceNumber' in dataset else None

    #         dir_dict[dir_iter]['class'] = sop_class
    #         dir_dict[dir_iter]['modality'] = modality
    #         dir_dict[dir_iter]['patient'] = patient
    #         dir_dict[dir_iter]['study'] = study
    #         dir_dict[dir_iter]['series'] = series
    #         dir_dict[dir_iter]['instance'] = instance
    #         dir_dict[dir_iter]['instance_num'] = instance_num
    #         dir_dict[dir_iter]['file_name'] = file 
    #         dir_dict[dir_iter]['file_path'] = file_path

    #         dir_iter += 1

    #     return dir_dict