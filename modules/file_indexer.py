#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This module is used to index files

"""

import os
from pydicom import dcmread
import pandas as pd
import numpy as np
import logging
import warnings

import concurrent.futures as futures

class file_indexer(object):

    #def __init__(self):

    def get_file_table(self, file_data, multiproc, multiproc_cpus, log_path, log_level):

        dcm_dicts = []

        cpu_count = os.cpu_count()

        if multiproc and len(file_data) > (multiproc_cpus * 100):
            workers = max(1, min(multiproc_cpus, os.cpu_count(), 60))
            
            file_lists = np.array_split(file_data, workers)

            with futures.ProcessPoolExecutor(max_workers=workers) as executor:

                futures_list = []

                for file_list in file_lists:
                    futures_list.append(executor.submit(self.index_files, file_list, log_path, log_level))

                for future in futures.as_completed(futures_list):
                    dcm_dicts.append(future.result())

        else:
            dcm_dicts.append(self.index_files(file_data, log_path, log_level))

        dcm_df = pd.concat((pd.DataFrame.from_dict(dcm_dict, 'index') for dcm_dict in dcm_dicts))

        return dcm_df

    def index_files(self, list_df, log_path, log_level):
        
        def initialize_logging(log_path, log_level):

            logging.basicConfig(
                level=log_level,
                format="%(asctime)s - [%(levelname)s] - %(message)s",
                handlers=[
                    logging.FileHandler(log_path, 'a'),
                    logging.StreamHandler()
                ]
            )

        def index_file_elements(dataset, dict, depth, count, label):

            #recursively iterate all items in dataset

            ignore_value = ['Pixel Data', 'Overlay Data', 'File Meta Information Version']

            tag_dict = dict

            for tag in dataset:
            
                tag_path = ''

                if tag.is_private:
                    tag_label = str(tag.tag).strip().replace(r', ',r',')

                    part_01 = tag_label[1:5]
                    part_02 = tag_label[6:8]
                    part_03 = tag_label[8:10]

                    value = ''

                    if tag.private_creator:
                        private_creator = str(tag.private_creator).upper()
                        tag_label = f'({part_01},"{private_creator}",{part_03})'
                    else:
                        tag_label = str(tag.tag).strip().replace(r', ',r',')

                else:
                    private_creator = ''

                    tag_label = str(tag.tag).strip().replace(r', ',r',')

                #---------------------------------

                if count:
                    append = f'[<{str(count).zfill(4)}>]'
                else:
                    append = f'[<0000>]'

                #---------------------------------

                if depth == 0:
                    tag_path = f'<{tag_label}>'
                else:
                    tag_path = f'{label}{append}<{tag_label}>'

                #---------------------------------

                #if tag_path in ['<(0018,115e)>','<(0018,1702)>','<(0018,1706)>','<(0018,7032)>','<(0028,0103)>','<(0028,1052)>','<(0040,0302)>']:
                #    a='a'

                #if tag_path in ['<(0019,"SIEMENS CT VA0  COAD",92)>','<(0021,"SIEMENS MED",11)>']:
                #    a='a'

                if tag.name in ignore_value:
                    if tag.value:
                        tag_dict[tag_path] = f'<REMOVED>'
                    else:
                        tag_dict[tag_path] = f'<REMOVED>'
                else:
                    if tag.value is not None:
                        tag_dict[tag_path] = f'<{str(tag.value).strip()}>'
                    else:
                        tag_dict[tag_path] = f'<>'

                #---------------------------------

                #logging.debug(f'  {tag_path}')

                if tag.VR == 'SQ':   # a sequence
                
                    for i, seq_tag in enumerate(tag.value):
                    
                        tag_dict = index_file_elements(seq_tag, tag_dict, depth + 1, i, tag_path)

            return tag_dict

        initialize_logging(log_path, log_level)

        table_dict = {}
        table_iter = 0

        for index, row in list_df.iterrows():

            #logging.debug(f'Indexing {row.file_path}')

            table_dict[table_iter] = {}
            table_dict[table_iter]['file_name'] = f'<{row.file_name}>'
            table_dict[table_iter]['file_path'] = f'<{row.file_path}>'
            table_dict[table_iter]['file_digest'] = f'<{row.file_digest}>'

            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", message="Unknown encoding")  
                
                with open(row.file_path, 'rb') as dcm:
                    dataset = dcmread(dcm, force=True)
                
                    table_dict[table_iter]['modality'] = f'<{dataset.Modality}>'
                    table_dict[table_iter]['class'] = f'<{dataset.SOPClassUID}>'
                    table_dict[table_iter]['patient'] = f'<{dataset.PatientID}>'
                    table_dict[table_iter]['study'] = f'<{dataset.StudyInstanceUID}>'
                    table_dict[table_iter]['series'] = f'<{dataset.SeriesInstanceUID}>'
                    table_dict[table_iter]['instance'] = f'<{dataset.SOPInstanceUID}>'

                    table_dict[table_iter] = index_file_elements(dataset, table_dict[table_iter], 0, 0, None)

            table_iter += 1

        return table_dict