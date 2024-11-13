


"""
    Utility functions for reading OMOP vocabulary files:
        concept, concept_relationship (maps-to subset), source_to_concept_map
    Also reads the CCDA-tools local file oid.csv
"""


import pandas as pd
import numpy as np
import argparse
import os




def read_concept():
    from foundry.transforms import Dataset
    concept_df = Dataset.get("omop_concept").read_table(format="pandas")
    """
    concept_df = pd.read_csv("omop_concept_files",
                             engine='c', header=0, sep='\t',
                             #index_col=0,
                             on_bad_lines='warn',
                             dtype={
                                    #'concept_id': , 'Int64',
                                    'concept_id': np.int32 ,
                                    'concept_name': str,
                                    'domain_id' : str,
                                    'vocabulary_id' : str,
                                    'concept_class_id' : str,
                                    'standard_concept' : str,
                                    'concept_code': str,
                                    'invalid_reason': str,
                                    'valid_start_date' : str,
                                    'valid_end_date' : str,
                                    #'valid_start_date' : some proper date class TBD
                                    #'valid_end_date' : some proper date class TBD,
                                },
                                usecols = [0, 1, 2, 3, 4, 5, 6, 7] # no dates for now
                             )
    """
    return concept_df


def read_concept_maps_to():
    concept_relationship_df = pd.read_csv("../CCDA_OMOP_Private/CONCEPT_RELATIONSHIP.csv",
                             engine='c',
                             header=0,
                             # index_col=0,
                             sep='\t',
                             on_bad_lines='warn',
                             dtype={
                                    #'concept_id': , 'Int64',
                                    'concept_id_1': np.int32 ,
                                    'concept_id_2': np.int32 ,
                                    'relationship_id' : str,
                                    'valid_start_date' : str,
                                    'valid_end_date' : str,
                                    #'valid_start_date' : some proper date class TBD
                                    #'valid_end_date' : some proper date class TBD,
                                    'invalid_reason': str
                                }
                                #,usecols = [0, 1, 2] # no dates or date types for now
                             )

    #concept_maps_to_df = concept_relationship_df[concept_relationship_df['relationship_id' == 'Maps to']]
    concept_maps_to_df = concept_relationship_df.query("relationship_id == 'Maps to'")
    return concept_maps_to_df

def source_to_concept_map():
    print("SOURCE_TO_CONCEPT_MAP.csv")
    source_to_concept_df = pd.read_csv("../CCDA_OMOP_Private/SOURCE_TO_CONCEPT_MAP.csv",
                                 engine='c',
                                 header=0,
                                 # index_col=0,
                                 sep='\t',
                                 on_bad_lines='warn',
                                 dtype={
                                        'source_code': str,
                                        'source_concept_id': np.int32 ,
                                        'source_vocabulary_id': str ,
                                        'source_code_description' : str,
                                        'target_concept_id': np.int32 ,
                                        'target_vocabulary_id': str ,
                                        'valid_start_date' : str,
                                        'valid_end_date' : str,
                                        'invalid_reason': str
                                    }
                                       )
    return source_to_concept_df


def read_vocabulary_tables():
    """ returns (concept_df, concept_relationship_df)
    """

    concept_df = read_concept()
    concept_maps_to_df = read_concept_maps_to()
    source_to_conceptt_df = source_to_concept_map()

    return (concept_df, concept_maps_to_df,source_to_concept_df)

