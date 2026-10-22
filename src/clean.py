#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 22 13:04:58 2026

@author: hakim
"""
import pyspark.sql.functions as F




def clean_datasetes (datasets_df): 
    #count doublicate 
    num_doublons = (
    datasets_df
    .groupBy("dataset_id")
    .count()
    .filter(F.col("count") > 1)
    .count())  
   
    #drop dublicate
    datasets_df = datasets_df.dropDuplicates(["dataset_id"]) 
    
    ## None value traitment ## 
    datasets_df = datasets_df.withColumn('todrop',
                                         F.when(F.col('dataset_id').isNull()
                                                | F.col('title').isNull(),
                                                1)
                                         .otherwise(0)) 
    count_nan_dataset = datasets_df.filter(datasets_df.todrop == 1).count()  
    datasets_df = datasets_df.filter(F.col('todrop') == 0)  
    rejected_df = datasets_df.filter(F.col('todrop') == 1)  
    
    return  datasets_df , rejected_df 




            
    