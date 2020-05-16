# -*- coding: utf-8 -*-
"""
Created on Tue Apr  9 15:58:28 2019

@author: ayelita.ray
"""

import os
import time

# Open URL in browser window
from redshift import make_select_query_with_redshift
# import pandas as pd
import csv


class Download:

    def __init__(self):
        pass

    def generate_and_save_csv_file(self, out_filepath, sql_query):
        print('Beginning Adoption Sectionwise file download with requests')
        start_time = time.time()

        query_results, colnames = make_select_query_with_redshift(sql_query)

        end_time = time.time()
        print(f"Query time: {end_time-start_time}")

        # Save the results to CSV
        with open(out_filepath, 'w') as f:
            writer = csv.writer(f, delimiter=',')
            writer.writerow(colnames)

            for row in query_results:
                writer.writerow(row)

        # df = pd.DataFrame(query_results,
        #                   columns=colnames)
        # df.to_csv(out_filepath, index=False)

        another_end_time = time.time()

        # os.remove(out_filepath)

        print(f"Overall Time taken: {another_end_time - start_time} seconds")
