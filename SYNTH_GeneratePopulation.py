import pandas as pd
import numpy as np
import random
from unidecode import unidecode
from random import choices, choice, randrange
import warnings
import os
from collections import defaultdict
from itertools import product
from joblib import Parallel, delayed
import time
from faker import Faker
import datetime
import json

import src.GEN_Household
import src.GEN_Individual
import src.GEN_Identity

genhh = src.GEN_Household
genind = src.GEN_Individual
genid = src.GEN_Identity


warnings.filterwarnings('ignore')

class DataAnalysis:
    def __init__(self, data_dir, hh_count, num_cores):
        # Initialize the DataAnalysis class with the data directory
        self.data_dir = data_dir
        self.hh_count = hh_count
        self.num_cores = num_cores
        self.load_data()  # Load the data
        self.preprocess_data()  # Preprocess the data
        print("Data Analysis Initialized")

    def load_data(self):
        # Load all required data files
        self.df = pd.read_csv(os.path.join(self.data_dir, 'populationData/EUONLYPOPAGG.csv'))
        self.cw = pd.read_csv(os.path.join(self.data_dir, 'geoCrosswalk/GeoCrossWalkMed.csv'))
        self.iso_codes = pd.read_csv(os.path.join(self.data_dir, 'ISO.csv'))
        print("Data Loaded")

    def preprocess_data(self):
        # Preprocess loaded data
        self.ISO = np.unique(self.iso_codes['HH_ISO']).tolist()
        self.nationality_list = self.cw['HH_ISO'].unique().tolist()
        self.ISO_eur = list(self.df['Alpha-3'])
        self.hh_type_list = [f'{g}_AGEHOH{i}_T{j}_PERC' for g in ['F', 'M'] for i in range(1, 6) for j in range(1, 5)]
        
        # Add language information
        self.available_langs = self.cw['Lang'].unique().tolist()
        self.LangDF = self.cw[['HH_ISO', 'Lang']].drop_duplicates(subset='HH_ISO', keep="first")
        
        print("Data Preprocessed")

    def compute_region_stats(self):
        # Compute statistics for each region
        region_stats = {}
        for region in self.ISO:
            current_region = region if region in self.ISO_eur else 'Other'
            region_data = self.df[self.df['Alpha-3'] == current_region]
            hh_perc3 = region_data['HH_PERC3'].iloc[0]
            hh_stat = region_data[self.hh_type_list].values.flatten().tolist()
            region_stats[region] = (hh_perc3, hh_stat)
        return region_stats
    


    def process_region(self, region, hh_count, region_stats):
        # Process a single region
        data = defaultdict(list)
        hh_perc3, region_hh_stat = region_stats[region]
        make_hh = max(int(hh_count * hh_perc3), 1)

        age_ranges = self.define_age_ranges()
        size_ranges = {'T1': (1, 1), 'T2': (2, 3), 'T3': (4, 5), 'T4': (6, 8)}

        for hh_id in range(make_hh):
            hh_type = random.choices(self.hh_type_list, region_hh_stat)[0]
            hh_type_parsed = hh_type.split('_')

            data['GenderHOH'].append(hh_type_parsed[0])
            age_key = ''.join(hh_type_parsed[1:2])
            data['AgeHOH'].append(random.randrange(*age_ranges[age_key]))
            size_key = hh_type_parsed[2]
            data['SizeHH'].append(random.randint(*size_ranges[size_key]))
            data['HHID'].append(f"{region}_{hh_id}")
            data['HH_ISO'].append(region)

        return data

    def define_age_ranges(self):
        # Define age ranges for household heads
        return {'AGEHOH1': (18, 25), 'AGEHOH2': (25, 35), 'AGEHOH3': (35, 50),
                'AGEHOH4': (50, 65), 'AGEHOH5': (65, 80)}
    
    def classify_age(self, age):
        if age >= 100: return 'AGE21'
        return f'AGE{age // 5 + 1}'
    
    def flatten_weights(self, weights):
    # Create a list to hold all the flattened entries
        flat_list = []

        # Iterate over each country code in weights
        for iso, weight_details in weights.items():
            age_weights = weight_details['age']
            gender_weights = weight_details['gender']
            
            # Assuming gender weights are lists of equal length and correspond to age ranges
            for index, age_weight in enumerate(age_weights):
                # Prepare a dictionary for each age range
                row = {
                    'ISO': iso,
                    'AgeRangeIndex': index + 1,  # Human-readable index (1-based)
                    'AgeWeight': age_weight,
                    'MaleWeight': gender_weights['M'][index] if index < len(gender_weights['M']) else None,
                    'FemaleWeight': gender_weights['F'][index] if index < len(gender_weights['F']) else None
                }
                flat_list.append(row)

        return flat_list
    

    def run_analysis(self):
        # Run the data analysis
        print("Running Data Generation")
        region_stats = self.compute_region_stats()
        if self.num_cores == -1:
            num_partitions = 4
        else:
            num_partitions = self.num_cores 
        HH_data = defaultdict(list)

        # Generate Basic Household Data
        results = Parallel(n_jobs=self.num_cores)(
            delayed(self.process_region)(region, self.hh_count, region_stats)
            for region in region_stats.keys()
        )
        
        for result in results:
            for key, value in result.items():
                HH_data[key].extend(value)
        HH_data = pd.DataFrame(HH_data)
        group = HH_data.groupby(['HH_ISO', "AgeHOH", "GenderHOH"])['HHID'].count().reset_index()
        group['AgeGroup'] = group['AgeHOH'].apply(lambda age: self.classify_age(age))
        chunks = np.array_split(HH_data, num_partitions)
        print(chunks[0].shape)
        HH_data = Parallel(n_jobs=self.num_cores)(delayed(genhh.compute_HHType)(chunk) for chunk in chunks)
        HH_data = pd.concat(HH_data)
        HH_data_pop_built = HH_data.groupby(['HH_ISO'])['SizeHH'].sum().reset_index()

        # Generate Basic Individual Data
        P_ID, P_AGE, P_GENDER, HHID = genhh.parallel_process_individuals(HH_data, num_partitions)
        AgeRange, AgeGroup = genhh.classify_age_groups(P_AGE)
        person_data = pd.DataFrame({'HHID': HHID, 'P_ID': P_ID, 'P_AGE': P_AGE,'AgeRange': AgeRange, 'AgeGroup': AgeGroup, 'P_GENDER': P_GENDER})
        person_data = person_data.merge(HH_data, on='HHID', how='left')
        print('Basic Individual Data Generated')

        # Measuring Statistical Fit of Generated Basic Individual Data
        nation = Parallel(n_jobs=self.num_cores)(delayed(genind.process_country)(alpha_3, HH_data_pop_built, person_data, self.ISO_eur) for alpha_3 in self.ISO)
        df_handler = pd.concat([res[0] for res in nation])
        df_handlerM = pd.concat([res[1] for res in nation])
        df_handlerF = pd.concat([res[2] for res in nation])
        df_handlerM = df_handlerM.fillna(0)
        df_handlerF = df_handlerF.fillna(0)
        df_handler = df_handler.fillna(0)
        print('Statistical Fit Measuring Completed')

        #Statistical Fitting The Generated Basic Individual Data to the Population Data
        updated_rows = []
        updated_rows = Parallel(n_jobs=self.num_cores)(delayed(genind.update_age_gender)(row, df_handler, df_handlerM, df_handlerF, AgeGroup) for index, row in person_data.iterrows())
        person_data = pd.DataFrame(updated_rows)
        HH_data = HH_data.merge(self.LangDF, on = 'HH_ISO', how = 'left')
        print('Statistical Fitting Completed')

        # Assign Household Identities
        HH_identity = Parallel(n_jobs=self.num_cores)(delayed(genid.household_identity)(row, self.available_langs, self.nationality_list) for index, row in HH_data.iterrows())
        HH_identity = pd.DataFrame(HH_identity)
        HH_identity.columns = ['Lang_P', 'Surname', 'Address', 'PostCode', 'Country', 'NationalityLP', 'NationalityNat']
        HH_data = pd.concat([HH_data, HH_identity], axis=1)
        HH_ID_data = HH_data[['HHID', 'Lang', 'Surname', 'Address', 'PostCode', 'Country']].copy()
        person_data = person_data.merge(HH_ID_data, on = 'HHID', how = 'left')
        print('Assigning Household Identities Completed')

        # Assign Individual Identities
        Ind_identity = Parallel(n_jobs=self.num_cores)(delayed(genid.individual_identity)(row, self.available_langs) for index, row in person_data.iterrows())
        Ind_identity = pd.DataFrame(Ind_identity)
        person_data = pd.concat([person_data, Ind_identity], axis=1)
        person_data['DOC_FirstName'] = person_data['FirstName'].apply(unidecode)
        person_data['DOC_Surname'] = person_data['Surname'].apply(unidecode)

        
        print('Assigning Individual Identities Completed')

        # Assign Typos in DOCS
        person_data_docs = pd.DataFrame(Parallel(n_jobs=self.num_cores)(delayed(genid.docIDs)(row) for index, row in person_data.iterrows()))
        print('Assigning Typos in DOCS Completed')
        person_data = pd.concat([person_data, person_data_docs], axis=1)
        HH_data.insert(0, 'HH_num', range(1, HH_data.shape[0]+1))
        person_data.insert(0, 'P_num', range(1, person_data.shape[0]+1))
        HH_Nat = HH_data[['HHID', 'NationalityNat']].copy()
        person_data = person_data.merge(HH_Nat, on = 'HHID', how = 'left')
        print(person_data.columns)
        HH_data.to_csv(os.path.join(self.data_dir, 'synthesizedData/HH_data.csv'), index=False)
        person_data.to_csv(os.path.join(self.data_dir, 'synthesizedData/person_data.csv'), index=False)
        print('Data Saved')
        
    

    
# Usage
# data_analysis = DataAnalysis(data_dir='data', hh_count=100, num_cores=4)
# data_analysis.run_analysis()
# print("Data Analysis Completed")

