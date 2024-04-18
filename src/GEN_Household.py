import pandas as pd
import numpy as np
from unidecode import unidecode
from random import choices, choice, randrange
import random
import warnings
from collections import defaultdict
from itertools import product
from joblib import Parallel, delayed
import time

def get_distribution(age, size):
    # Mapping age ranges and sizes to their corresponding distributions and probabilities
    age_size_to_dist_probs = {
        (24, 1): (['1B'], [1]),
        (24, 2): (['1A', '2B', '3AB'], [0.01, 0.28, 0.71]),
        (24, 3): (['1A', '2A', '3AB'], [0.01, 0.062, 0.92]),
        (24, float('inf')): (['1A', '2A', '3AB'], [0.003, 0.05, 0.94]),
        (54, 1): (['1B'], [1]),
        (54, 2): (['1A', '2B', '3AB'], [0.07, 0.67, 0.26]),
        (54, 3): (['1A', '2A', '3AB'], [0.053, 0.717, 0.23]),
        (54, float('inf')): (['1A', '2A', '3AB'], [0.02, 0.68, 0.3]),
        (64, 1): (['1B'], [1]),
        (64, 2): (['1A', '2B', '3AB'], [0.01, 0.85, 0.14]),
        (64, 3): (['1A', '2A', '3AB'], [0.02, 0.2, 0.78]),
        (64, float('inf')): (['1A', '2A', '3AB'], [0.005, 0.15, 0.845]),
        (float('inf'), 1): (['1B'], [1]),
        (float('inf'), 2): (['1A', '2B', '3AB'], [0.0005, 0.93, 0.0695]),
        (float('inf'), 3): (['1A', '2A', '3AB'], [0.0065, 0.033, 0.9605]),
        (float('inf'), float('inf')): (['1A', '2A', '3AB'], [0.002, 0.02, 0.978]),
    }

    # Determine the appropriate distribution and probabilities based on age and size
    for (max_age, max_size), (distribution, probabilities) in age_size_to_dist_probs.items():
        if age <= max_age and (size == max_size or max_size == float('inf')):
            return distribution, probabilities

    # Default case if no specific match is found
    return ['Unknown'], [1]

def compute_HHType(subset):
    HH_Type = []
    for _, row in subset.iterrows():
        age, size = row['AgeHOH'], row['SizeHH']
        distribution, probabilities = get_distribution(age, size)
        hh_type = choices(distribution, probabilities)[0]
        HH_Type.append(hh_type)
    # Instead of returning a list, return a DataFrame with the new HH_Type column
    subset['HHType'] = HH_Type
    return subset

def assign_demographics(age_hoh, gender_hoh, hh_type, index, min_age, max_age):
    """Assigns age and gender based on household type and position."""
    if hh_type == '1B' or (hh_type == '3AB' and index == 0):
        return age_hoh, gender_hoh
    elif hh_type in ['1A', '2A', '2B']:
        age = age_hoh if index == 0 else randrange(min_age, max_age)
        gender = gender_hoh if index == 0 else choices(['M', 'F'], [0.5, 0.5])[0]
        if hh_type in ['2A', '2B'] and index == 1:
            gender = choices(['M', 'F'], [0.999, 0.001] if gender_hoh == 'M' else [0.001, 0.999])[0]
        return age, gender
    elif hh_type == '3AB':
        return -1, 'X'  # Assuming 'X' is a placeholder for unspecified.

def process_individuals(subset):
    """Processes a subset of households to generate individual-level data."""
    P_ID, P_AGE, P_GENDER, HHID = [], [], [], []
    for _, row in subset.iterrows():
        hh_id, hh_size, age_hoh, gender_hoh, hh_type = row['HHID'], row['SizeHH'], row['AgeHOH'], row['GenderHOH'], row['HHType']
        min_age = max(0, age_hoh - 40)
        max_age = randrange(1, 3) if age_hoh - 25 <= min_age else age_hoh - 25
        
        for j in range(hh_size):
            P_ID.append(f"{hh_id}_{j+1}")
            HHID.append(hh_id)
            age, gender = assign_demographics(age_hoh, gender_hoh, hh_type, j, min_age, max_age)
            P_AGE.append(age)
            P_GENDER.append(gender)
    
    return P_ID, P_AGE, P_GENDER, HHID

def parallel_process_individuals(data, num_partitions=-1):
    """Splits the data into chunks and processes them in parallel."""
    chunks = np.array_split(data, num_partitions)
    results = Parallel(n_jobs=num_partitions)(delayed(process_individuals)(chunk) for chunk in chunks)
    return map(lambda x: [item for sublist in x for item in sublist], zip(*results))

def classify_age_groups(P_AGE):
    age_bounds = [(range(0, 5), '0-4', 'AGE1'),
                  (range(5, 10), '5-9', 'AGE2'),
                  (range(10, 15), '10-14', 'AGE3'),
                  (range(15, 20), '15-19', 'AGE4'),
                  (range(20, 25), '20-24', 'AGE5'),
                  (range(25, 30), '25-29', 'AGE6'),
                  (range(30, 35), '30-34', 'AGE7'),
                  (range(35, 40), '35-39', 'AGE8'),
                  (range(40, 45), '40-44', 'AGE9'),
                  (range(45, 50), '45-49', 'AGE10'),
                  (range(50, 55), '50-54', 'AGE11'),
                  (range(55, 60), '55-59', 'AGE12'),
                  (range(60, 65), '60-64', 'AGE13'),
                  (range(65, 70), '65-69', 'AGE14'),
                  (range(70, 75), '70-74', 'AGE15'),
                  (range(75, 80), '75-79', 'AGE16'),
                  (range(80, 85), '80-84', 'AGE17'),
                  (range(85, 90), '85-89', 'AGE18'),
                  (range(90, 95), '90-94', 'AGE19'),
                  (range(95, 100), '95-99', 'AGE20'),
                  (range(100, 120), '100+', 'AGE21')]

    AgeRange, AgeGroup = [], []
    for age in P_AGE:
        for bounds, range_label, group_label in age_bounds:
            if age in bounds:
                AgeRange.append(range_label)
                AgeGroup.append(group_label)
                break
        else:  # This else corresponds to the for-loop (not the if statement).
            AgeRange.append('Unidentified')
            AgeGroup.append('Unidentified')

    return AgeRange, AgeGroup



