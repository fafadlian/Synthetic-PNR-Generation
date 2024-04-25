import random
import numpy as np
import pandas as pd
from joblib import Parallel, delayed
import warnings



def assign_city(HH_ISO, grouped_org):
    # Filter the dataframe based on HH_ISO_O
    org = grouped_org[grouped_org['HH_ISO_O'] == HH_ISO]

    # Special case for 'GRC'
    if HH_ISO == 'GRC':
        return {'HH_ISO': HH_ISO, 'IATA_O': ['ATH'], 'Cap_O': [1.0]}

    # Compute the capacity proportion
    total_capacity = org['capacity'].sum()
    cap_org = org['capacity'] / total_capacity if total_capacity != 0 else org['capacity']

    # Return the result as a dictionary
    return {'HH_ISO': HH_ISO, 'IATA_O': org['IATA_O'].tolist(), 'Cap_O': cap_org.tolist()}

def select_iata_code(iso_code, df_city_lookup):
    row = df_city_lookup[df_city_lookup['HH_ISO'] == iso_code].iloc[0]

    # Check if row['IATA_O'] and row['Cap_O'] are non-empty lists
    iata_list = row['IATA_O'] if isinstance(row['IATA_O'], list) and len(row['IATA_O']) > 0 else None
    cap_list = row['Cap_O'] if isinstance(row['Cap_O'], list) and len(row['Cap_O']) > 0 else None

    if iata_list is None or cap_list is None:
        return None  # or a default value

    total = sum(cap_list)
    if total > 0:
        normalized_probs = [float(i)/total for i in cap_list]
    else:
        return random.choice(iata_list)

def choose_route(row):
    routes = [row['fixRoute_1'], row['fixRoute_2'], row['fixRoute_3']]
    probabilities = [row['Prob_fix_Route_1'], row['Prob_fix_Route_2'], row['Prob_fix_Route_3']]
    
    # Filter out invalid routes and probabilities
    valid_routes = [route for route, prob in zip(routes, probabilities) if pd.notna(route) and prob > 0]
    valid_probabilities = [prob for route, prob in zip(routes, probabilities) if pd.notna(route) and prob > 0]
    
    if not valid_routes or not valid_probabilities:
        return None
    return np.random.choice(valid_routes, p=valid_probabilities)

def original_city_assign_init(df_HH, df_flight, df_hubs, num_core):
    # Precompute the groupby
    grouped_org = df_flight.groupby(['HH_ISO_O', 'IATA_O'])['capacity'].sum().reset_index()

    # Using vectorized operation instead of apply for get_travel_iso
    # Assuming get_travel_iso can be rewritten in a vectorized form
    df_HH['ISO_Travel'] = vectorized_get_travel_iso(df_HH, df_hubs)  # Placeholder function

    # Parallel computation for city lookup
    unique_iso_travel = df_HH['ISO_Travel'].unique()
    city_lookup_list = Parallel(n_jobs=num_core)(
        delayed(assign_city)(HH_ISO, grouped_org) for HH_ISO in unique_iso_travel
    )
    
    df_city_lookup = pd.DataFrame(city_lookup_list)

    # Parallel computation for IATA code selection
    IATA_O = Parallel(n_jobs=num_core)(
        delayed(select_iata_code)(iso_code, df_city_lookup) for iso_code in df_HH['ISO_Travel']
    )

    df_HH['IATA_O'] = IATA_O

    # Handling missing values in 'IATA_O'
    df_HH['IATA_O'].fillna(pd.Series(np.random.choice(['LHR', 'CDG', 'IST', 'DXB', 'AUH'], size=len(df_HH))), inplace=True)

    return df_city_lookup, df_HH

def vectorized_get_travel_iso(df_HH, df_hubs):
    # Merge df_HH with df_hubs on 'HH_ISO' and 'Country Assoc'
    merged = df_HH.merge(df_hubs, left_on='HH_ISO', right_on='Country Assoc', how='left')

    # Function to handle the choice operation in a vectorized manner
    def choose_random_hub(x):
        if pd.notna(x) and x != '':
            hubs = x.split('|')  # Assuming hubs are separated by '|'
            return np.random.choice(hubs)
        return None

    # Apply the function to the 'Filtered Country Hubs' column
    merged['ISO_Travel'] = merged['Filtered Country Hubs'].apply(choose_random_hub)

    # Where 'ISO_Travel' is None, fill with the original 'HH_ISO'
    merged['ISO_Travel'].fillna(merged['HH_ISO'], inplace=True)

    return merged['ISO_Travel']
    
def assign_destination(HH_ISO, df_flight_EU, df_flight,  EU_ISO):
    # if HH_ISO in not_include:
    #     return {'HH_ISO': HH_ISO, 'IATA_D': None, 'Cap_D': None}

    if HH_ISO in EU_ISO:
        df_flight = df_flight
    else:
        df_flight = df_flight_EU

    des = df_flight[(df_flight['HH_ISO_O'] != HH_ISO) & (df_flight['HH_ISO_D'] != HH_ISO)].groupby('IATA_D')['capacity'].sum().reset_index()
    destination = des['IATA_D'].tolist()
    cap_des = (des['capacity'] / des['capacity'].sum()).tolist()

    # if HH_ISO == 'GRC':
    #     destination, cap_des = ['ATH'], [1.0]

    return {'HH_ISO': HH_ISO, 'IATA_D': [destination], 'Cap_D': [cap_des]}

def select_destination(iso_code, df_city_lookup):
    row = df_city_lookup[df_city_lookup['HH_ISO'] == iso_code]
    if row.empty or row['IATA_D'].iloc[0] is None:
        return None  # or a default value

    iata_list, cap_list = row['IATA_D'].iloc[0][0], row['Cap_D'].iloc[0][0]
    if len(iata_list) == len(cap_list):
        return random.choices(iata_list, cap_list)[0]
    else:
        return None  # Or your default value
    
def destination_assign_init(df_HH, df_flight_EU, df_flight, EU_ISO, num_core):


    # Parallel computation for destination assignment
    city_lookup_list = Parallel(n_jobs=num_core)(delayed(assign_destination)(HH_ISO, df_flight_EU, df_flight, EU_ISO) 
                                        for HH_ISO in df_HH['ISO_Travel'].unique())
    df_city_lookup = pd.DataFrame(city_lookup_list)

    # Parallel computation for destination selection
    IATA_D = Parallel(n_jobs=num_core)(delayed(select_destination)(iso_code, df_city_lookup) 
                                for iso_code in df_HH['ISO_Travel'])

    return IATA_D