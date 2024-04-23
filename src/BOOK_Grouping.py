import random
import numpy as np
import pandas as pd
from joblib import Parallel, delayed
import warnings


def basic_grouping(leisure, unique_id, select_behaviour):
    """Generate a list of passenger identifiers for leisure groups."""
    behaviour_count = leisure.loc[unique_id, select_behaviour][3]
    return [f"{leisure.loc[unique_id, 'HHID']}_{j + 1}" for j in range(behaviour_count)]

def bus_group_assign(business_group):
    """Assign business groups based on predefined probability distributions."""
    if business_group.empty:
        print("Warning: 'business_group' is empty. No data to process.")
        return pd.DataFrame()

    all_df_list = []
    for group_value, group in business_group.groupby('group_value'):
        entities = [f"{hhid}_1" for hhid in group['HHID']]
        group_size_probs = {1: 0.25, 2: 0.25, 3: 0.25, 4: 0.25}
        all_groups = []

        while entities:
            chosen_size = min(random.choices(list(group_size_probs.keys()), weights=list(group_size_probs.values()))[0], len(entities))
            group = [entities.pop() for _ in range(chosen_size)]
            all_groups.append(group)

        init_ids = [f"{group_value}-ID{index}" for index, _ in enumerate(all_groups)]
        df = pd.DataFrame({'init_id': init_ids, 'list_of_passengers': all_groups})
        all_df_list.append(df)

    return pd.concat(all_df_list, ignore_index=True) if all_df_list else pd.DataFrame()

def extract_group_value(row):
    """Extract group values for identifying unique group characteristics."""
    return f"{row[0]}-{row[1]}" if row else None

def extract_od(row):
    """Extract origin-destination string from route information."""
    return row[0:7] if len(row) > 0 else None

def stay_day_rt(df, bus_stay_day, bus_stay_weight, vac_stay_day, vac_stay_weight):
    """Assign stay days for business and vacation based on predefined weights."""
    df = df.reset_index(drop=True)
    tr_type = df['init_id'].str.split("-", expand=True)[2]
    business_mask = tr_type == 'business'
    df['stay_day'] = None
    df.loc[business_mask, 'stay_day'] = random.choices(bus_stay_day, bus_stay_weight, k=business_mask.sum())
    df.loc[~business_mask, 'stay_day'] = random.choices(vac_stay_day, vac_stay_weight, k=(~business_mask).sum())

    df['airport_itinerary_out'] = df['fixRoute'].str.split("->").apply(lambda x: x if x else [])
    df['full_routes'] = df['airport_itinerary_out'].apply(lambda sublist: [f"{sublist[i]}-{sublist[i+1]}" for i in range(len(sublist)-1)] if len(sublist) >= 2 else [])
    return df


def complete_routing(sublist):
    return [f"{sublist[i]}-{sublist[i+1]}" for i in range(len(sublist)-1)] if len(sublist) >= 2 else []


def assign_agencies(df, agencies, agency_weights):
    """Assign agencies to the bookings using a vectorized operation."""
    if np.isclose(sum(agency_weights), 1):
        df['BookingAgency'] = np.random.choice(agencies, size=len(df), p=agency_weights)
    else:
        raise ValueError("Probabilities must sum to 1.")
    return df

def assign_booking_days(df, max_day):
    """Assign booking days based on a linear increasing probability distribution."""
    probabilities = np.linspace(1, max_day, max_day)
    probabilities /= probabilities.sum()  # Normalize to sum to 1
    df['BookingDay'] = np.random.choice(np.arange(1, max_day + 1), size=len(df), p=probabilities)
    return df

def choose_route(row):
    """Choose route based on probabilities."""
    routes = [row[f'fixRoute_{i}'] for i in range(1, 4)]
    probabilities = [row[f'Prob_fix_Route_{i}'] for i in range(1, 4)]
    valid_routes = [route for route, prob in zip(routes, probabilities) if pd.notna(route) and prob > 0]
    valid_probabilities = [prob for route, prob in zip(routes, probabilities) if pd.notna(route) and prob > 0]
    if not valid_routes or not valid_probabilities:
        return None
    return np.random.choice(valid_routes, p=valid_probabilities)

def merge_and_process_route(df, route):
    """Merge booking data with routes and process probabilities for fixing routes."""
    df = pd.merge(df, route, on='route', how='left')
    df = fill_na_and_calculate_inverse_probabilities(df)
    df['Chosen_Route'] = df.apply(choose_route, axis=1)
    df = df[~df['Chosen_Route'].isna()]
    return df

def fill_na_and_calculate_inverse_probabilities(df):
    """Handle NaN values and calculate inverse probabilities for fixing routes."""
    for i in range(1, 4):
        df[f'Leg_fixRoute_{i}'].fillna(0, inplace=True)
        df[f'Inv_Leg_fixRoute_{i}'] = 1 / df[f'Leg_fixRoute_{i}'].replace(0, np.inf)

    df['Total_Inverse'] = df[[f'Inv_Leg_fixRoute_{i}' for i in range(1, 4)]].sum(axis=1)
    for i in range(1, 4):
        df[f'Prob_fix_Route_{i}'] = df[f'Inv_Leg_fixRoute_{i}'] / df['Total_Inverse']
    return df

def grouping_init(df_behaviour_complete, select_behaviour, agencies, agency_weight, route, bus_stay_day, bus_stay_weight, vac_stay_day, vac_stay_weight):
    # Business and Group Part
    target_value = ['business', 'group']
    businessgroup = df_behaviour_complete[df_behaviour_complete[select_behaviour].apply(lambda x: any(val in x for val in target_value))]
    businessgroup['group_value'] = businessgroup['IATA_O'] + "-" + businessgroup[select_behaviour].apply(extract_group_value)

    df_bus = bus_group_assign(businessgroup) 
    print('done business group')
    
    # Leisure Part    
    target_value = ['leisure']
    leisure = df_behaviour_complete[df_behaviour_complete[select_behaviour].apply(lambda x: any(val in x for val in target_value))].reset_index()
    leisure['group_value'] = leisure['IATA_O'] + "-" + leisure[select_behaviour].apply(extract_group_value)
    s = leisure.groupby('group_value').cumcount().add(0).astype(str)
    leisure['group_value'] += ('-ID' + s)
    unique_ids = leisure['HHID'].unique()
    #grouping based on the person in household
    list_of_passengers = Parallel(n_jobs=1)(delayed(basic_grouping)(leisure, unique_id, select_behaviour) for unique_id in range(len(unique_ids)))
    df_leisure = pd.DataFrame(list(zip(leisure['group_value'], list_of_passengers)), columns=['init_id', 'list_of_passengers'])
    print('done leisure')

    # SOI Part
    target_value = ['SOI']
    SOI = df_behaviour_complete[df_behaviour_complete[select_behaviour].apply(lambda x: any(val in x for val in target_value))].reset_index()
    SOI['group_value'] = SOI['IATA_O'] + "-" + SOI[select_behaviour].apply(extract_group_value)
    so = SOI.groupby('group_value').cumcount().add(0).astype(str)
    SOI['group_value'] += ('-ID' + so)
    unique_ids_SOI = SOI['HHID'].unique()
    #use the same process as leisure group (grouping based on the person in household)
    list_of_passengers_SOI = Parallel(n_jobs=1)(delayed(basic_grouping)(SOI, unique_id) for unique_id in range(len(unique_ids_SOI)))
    df_SOI = pd.DataFrame(list(zip(SOI['group_value'], list_of_passengers_SOI)), columns=['init_id', 'list_of_passengers'])
    print('done SOI')

                                                


    
    

    # Combine and Merge Data
    dataframes = []
    if 'df_bus' in locals() and df_bus is not None and not df_bus.empty:
        dataframes.append(df_bus)
    if 'df_leisure' in locals() and df_leisure is not None and not df_leisure.empty:
        dataframes.append(df_leisure)
    if 'df_SOI' in locals() and df_SOI is not None and not df_SOI.empty:
        dataframes.append(df_SOI)

    if dataframes:
        df_combined = pd.concat(dataframes, ignore_index=True)
    else:
        df_combined = pd.DataFrame()

    # print(df_combined['init_id'].head())
    df_combined['route'] = df_combined['init_id'].apply(extract_od)
    df_combined['num_in_party'] = df_combined['list_of_passengers'].str.len()
    df_combined = assign_agencies(df_combined, agencies, agency_weight)
    df_combined = assign_booking_days(df_combined, 30)
    df_combined = merge_and_process_route(df_combined, route)
    df_combined.rename(columns={'Chosen_Route': 'fixRoute'}, inplace=True)
    df_combined.drop(columns=[f'fixRoute_{i}' for i in range(1, 4)] +
                            [f'Leg_fixRoute_{i}' for i in range(1, 4)] +
                            [f'Inv_Leg_fixRoute_{i}' for i in range(1, 4)] +
                            ['Total_Inverse'] + [f'Prob_fix_Route_{i}' for i in range(1, 4)],
                    inplace=True)
    df_combined['Leg_fixRoute'] = df_combined['fixRoute'].str.count('->')
    # print(df_combined.columns)
    df_combined = stay_day_rt(df_combined, bus_stay_day, bus_stay_weight, vac_stay_day, vac_stay_weight)

 

    return df_combined