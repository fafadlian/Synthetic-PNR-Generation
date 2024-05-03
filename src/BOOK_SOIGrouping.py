import random
import numpy as np
import pandas as pd
from joblib import Parallel, delayed
import warnings
import ast

from src import BOOK_Grouping as bkgrp

def stay_day_rt(df, SOI_Type_order, poi_base):
    """Assign stay days for business and vacation based on predefined weights."""
    df = df.reset_index(drop=True)
    stay_days = []
    for i in SOI_Type_order:
        row = poi_base[poi_base['SOI_Type'] == i]
        if not row['stay_day'].isna().any() and not row['stay_day_weight'].isna().any():
            stay_day_list = [int(x) for x in row['stay_day'].values[0]]
            probs = [float(x) for x in (ast.literal_eval(row['stay_day_weight'].values[0]) if isinstance(row['stay_day_weight'].values[0], str) else row['stay_day_weight'].values[0])]
            stay_day = random.choices(stay_day_list, probs)[0] if len(stay_day_list) == len(probs) else 0
            stay_days.append(stay_day)
    
    df['stay_day'] = stay_days
    df['airport_itinerary_out'] = df['fixRoute'].str.split("->").apply(lambda x: x if x else [])
    df['full_routes'] = df['airport_itinerary_out'].apply(lambda sublist: [f"{sublist[i]}-{sublist[i+1]}" for i in range(len(sublist)-1)] if len(sublist) >= 2 else [])
    return df


def grouping_init(df_behaviour_complete, select_behaviour, agencies, agency_weight, route, poi_base, num_cores):
    # SOI_Type_order = df_behaviour_complete['HHType'].tolist()
    target_value = ['SOI']
    SOI = df_behaviour_complete[df_behaviour_complete[select_behaviour].apply(lambda x: any(val in x for val in target_value))].reset_index()
    SOI[select_behaviour] = SOI[select_behaviour].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)
    SOI['group_value'] = SOI['IATA_O'] + "-" + SOI[select_behaviour].apply(bkgrp.extract_group_value)
    so = SOI.groupby('group_value').cumcount().add(0).astype(str)
    SOI['group_value'] += ('-ID' + so)
    unique_ids_SOI = SOI['HHID'].unique()
    #use the same process as leisure group (grouping based on the person in household)
    list_of_passengers_SOI = Parallel(n_jobs=num_cores)(delayed(bkgrp.basic_grouping)(SOI, unique_id, select_behaviour) for unique_id in unique_ids_SOI)
    df_SOI = pd.DataFrame(list(zip(SOI['group_value'], list_of_passengers_SOI)), columns=['init_id', 'list_of_passengers'])
    print('done SOI')

    df_SOI['route'] = df_SOI['init_id'].apply(bkgrp.extract_od)
    df_SOI['num_in_party'] = df_SOI['list_of_passengers'].str.len()
    df_SOI = bkgrp.assign_agencies(df_SOI, agencies, agency_weight)
    df_SOI = bkgrp.assign_booking_days(df_SOI, 30)
    df_SOI = bkgrp.merge_and_process_route(df_SOI, route)
    df_SOI.rename(columns={'Chosen_Route': 'fixRoute'}, inplace=True)
    df_SOI.drop(columns=[f'fixRoute_{i}' for i in range(1, 4)] +
                            [f'Leg_fixRoute_{i}' for i in range(1, 4)] +
                            [f'Inv_Leg_fixRoute_{i}' for i in range(1, 4)] +
                            ['Total_Inverse'] + [f'Prob_fix_Route_{i}' for i in range(1, 4)],
                    inplace=True)
    df_SOI['Leg_fixRoute'] = df_SOI['fixRoute'].str.count('->')
    SOI_Type_order = df_SOI['list_of_passengers'].apply(lambda x: x[0].split('_')[0] if x and len(x) > 0 else None).tolist()
    df_SOI = stay_day_rt(df_SOI, SOI_Type_order, poi_base)

    return df_SOI