import pandas as pd
import numpy as np
from random import choices, choice
from joblib import Parallel, delayed

from src import BOOK_Trip as bktrip

def select_traveller_type(hhid, personas, weight):
    types = choices(personas, weight)[0]
    return hhid, types

def persona_selection(df_HH, personas, weights, num_cores):
    """Assigns a persona to each household based on provided weights."""
    results = Parallel(n_jobs=num_cores)(
        delayed(select_traveller_type)(hhid, personas, weights) for hhid in df_HH['HHID']
    )
    _, traveller_types = zip(*results)
    return list(traveller_types)

def conditions(purpose, Nparty, HHType):
    if   purpose == 'business':
        return 'business', 0
    elif purpose == 'leisure':
        if Nparty == 1: 
            return 'solo', Nparty
        else:
            if HHType == '2B':
                return 'couple', Nparty
            if HHType == '2A':
                tipe = choices(['family', 'couple'], [0.5, 0.5])
                if tipe[0] == 'family':
                    return 'family', Nparty
                else:
                    return 'couple', 2
            elif HHType == '1A':
                return 'family', Nparty
            else:
                return 'family', Nparty
    elif purpose == 'group':
#         z = choices([3,4,5,6,7], [0.518733, 0.209850, 0.211782, 0.029883, 0.029752])
        return 'group', 0
    elif purpose == 'SOI':
        return 'SOI', Nparty
    
def process_behaviour(df, i):
    colName = 'Behaviour_' + str(i + 1)
    func = np.vectorize(conditions)
    # print("df_sizeHH:", df['SizeHH'])
    trType = func([el[1] for el in df[colName]], list(df['SizeHH']), list(df['HHType']))
    travelType = list(trType[0])
    NP = list(trType[1])

    # Update the inner lists in the 'list_column' with different values
    updated_col = [sublist + [value] for sublist, value in zip(df[colName], travelType)]
    updated_col = [sublist + [value] for sublist, value in zip(updated_col, NP)]

    return colName, updated_col

def update_travel_type(df, behaviour_num, num_cores):
    """Parallel computation for updating travel types for behaviours."""
    results = Parallel(n_jobs=num_cores)(
        delayed(process_behaviour)(df, i) for i in range(behaviour_num)
    )
    for col_name, updated_col in results:
        df[col_name] = updated_col
    return df

def generate_behaviour(df_HH, df_flight, personas, weight, behaviour_num, crosswalk, num_cores):
    """Main function to integrate and manage traveller behaviour analysis."""
    # print(f'df_HH.shape: {df_HH.shape}')
    df_flight_EU = df_flight[df_flight['region_D'] == 'Europe']
    EU_ISO = crosswalk[crosswalk['region'] == 'Europe']['HH_ISO'].unique()

    df_behaviour = pd.DataFrame({'HHID': df_HH['HHID']})

    for i in range(behaviour_num):
        destination = bktrip.destination_assign_init(df_HH, df_flight_EU, df_flight, EU_ISO, num_cores)
        print("Destination Assigned")
        traveller_type = persona_selection(df_HH, personas, weight, num_cores)
        print("Traveller Type Assigned")
        behaviour_list = [list(x) for x in zip(destination, traveller_type)]
        df_behaviour['Behaviour_'+str(i+1)] = pd.Series(behaviour_list)
        # print('df_behaviour_shape: ', df_behaviour.shape)
        
    if 'HHID' not in df_behaviour.columns:
        df_behaviour.insert(0, 'HHID', df_HH['HHID'].tolist())

    df_behaviour_col = list(df_behaviour.columns)
    del df_behaviour_col[0]
    column_name = list(list(df_HH.columns)+df_behaviour_col)
    df_behaviour_complete = df_HH.merge(df_behaviour, on = 'HHID', how = 'left')
    df_behaviour_complete = update_travel_type(df_behaviour_complete, behaviour_num, num_cores)
    print("Behaviour Complete")
    df_behaviour_complete = pd.DataFrame(df_behaviour_complete, columns=column_name)

    
    return df_behaviour_complete

# def behaviour(df_HH, df_flight, personas, weights, behaviour_num, crosswalk):
#     """Main function to integrate and manage traveller behaviour analysis."""
#     print(f'df_HH.shape: {df_HH.shape}')
#     df_flight_EU = df_flight[df_flight['region_D'] == 'Europe']
#     EU_ISO = crosswalk[crosswalk['region'] == 'Europe']['HH_ISO'].unique()

#     df_behaviour = pd.DataFrame({'HHID': df_HH['HHID']})
#     for i in range(behaviour_num):
#         destination = bktrip.destination_assign_init(df_HH, df_flight_EU, df_flight, EU_ISO)
#         traveller_type = persona_selection(df_HH, personas, weights)
#         df_behaviour[f'Behaviour_{i+1}'] = list(zip(destination, traveller_type))
#         print(f'df_behaviour_shape after {i+1} iteration: ', df_behaviour.shape)

#     df_behaviour_complete = df_HH.merge(df_behaviour, on='HHID', how='left')
#     df_behaviour_complete = update_travel_type(df_behaviour_complete, behaviour_num)
    
#     return df_behaviour, df_behaviour_complete