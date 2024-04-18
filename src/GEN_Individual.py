import pandas as pd
import numpy as np
from joblib import Parallel, delayed
import random



def update_age_gender(row, df_handler, df_handlerM, df_handlerF, AgeGroup):
    if row['P_GENDER'] == 'X':
        # print(row['HHID'])
        AGE_WEIGHT = df_handler.loc[df_handler['Alpha-3'] == row['HH_ISO']].values.flatten().tolist()
        del AGE_WEIGHT[-1]
        
        if sum(AGE_WEIGHT) == 0:
            if 20 < len(AGE_WEIGHT):  # Ensure n is within the range of the list
                AGE_WEIGHT[9] = 1
            else:
                print("n is out of the range of the list")
        # print('AGE WEIGHT:', AGE_WEIGHT)
        # print('AGE WEIGHT LEN:', len(AGE_WEIGHT))
        AGE = random.choices(range(1, 22), AGE_WEIGHT)[0]
        # print(AGE)
        age_range_min = (AGE - 1) * 5
        age_range_max = min(AGE * 5, 99) - 1
        # print('AGE_MIN:', age_range_min, 'AGE_MAX:', age_range_max)
        if age_range_min <= age_range_max:
            row['P_AGE'] = random.randint(age_range_min, age_range_max)
        else:
            row['P_AGE'] = random.randint(age_range_max, age_range_min)           
        row['AgeGroup'] = 'AGE' + str(AGE)
        row['AgeRange'] = f'{age_range_min}-{age_range_max}'
        gender_weight = df_handlerM.loc[df_handlerM['Alpha-3'] == row['HH_ISO']]['AGE' + str(AGE)].values[0]
        row['P_GENDER'] = random.choices(['M', 'F'], [gender_weight, 1 - gender_weight])[0]
    return row


def process_country(alpha_3, HH_data_pop_built, Combined, ISO_eur):
    alpha_3_name = alpha_3 if alpha_3 in ISO_eur else 'FRA'
    filename = f'data/populationData/{alpha_3_name}.csv'
    nation = pd.read_csv(filename)


    # Create AgeGroup and perform calculations
    nation['AgeGroup'] = ['AGE' + str(j+1) for j in range(nation.shape[0])]
    nation['AccPOP'] = nation['M'] + nation['F']
    TotalPOP = nation['AccPOP'].sum()
    nation['PercPOP'] = nation['AccPOP'] / TotalPOP
    nation['PercPOPM'] = nation['M'] / nation['AccPOP']
    nation['PercPOPF'] = nation['F'] / nation['AccPOP']
    
    matching_rows = HH_data_pop_built[HH_data_pop_built['HH_ISO'] == alpha_3_name]['SizeHH']
    if matching_rows.empty:
        TotalExpPOP = 0  # or some other default or error handling
    else:
        TotalExpPOP = matching_rows.values[0]
    ExpPOP = round(nation['PercPOP'] * TotalExpPOP).astype(int)
    nation['ExpPOPM'] = round(nation['PercPOPM'] * ExpPOP).astype(int)
    nation['ExpPOPF'] = round(nation['PercPOPF'] * ExpPOP).astype(int)

    # Merge with Combined data
    step_ISO = Combined.loc[Combined['HH_ISO'] == alpha_3_name].pivot_table(values='P_ID', index='AgeGroup', columns=['P_GENDER'], aggfunc='count').reset_index()
    step_ISO = step_ISO[step_ISO.AgeGroup != 'Unidentified']
    nation = nation.merge(step_ISO, on='AgeGroup', how='left')
    # Check if 'F_y' and 'M_y' column exists, if not, create it with default 0 values
    if 'F_y' not in nation.columns:
        nation['F_y'] = 0
    if 'M_y' not in nation.columns:
        nation['M_y'] = 0
    
    # Calculate GenPOP
    nation[['F_y', 'M_y']] = nation[['F_y', 'M_y']].fillna(0).astype(int)
    nation['GenPOPM'] = np.maximum(0, nation['ExpPOPM'] - nation['M_y'])
    nation['GenPOPF'] = np.maximum(0, nation['ExpPOPF'] - nation['F_y'])
    nation['GenPOP'] = nation['GenPOPM'] + nation['GenPOPF']
    nation['PercGenPOP'] = nation['GenPOP'] / nation['GenPOP'].sum()
    nation['PercGenPOPM'] = nation['GenPOPM'] / nation['GenPOP']
    nation['PercGenPOPF'] = nation['GenPOPF'] / nation['GenPOP']

    # Prepare data for concatenation
    nation_use = nation[['AgeGroup', 'PercGenPOP']].set_index('AgeGroup').T
    nation_use['Alpha-3'] = alpha_3
    nation_useM = nation[['AgeGroup', 'PercGenPOPM']].set_index('AgeGroup').T
    nation_useM['Alpha-3'] = alpha_3
    nation_useF = nation[['AgeGroup', 'PercGenPOPF']].set_index('AgeGroup').T
    nation_useF['Alpha-3'] = alpha_3    
    
    return nation_use, nation_useM, nation_useF
    # Additional code for final preparation and output
   

