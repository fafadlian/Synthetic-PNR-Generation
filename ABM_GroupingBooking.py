import pandas as pd
import numpy as np
import random
from collections import defaultdict
from itertools import product
from joblib import Parallel, delayed
import os

from src import BOOK_Trip as bktrip
from src import BOOK_GenBehaviour as bkbeh
from src import BOOK_Grouping as bkgroup


class GroupBooking:
    def __init__(self, data_dir, num_cores):
        self.data_dir = data_dir
        self.num_cores = num_cores
        self.load_data()
        print("Group and Booking Initialized")

    def load_data(self):
        print("Loading data...")
        self.df_flight = pd.read_csv(os.path.join(self.data_dir, 'flightData/EU_flight_new.csv'))
        self.crosswalk = pd.read_csv(os.path.join(self.data_dir, 'geoCrosswalk/GeoCrossWalkLarge.csv'))
        self.route = pd.read_csv(os.path.join(self.data_dir, 'flightData/route_all.csv'))
        self.df_HH = pd.read_csv(os.path.join(self.data_dir, 'synthesizedData/HH_data.csv'))
        self.df_hubs = pd.read_csv(os.path.join(self.data_dir, 'geoCrosswalk/ReverseHubsV2.csv'))
        self.df_flight = self.df_flight[self.df_flight['IATA_O'] != r'\N']
        self.df_flight = self.df_flight[self.df_flight['IATA_D'] != r'\N']
        self.df_hubs = self.df_hub_clean(self.df_hubs)
        self.bus_stay_day = pd.read_csv(os.path.join(self.data_dir, 'business_stay.csv'))['bus_stay_day'].tolist()
        self.bus_stay_weight = pd.read_csv(os.path.join(self.data_dir, 'business_stay.csv'))['bus_stay_weight'].tolist()
        self.vac_stay_day = pd.read_csv(os.path.join(self.data_dir, 'vacation_stay.csv'))['vac_stay_day'].tolist()
        self.vac_stay_weight = pd.read_csv(os.path.join(self.data_dir, 'vacation_stay.csv'))['vac_stay_weight'].tolist()
        self.personas = pd.read_csv(os.path.join(self.data_dir, 'personas.csv'))['personas'].tolist()
        self.weight = pd.read_csv(os.path.join(self.data_dir, 'personas.csv'))['weight'].tolist()
        self.agencies = pd.read_csv(os.path.join(self.data_dir, 'agencies.csv'))['agencies'].tolist()
        self.agency_weight = pd.read_csv(os.path.join(self.data_dir, 'agencies.csv'))['agency_weight'].tolist()
        print("Data Loaded")

    def df_hub_clean(self, df_hubs):
        # Using regular expressions to replace unwanted characters
        df_hubs['Filtered Country Hubs'] = df_hubs['Filtered Country Hubs'].str.replace(r"[\[\]']", "", regex=True).str.split(",").str.join("|")
        return df_hubs
    
    def run_analysis(self):
        print("Running Grouping and Booking")
        df_city, self.df_HH = bktrip.original_city_assign_init(self.df_HH, self.df_flight, self.df_hubs, self.num_cores)
        print("Original City Assigned")
        df_behaviour_complete = bkbeh.generate_behaviour(self.df_HH, self.df_flight, self.personas, self.weight, 3, self.crosswalk, self.num_cores)
        print("Behaviour Generated")
        select_behaviour = 'Behaviour_1'
        df_group = bkgroup.grouping_init(df_behaviour_complete, select_behaviour, self.agencies, self.agency_weight, self.route, self.bus_stay_day, self.bus_stay_weight, self.vac_stay_day, self.vac_stay_weight, self.num_cores)
        print("Grouping Completed")
        df_behaviour_complete.to_csv(os.path.join(self.data_dir, 'synthesizedData/behaviour_complete.csv'), index=False)
        df_group.to_csv(os.path.join(self.data_dir, 'synthesizedData/group.csv'), index=False)
        print("Grouping and Booking Completed")

# Usage Example
# grouping_booking = GroupBooking(data_dir='data', num_cores=-1)
# grouping_booking.run_analysis()
    
