import json
import time
from os import listdir
from os.path import isfile, join
import pandas as pd
import numpy as np
from random import choices

from xml.dom import minidom
import random
import os
from joblib import Parallel, delayed
import time, math

# Standard library imports

# Third-party library imports
import xml.etree.ElementTree as ET

# Local imports
from src import BUILD_xml as bxml

class BuildXML:
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.load_data()
        print("Build XML Initialized")

    def load_data(self):
        """Load data from csv files."""
        self.df_HH = pd.read_csv(os.path.join(self.data_dir, 'synthesizedData/HH_data.csv'))
        self.df_P = pd.read_csv(os.path.join(self.data_dir, 'synthesizedData/person_data.csv'))
        self.df_flight = pd.read_csv(os.path.join(self.data_dir, 'synthesizedData/flights_complete.csv'))
        self.df_book = pd.read_csv(os.path.join(self.data_dir, 'synthesizedData/bookings_complete.csv'))
        self.crosswalk = pd.read_csv(os.path.join(self.data_dir, 'geoCrosswalk/GeoCrossWalkMed.csv'))
        print("Data Loaded")

    def data_cleaning(self):
        """Clean data."""
        self.df_flight['bookings'] = self.df_flight['bookings'].str.replace("[", "")
        self.df_flight['bookings'] = self.df_flight['bookings'].str.replace("]", "")
        self.df_flight['bookings'] = self.df_flight['bookings'].str.replace("'", "")
        self.df_flight['bookings'] = self.df_flight['bookings'].str.replace(" ", "")
        self.df_flight = self.df_flight[self.df_flight['occupancy'] != 0]

        self.df_book['airport_itinerary_out'] = self.df_book['airport_itinerary_out'].str.replace("[", "")
        self.df_book['airport_itinerary_out'] = self.df_book['airport_itinerary_out'].str.replace("]", "")
        self.df_book['airport_itinerary_out'] = self.df_book['airport_itinerary_out'].str.replace("'", "")
        self.df_book['airport_itinerary_out'] = self.df_book['airport_itinerary_out'].str.replace(" ", "")
        self.df_book['airport_itinerary_out'] = self.df_book['airport_itinerary_out'].str.split(",")
        self.df_book['IATA'] = self.df_book['airport_itinerary_out'].str.get(0)
        cw = self.crosswalk[['IATA', 'Currency', 'HH_ISO', 'City']].copy()
        self.df_book = self.df_book.merge(cw, on = 'IATA', how = 'left')
        
        self.df_book['list_of_passengers'] = self.df_book['list_of_passengers'].str.replace("[", "")
        self.df_book['list_of_passengers'] = self.df_book['list_of_passengers'].str.replace("]", "")
        self.df_book['list_of_passengers'] = self.df_book['list_of_passengers'].str.replace("'", "")
        self.df_book['list_of_passengers'] = self.df_book['list_of_passengers'].str.replace(" ", "")
        self.df_book['list_of_passengers'] = self.df_book['list_of_passengers'].str.split(",")
        self.df_book['primary_passengers'] = self.df_book['list_of_passengers'].str.get(0)
        
        self.df_book['outboundFlights'] = self.df_book['outboundFlights'].str.replace("[", "")
        self.df_book['outboundFlights'] = self.df_book['outboundFlights'].str.replace("]", "")
        self.df_book['outboundFlights'] = self.df_book['outboundFlights'].str.replace("'", "")
        self.df_book['outboundFlights'] = self.df_book['outboundFlights'].str.replace(" ", "")
        self.df_book['outboundFlights'] = self.df_book['outboundFlights'].str.split(",")
        
        self.df_book['returnFlights'] = self.df_book['returnFlights'].str.replace("[", "")
        self.df_book['returnFlights'] = self.df_book['returnFlights'].str.replace("]", "")
        self.df_book['returnFlights'] = self.df_book['returnFlights'].str.replace("'", "")
        self.df_book['returnFlights'] = self.df_book['returnFlights'].str.replace(" ", "")
        self.df_book['returnFlights'] = self.df_book['returnFlights'].str.split(",")
        
        self.df_book['allFlights'] = self.df_book['outboundFlights'] + self.df_book['returnFlights']
        self.df_book['allFlights'] = self.df_book['allFlights'].apply(lambda x: x if isinstance(x, list) else [])
        strings_to_remove = ['nan', 'na', 'None', 'Null']
        self.df_book['allFlights'] = self.df_book['allFlights'].apply(lambda x: [item for item in x if item not in strings_to_remove])
        self.df_book['allFlights'] = self.df_book['allFlights'].apply(lambda x: [int(item) for item in x])
        self.df_book['allFlights'] = self.df_book['allFlights'].apply(lambda x: [item for item in x if not pd.isna(item)])
        print("Data Cleaned")

    

    def print_flight_stats(self):
        print('Number of Passengers: ', f"{self.df_flight['occupancy'].sum().round().astype(int):,d}")
        print('Number of Unique Aircraft Callsign: ', f"{self.df_flight['callsign'].nunique():,d}")
        print('Number of Airlines ', self.df_flight['IATA_A'].nunique())
        print('Number of Unique Routes: ', f"{self.df_flight['LegAirport_IATA'].nunique():,d}")
        print('Number of Countries: ', self.df_flight['HH_ISO_O'].nunique())
        print('Number of Cities: ', self.df_flight['City_O'].nunique())
        print('Number of Airports: ', self.df_flight['IATA_O'].nunique())
        print('Number of Recorded Days: ', self.df_flight['day'].nunique())
        print('First Record (day): ', self.df_flight['day'].min())
        print('Last Record (day): ', self.df_flight['day'].max())

    def preprocess_data(self, df, index_col):
        """
        Converts a dataframe into a dictionary for faster access.

        Parameters:
        df (pandas.DataFrame): The dataframe to be converted.
        index_col (str): The column name to use as the key in the dictionary.

        Returns:
        dict: A dictionary with keys from the specified index column and values as row data.
        """
        df.fillna('NaN', inplace = True)
        return df.set_index(index_col).T.to_dict()
    
    def build_pnr(self, n_flight=10):
        """Build PNR XML."""
        self.data_cleaning()
        self.print_flight_stats()
        self.df_HH_Nat = self.df_HH[['HHID','NationalityNat']]
        self.df_P = pd.merge(self.df_P, self.df_HH_Nat, on='HHID', how = 'left')   

        flight_data = self.preprocess_data(self.df_flight, 'flight_id')
        book_data = self.preprocess_data(self.df_book, 'init_id')
        p_data = self.preprocess_data(self.df_P, 'P_ID')
        flight_ids = self.df_flight['flight_id'].tolist()
        # Parallel(n_jobs=6)(delayed(bxml.PNRGeneration)(i, flight_data, book_data, p_data) for i in flight_ids)
        self.error_flight_ids = []
        self.built_flight_ids = []
        self.start_time = time.time()
        for i in flight_ids:
            try:
                # print(i)
                bxml.PNRGeneration(i, flight_data, book_data, p_data)
                self.built_flight_ids.append(i)
            except Exception as e:
                # print(f"Error processing flight_id {i}: {e}")
                self.error_flight_ids.append(i)
                # Optionally, log more information or handle the exception in other ways

        # Calculate the total time taken
        duration_without_parallel = time.time() - self.start_time
        print(f"Duration without parallel processing: {duration_without_parallel} seconds")

        # Print or handle error_flight_ids as needed
        print("Flight IDs: ", len(flight_ids))
        print("Built Flight IDs: ", len(self.built_flight_ids))
        print("Error Flight IDs: ", len(self.error_flight_ids))
        
        print("PNR XML Built")

build_xml = BuildXML('data')
build_xml.build_pnr()
    
