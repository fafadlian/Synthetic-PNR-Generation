import pandas as pd
import numpy as np
import os

from src import FLY_Booking as fb


class FlightBookingSystem:
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.load_data()
        print("Group and Booking Initialized")

    def load_data(self):
        """Load data from csv files."""
        self.df_flight = pd.read_csv(os.path.join(self.data_dir, 'flightData/EU_flight_new.csv'))
        self.df_book = pd.read_csv(os.path.join(self.data_dir, 'synthesizedData/group.csv'))
        self.route = pd.read_csv(os.path.join(self.data_dir, 'flightData/route_all.csv'))
        self.clean_data()

    def clean_data(self):
        """Clean and prepare initial data."""
        self.df_flight.rename({'Unnamed: 0': 'flight_id'}, axis=1, inplace=True)
        self.df_flight['occupancy'] = 0
        self.df_flight['firstseen'] = pd.to_datetime(self.df_flight['firstseen'], utc=True)
        self.df_flight['lastseen'] = pd.to_datetime(self.df_flight['lastseen'], utc=True)
        self.df_flight['day'] = pd.to_datetime(self.df_flight['day'], utc=True)
        self.df_flight['bookings'] = [[] for _ in range(len(self.df_flight))]
        self.df_book = self.clean_list_column(self.df_book, 'airport_itinerary_out')
        self.df_book = self.clean_list_column(self.df_book, 'full_routes')
        self.df_book = self.initID_clean(self.df_book)

    def clean_list_column(self, df_hubs, column):
        df_hubs[column] = df_hubs[column].str.replace("[", "")
        df_hubs[column] = df_hubs[column].str.replace("]", "")
        df_hubs[column] = df_hubs[column].str.replace("'", "")
        df_hubs[column] = df_hubs[column].str.replace(" ", "")
        df_hubs[column] = df_hubs[column].str.split(",")
        return df_hubs
    
    def initID_clean(self, df):
        """Extract travel purpose and IDX from 'initID' and ensure unique identifiers."""
        df[['Purpose', 'IDX']] = df['init_id'].str.extract(r'([^-]+)-(ID\d+)$')
        df['UniqueCounter'] = df.groupby(['IATA_O', 'IATA_D', 'Purpose', 'IDX']).cumcount() + 1
        df['New_IDX'] = df.apply(lambda x: f"{x['IDX']}-{x['UniqueCounter']}" if x['UniqueCounter'] > 1 else x['IDX'], axis=1)
        df['init_id'] = df['Origin'] + '-' + df['Destination'] + '-' + df['Purpose'] + '-' + df['New_IDX']
        df.drop(['Purpose', 'IDX', 'UniqueCounter', 'New_IDX'], axis=1, inplace=True)
        return df
    
    def outbound_day(self):
        """Assigns outbound and return dates based on flight capacities and probabilities."""
        flight_sched = self.df_flight.groupby(['day'])['capacity'].sum().reset_index()
        flight_sched['prob'] = flight_sched['capacity'] / flight_sched['capacity'].sum()
        days = flight_sched['day'].to_numpy()
        probs = flight_sched['prob'].to_numpy()
        self.df_book['outboundDate'] = np.random.choice(days, size=len(self.df_book), p=probs)
        self.df_book['returnDate'] = pd.to_datetime(self.df_book['outboundDate']) + pd.to_timedelta(self.df_book['stay_day'], unit='D')
    
    def run_analysis(self):
        """Run analysis for all groups."""
        self.outbound_day()
        self.df_book, self.df_flight = fb.outbound_itinerary(self.df_book, self.df_flight)
        self.df_book, self.df_flight = fb.return_itinerary(self.df_book, self.df_flight)
        self.df_book.to_csv(os.path.join(self.data_dir, 'synthesizedData/bookings_complete.csv'), index=False)
        self.df_flight.to_csv(os.path.join(self.data_dir, 'synthesizedData/flights_complete.csv'), index=False)
        print("Analysis complete")

flight_booking = FlightBookingSystem(data_dir='data')
flight_booking.run_analysis()
