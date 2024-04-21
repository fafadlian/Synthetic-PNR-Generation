import pandas as pd
import numpy as np


def process_row(row, df_flight, date_key):
    ID = row['init_id']
    numLeg = int(row['Leg_fixRoute'])
    initDT = row[date_key]
    numPrty = row['num_in_party']

    chosenFlights = []
    nextFlight = None
    for i in range(numLeg):
        LEG = row['full_routes'][i]
        chosenFlight, nextFlight = search_flight(ID, df_flight, initDT, LEG, numPrty, nextFlight, update_df=False)
        chosenFlights.append(chosenFlight)
    
    return chosenFlights

def complete_routing(sublist):
    return [f"{sublist[i]}-{sublist[i+1]}" for i in range(len(sublist)-1)] if len(sublist) >= 2 else []

def outbound_itinerary(df, df_flight, date_key='outboundDate'):
    # Apply function to collect flight selections
    df['outboundFlights'] = df.apply(lambda row: process_row(row, df_flight, date_key), axis=1)

    # Update df_flight based on the collected flight selections
    for index, row in df.iterrows():
        for flight_id in row['outboundFlights']:
            if not pd.isna(flight_id):
                df_flight.loc[df_flight['flight_id'] == flight_id, 'occupancy'] += row['num_in_party']
                
                # Correctly append to the 'bookings' list
                bookings = df_flight.at[df_flight[df_flight['flight_id'] == flight_id].index[0], 'bookings']
                if not isinstance(bookings, list):
                    bookings = []
                bookings.append(row['init_id'])
                df_flight.at[df_flight[df_flight['flight_id'] == flight_id].index[0], 'bookings'] = bookings

    df_flight['outPNRs'] = df_flight['bookings'].apply(len)

    return df, df_flight

def return_itinerary(df, df_flight, date_key='returnDate'):
    # Reverse the outbound itinerary to create the return itinerary
    df['airport_itinerary_ret'] = df['airport_itinerary_out'].apply(lambda x: x[::-1])
    df['return_routes'] = df['airport_itinerary_ret'].apply(complete_routing)

    # Apply the function to each row to collect return flight selections
    df['returnFlights'] = df.apply(lambda row: process_row(row, df_flight, date_key), axis=1)

    # Update df_flight based on the collected flight selections
    for index, row in df.iterrows():
        for flight_id in row['returnFlights']:
            if not pd.isna(flight_id):
                df_flight.loc[df_flight['flight_id'] == flight_id, 'occupancy'] += row['num_in_party']
                

                # Correctly append to the 'bookings' list
                bookings = df_flight.at[df_flight[df_flight['flight_id'] == flight_id].index[0], 'bookings']
                if not isinstance(bookings, list):
                    bookings = []
                bookings.append(row['init_id'])
                df_flight.at[df_flight[df_flight['flight_id'] == flight_id].index[0], 'bookings'] = bookings

    df_flight['retPNRs'] = df_flight['bookings'].apply(len)

    return df, df_flight

import pandas as pd

def search_flight(ID, df_flight, date, leg, num_party, time=None, update_df=True):
    """
    Search for a flight that fits the criteria and optionally update flight data.

    Parameters:
    - ID: Identifier for the booking.
    - df_flight: DataFrame containing flight data.
    - date: Date of the flight.
    - leg: The leg (route) of the flight.
    - num_party: Number of passengers in the booking party.
    - time: Optional time to filter flights that start within an 8-hour window.
    - update_df: Flag to indicate whether to update the DataFrame with booking info.

    Returns:
    - Tuple of the chosen flight ID and the timestamp of the next flight.
    """
    # Filter flights by day and leg
    filtered_flights = df_flight[(df_flight['LegAirport_IATA'] == leg) & (df_flight['day'] == date)]

    # If a specific time is provided, narrow down to flights within an 8-hour window starting from 'time'
    if time is not None:
        end_time = time + pd.to_timedelta(8, 'hours')
        filtered_flights = filtered_flights[(filtered_flights['firstseen'] >= time) & (filtered_flights['firstseen'] <= end_time)]

    # Further filter by available capacity
    available_flights = filtered_flights[filtered_flights['capacity'] - filtered_flights['occupancy'] >= num_party]

    # Choose a flight based on weighted probability proportional to remaining capacity
    if not available_flights.empty:
        probabilities = available_flights['capacity'] / available_flights['capacity'].sum()
        selected_flight = available_flights.sample(weights=probabilities).iloc[0]
        chosen_flight_id = selected_flight['flight_id']
        next_flight_time = pd.Timestamp(selected_flight['lastseen'])

        # Update the flight data if required
        if update_df:
            df_flight.loc[df_flight['flight_id'] == chosen_flight_id, 'occupancy'] += num_party
            bookings_list = df_flight.loc[df_flight['flight_id'] == chosen_flight_id, 'bookings'].item()
            bookings_list.append(ID)
            df_flight.at[df_flight['flight_id'] == chosen_flight_id, 'bookings'] = bookings_list

    else:
        chosen_flight_id = None  # Using None instead of float("NAN") for clarity
        next_flight_time = pd.NaT

    return chosen_flight_id, next_flight_time
