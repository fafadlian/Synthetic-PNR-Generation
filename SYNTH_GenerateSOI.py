import pandas as pd
import numpy as np
from faker import Faker
from random import choice, choices, randrange
from joblib import Parallel, delayed
from unidecode import unidecode
from datetime import datetime, timedelta
import os

from src import BOOK_GenBehaviour as bkbeh
from src import BOOK_Grouping as bkgroup
from src import BOOK_Trip as bktrip
from src import FLY_Booking as fb

class POI_Generation:
        
    def __init__(self, data_dir, hh_count, num_core):
        self.data_dir = data_dir
        self.hh_count = hh_count
        self.num_core = num_core

        self.load_data()
        print("Group and Booking Initialized")

    def load_data(self):
        self.df_city = pd.read_csv(os.path.join(self.data_dir, 'geoCrosswalk/GeoCrossWalkMed.csv'))
        self.df_flight = pd.read_csv(os.path.join(self.data_dir, 'flightData/EU_flight_new.csv'))
        self.df_hubs = pd.read_csv(os.path.join(self.data_dir, 'geoCrosswalk/ReverseHubsV2.csv'))
        self.route = pd.read_csv(os.path.join(self.data_dir, 'flightData/route_all.csv'))
        self.crosswalk = pd.read_csv(os.path.join(self.data_dir, 'geoCrosswalk/GeoCrossWalkMed.csv'))
        self.bus_stay_day = pd.read_csv(os.path.join(self.data_dir, 'business_stay.csv'))['bus_stay_day'].tolist()
        self.bus_stay_weight = pd.read_csv(os.path.join(self.data_dir, 'business_stay.csv'))['bus_stay_weight'].tolist()
        self.vac_stay_day = pd.read_csv(os.path.join(self.data_dir, 'vacation_stay.csv'))['vac_stay_day'].tolist()
        self.vac_stay_weight = pd.read_csv(os.path.join(self.data_dir, 'vacation_stay.csv'))['vac_stay_weight'].tolist()
        self.personas = pd.read_csv(os.path.join(self.data_dir, 'personas.csv'))['personas'].tolist()
        self.weight = pd.read_csv(os.path.join(self.data_dir, 'personas.csv'))['weight'].tolist()
        self.agencies = pd.read_csv(os.path.join(self.data_dir, 'agencies.csv'))['agencies'].tolist()
        self.agency_weight = pd.read_csv(os.path.join(self.data_dir, 'agencies.csv'))['agency_weight'].tolist()
        print("Data Loaded")
    
    def safe_locale_gen(self, locale):
        try:
            return Faker(locale)
        except AttributeError:
            return Faker('en')

    def generate_households(self, num_households, df_city):
        """
        Generate synthetic household data for flight passenger simulation.
        
        Parameters:
        - num_households: int, number of households to generate.
        - df_city: DataFrame, contains city data including available languages.
        - available_langs: list, languages available for selection.
        
        Returns:
        - DataFrame with household information.
        """

        households = []
        available_langs = df_city['Lang'].tolist()
        for i in range(num_households):
            HH_ISO = choice(df_city['HH_ISO'].tolist())
            HHID = f"POI_{i}"
            gender = choice(["M", "F"])
            HHType = choice(["T1", "T2"]) #T1: children travels alone, T2:children travels with adult with different surnames
            age = randrange(5, 17) if HHType == 'T1' else randrange(28, 56)
            sizeHH = 1 if HHType == 'T1' else 2
            
            lang = choice(available_langs) if np.random.random() < 0.2 else 'en'  # Simplified language logic
            faker_gen = self.safe_locale_gen(lang)
            
            # surname = faker_gen.last_name()
            address = faker_gen.address()
            postcode = faker_gen.postcode()
            country = faker_gen.country()
            payment_vendor = faker_gen.credit_card_provider()
            payment_expiry = faker_gen.credit_card_expire(start="now", end="+10y", date_format="%d/%m/%y")
            payment_number = faker_gen.credit_card_number(card_type=None)
            
            households.append([i, gender, age, sizeHH, HHID, HH_ISO, HHType, lang, address, postcode, country, payment_vendor, payment_expiry, payment_number])
        
        columns = ['HH_num', 'GenderHOH', 'AgeHOH', 'SizeHH', 'HHID', 'HH_ISO', 'HHType', 'Lang', 'Address', 'PostCode', 'Country', 'PaymentInfo_VendorCode', 'PaymentInfo_ExpiryDate', 'PaymentInfo_AccountNbr']
        return pd.DataFrame(households, columns=columns)
    def generate_dob(self, age):
        """
        Generate a Date of Birth for the given age.
        """
        today = datetime.today()
        start_of_year = datetime(today.year - age, 1, 1)
        end_of_year = datetime(today.year - age, 12, 31)
        random_days = timedelta(days=(end_of_year - start_of_year).days * np.random.random())
        dob = start_of_year + random_days
        return dob.strftime('%d/%m/%Y')


    def generate_typ_names(self, faker_gen, doc_first_name, doc_surname):
        """
        Generate typology names with a 20% chance of being different from the document names.
        """
        if np.random.rand() < 0.5:  # 20% chance
            typ_first_name = faker_gen.first_name()
            typ_surname = faker_gen.last_name()
        else:
            typ_first_name = doc_first_name
            typ_surname = doc_surname
        return typ_first_name, typ_surname


    def populate_passengers(self, household_row, df_city):
        """
        Populate passengers for a given household, ensuring diversity and generating comprehensive passenger attributes.
        Now includes a 20% chance for typology names to differ from document names.
        
        Parameters:
        - household_row: Series, a row from the household DataFrame.
        - df_city: DataFrame, contains city data for determining locales.
        
        Returns:
        - List of comprehensive passenger data for the household.
        """
        passengers = []
        HHID = household_row['HHID']
        HHType = household_row['HHType']
        base_age = household_row['AgeHOH']
        lang = household_row['Lang']
        payment_vendor = household_row['PaymentInfo_VendorCode']
        payment_expiry = household_row['PaymentInfo_ExpiryDate']
        payment_number = household_row['PaymentInfo_AccountNbr']    
        faker_gen = self.safe_locale_gen(lang)
        for j in range(household_row['SizeHH']):
            
            P_num = j
            P_ID = f"{HHID}_{j+1}"
            if HHType == 'T1' or j == 0:
                age = base_age
                gender = household_row['GenderHOH']
            else:
                age = randrange(5, 17)
                gender = choice(['M', 'F'])

            first_name = faker_gen.first_name_male() if gender == 'M' else faker_gen.first_name_female()
            surname = faker_gen.last_name()
            dob = self.generate_dob(age)
            free_email = faker_gen.free_email()
            # payment_vendor = faker_gen.credit_card_provider()
            # payment_expiry = faker_gen.credit_card_expire(start="now", end="+10y", date_format="%d/%m/%y")
            # payment_number = faker_gen.credit_card_number(card_type=None)
            work_email = faker_gen.company_email()
            docs_expiry = (datetime.today() + timedelta(days=365 * 10)).strftime('%Y-%m-%d')  # Assuming 10 years from now
            
            doc_first_name = unidecode(first_name)
            doc_surname = unidecode(surname)  # Document names are the real names
            typ_first_name, typ_surname = self.generate_typ_names(faker_gen, doc_first_name, doc_surname)  # Generate TYP names
            NationalityNat = choice(df_city['HH_ISO'].tolist())


            passenger = [
                P_num, HHID, P_ID, age, 
                f"{5 * (age // 5)}-{5 * (age // 5) + 4}" if age < 100 else "100+",  # AgeRange
                f"AGE{age // 5 + 1}" if age < 100 else "AGE21",  # AgeGroup
                gender, household_row['GenderHOH'], base_age, household_row['SizeHH'],
                household_row['HH_ISO'], HHType, lang, surname, household_row['Address'],
                household_row['PostCode'], household_row['Country'], first_name, dob, free_email,
                payment_vendor, payment_expiry, payment_number, work_email, docs_expiry,
                doc_first_name, doc_surname, typ_first_name, typ_surname, NationalityNat
                ]
        # No placeholder for P_num is added here
            passengers.append(passenger)
        
        return passengers

    def generate_passenger_data(self, df_HH, df_city):
        """
        Generate comprehensive passenger data for all households.
        
        Parameters:
        - df_HH: DataFrame, household data.
        - df_city: DataFrame, city data for locale information.
        
        Returns:
        - DataFrame with comprehensive passenger data.
        """
        # passenger_data = Parallel(n_jobs=-1)(delayed(populate_passengers)(row, df_city) for index, row in df_HH.iterrows())
        # passenger_data = [p for sublist in passenger_data for p in sublist]  # Flatten the list of lists

        passenger_data = []
        for index, row in df_HH.iterrows():
            passengers = self.populate_passengers(row, df_city)
            passenger_data.extend(passengers) 
        
        columns = [
            'P_num', 'HHID', 'P_ID', 'P_AGE', 'AgeRange', 'AgeGroup', 'P_GENDER', 'GenderHOH', 'AgeHOH', 'SizeHH', 'HH_ISO', 'HHType', 'Lang',
            'Surname', 'Address', 'PostCode', 'Country', 'FirstName', 'DOB', 'FreeEmail',
            'PaymentInfo_VendorCode', 'PaymentInfo_ExpiryDate', 'PaymentInfo_AccountNbr',
            'WorkEmail', 'DOCS_ExpiryDate', 'DOC_FirstName', 'DOC_Surname', 'TYP_FirstName',
            'TYP_Surname', 'NationalityNat'
        ]
        df_passengers = pd.DataFrame(passenger_data, columns=columns)
        
        return df_passengers

    def finalize_data(self, df):
        """
        Finalize the DataFrame by adding P_num and ensuring the correct column order.
        """
        df.insert(0, 'P_num', range(1, len(df) + 1))  # Insert P_num at the beginning
        column_order = [
            'P_num', 'HHID', 'P_ID', 'P_AGE', 'AgeRange', 'AgeGroup', 'P_GENDER', 'GenderHOH', 'AgeHOH', 'SizeHH', 'HH_ISO', 'HHType', 'Lang',
            'Surname', 'Address', 'PostCode', 'Country', 'FirstName', 'DOB', 'FreeEmail', 'PaymentInfo_VendorCode', 'PaymentInfo_ExpiryDate',
            'PaymentInfo_AccountNbr', 'WorkEmail', 'DOCS_ExpiryDate', 'DOC_FirstName', 'DOC_Surname', 'TYP_FirstName', 'TYP_Surname'
        ]
        return df[column_order]

    # Note: Ensure that df_city and df_HH are properly set up before calling generate_passenger_data.

    def introduce_typos(self, text, typo_rate):
        typo_text = list(text)
        for i in range(len(typo_text)-1):
            if np.random() < typo_rate:
                # Introduce a typo (e.g., swap with the next character)
                typo_text[i], typo_text[i+1] = typo_text[i+1], typo_text[i]
        return ''.join(typo_text)

    def docIDs(self, row):

        data={}
        random_number = np.random()
        if random_number > 0.005:
            data['TYP_FirstName'] = row['DOC_FirstName']
            data['TYP_Surname'] = row['DOC_Surname']
            # data['TYP_DOB'] = str(row['DOB'])
        else:
            data['TYP_FirstName'] = self.introduce_typos(row['DOC_FirstName'], typo_rate=0.2)
            data['TYP_Surname'] = self.introduce_typos(row['DOC_Surname'], typo_rate=0.2)
            # data['TYP_DOB'] = introduce_dob_typos2(str(row['DOB']), typo_rate=0.2)
        
        return data
    
    def generate_POI(self):
        """
        Generate synthetic data for POI.
        
        Parameters:
        - num_households: int, number of households to generate.
        
        Returns:
        - DataFrame with comprehensive POI data.
        """
        df_HH = self.generate_households(self.hh_count, self.df_city)
        df_passengers = self.generate_passenger_data(df_HH, self.df_city)
        df_city, df_HH = bktrip.original_city_assign_init(df_HH, self.df_flight, self.df_hubs, self.num_core)
        df_behaviour_complete = bkbeh.generate_behaviour(df_HH, self.df_flight, ['SOI'], [1], 3, self.crosswalk, self.num_core)
        select_behaviour = 'Behaviour_1'
        df_group = bkgroup.grouping_init(df_behaviour_complete, select_behaviour, self.agencies, self.agency_weight, self.route, self.bus_stay_day, self.bus_stay_weight, self.vac_stay_day, self.vac_stay_weight, self.num_core)


        df_HH.to_csv(os.path.join(self.data_dir, 'synthesizedData/HH_SOI.csv'), index=False)
        df_passengers.to_csv(os.path.join(self.data_dir, 'synthesizedData/person_SOI.csv'), index=False)
        df_group.to_csv(os.path.join(self.data_dir, 'synthesizedData/group_soi.csv'), index=False)
        return df_passengers

