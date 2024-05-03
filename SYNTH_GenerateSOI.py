import pandas as pd
import numpy as np
from faker import Faker
from random import choice, choices, randrange
from joblib import Parallel, delayed
from unidecode import unidecode
from datetime import datetime, timedelta
import os
import ast

from src import BOOK_GenBehaviour as bkbeh
from src import BOOK_Grouping as bkgroup
from src import BOOK_Trip as bktrip
from src import BOOK_SOIGrouping as bksoi
from src import FLY_Booking as fb

class POI_Generation:
        
    def __init__(self, data_dir, hh_count, num_core):
        self.data_dir = data_dir
        self.hh_count = hh_count
        self.num_core = num_core

        self.load_data()
        print("Group and Booking Initialized")


    def load_data(self):
        self.poi_base = pd.read_csv(os.path.join(self.data_dir, 'POI_base.csv'))
        self.df_flight = pd.read_csv(os.path.join(self.data_dir, 'flightData/EU_flight_new.csv'))
        self.route = pd.read_csv(os.path.join(self.data_dir, 'flightData/route_all.csv'))
        self.crosswalk = pd.read_csv(os.path.join(self.data_dir, 'geoCrosswalk/GeoCrossWalkMed.csv'))
        self.agencies = pd.read_csv(os.path.join(self.data_dir, 'agencies.csv'))['agencies'].tolist()
        self.agency_weight = pd.read_csv(os.path.join(self.data_dir, 'agencies.csv'))['agency_weight'].tolist()
        print("Data Loaded")

    def list_in_column(self, df, column):
        df[column] = df[column].str.replace("[", "")
        df[column] = df[column].str.replace("]", "")
        df[column] = df[column].str.replace("'", "")
        df[column] = df[column].str.replace(" ", "")
        df[column] = df[column].str.split(',')
        return df.copy()
    
    def safe_locale_gen(self, locale):
        try:
            return Faker(locale)
        except AttributeError:
            return Faker('en')

    def generate_household(self, hh_num, df):
        hh_list = []
        for i in range(hh_num):
            hh = {}
            hh['HH_num'] = i
            SOI_Type_List = df['SOI_Type'].dropna().unique().tolist()
            if not SOI_Type_List:
                continue
            SOI = choice(SOI_Type_List)
            row = df[df['SOI_Type'] == SOI]
            if row.empty:
                continue
            
            # Checking and converting the MF_Prob values
            if not row['MF_Prob'].isna().any():
                sex = ['M', 'F']
                # Check if the data is string and needs parsing
                probs = [float(x) for x in (ast.literal_eval(row['MF_Prob'].values[0]) if isinstance(row['MF_Prob'].values[0], str) else row['MF_Prob'].values[0])]
                hh['GenderHOH'] = choices(sex, probs)[0] if len(sex) == len(probs) else ''

            # Ensure AgeHOH values are integers and not NaN
            if not row['AgeMin_Main'].isna().any() and not row['AgeMax_Main'].isna().any():
                hh['AgeHOH'] = randrange(int(row['AgeMin_Main'].values[0]), int(row['AgeMax_Main'].values[0]) + 1)

            # Checking and converting the SizeHH and SizeHH_Prob values
            if not row['SizeHH'].isna().any() and not row['SizeHH_Prob'].isna().any():
                sizes = [int(x) for x in row['SizeHH'].values[0]]
                probs = [float(x) for x in (ast.literal_eval(row['SizeHH_Prob'].values[0]) if isinstance(row['SizeHH_Prob'].values[0], str) else row['SizeHH_Prob'].values[0])]
                hh['SizeHH'] = choices(sizes, probs)[0] if len(sizes) == len(probs) else ''

            # Simplify remaining attribute assignments
            hh['HHID'] = SOI + '_' + str(i)
            hh['HH_ISO'] = choice(row['HH_ISO_List'].dropna().values[0]) if not row['HH_ISO_List'].isna().all() else ''
            hh['HHType'] = SOI
            hh['Lang'] = choice(row['Lang_List'].dropna().values[0]) if not row['Lang_List'].isna().all() else ''
            faker_gen = self.safe_locale_gen(hh['Lang'])
            
            
            hh.update({
                'Address': faker_gen.address(),
                'PostCode': faker_gen.postcode(),
                'Country': faker_gen.current_country(),
                'PaymentInfo_VendorCode': faker_gen.credit_card_provider(),
                'PaymentInfo_ExpiryDate': faker_gen.credit_card_expire(start="now", end="+10y", date_format="%d/%m/%y"),
                'PaymentInfo_AccountNbr': faker_gen.credit_card_number(card_type=None)
            })
            hh['ISO_Travel'] = ''
            hh['IATA_O'] = choice(row['IATA_O_List'].dropna().values[0]) if not row['IATA_O_List'].isna().all() else ''
            hh_list.append(hh)
        
        return pd.DataFrame(hh_list)
    
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


    def populate_persons(self, household_row, df_city, poi_base):
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
        ISO_Travel = household_row['ISO_Travel']
        IATA_O = household_row['IATA_O']    
        faker_gen = self.safe_locale_gen(lang)
        row_HTV = poi_base[poi_base['SOI_Type'] == 'HTV']
        row_group = poi_base[poi_base['SOI_Type'] == HHType]

        for j in range(household_row['SizeHH']):
            P_num = j
            P_ID = f"{HHID}_{j+1}"
            if HHType == 'HTP' or j == 0:
                age = base_age
                gender = household_row['GenderHOH']
            elif HHType == 'HTP' and j > 0:
                age = randrange(int(row_HTV['AgeMin_Main'].values[0]), int(row_HTV['AgeMax_Main'].values[0]) + 1)
                sex_list = ['M', 'F']
                probs = [float(x) for x in (ast.literal_eval(row_HTV['MF_Prob'].values[0]) if isinstance(row_HTV['MF_Prob'].values[0], str) else row_group['MF_Prob'].values[0])]
                gender =  choices(sex_list, probs)[0] if len(sex_list) == len(probs) else ''
            else:
                age = randrange(int(row_group['AgeMin_Main'].values[0]), int(row_group['AgeMax_Main'].values[0]) + 1)
                sex_list = ['M', 'F']
                probs = [float(x) for x in (ast.literal_eval(row_group['MF_Prob'].values[0]) if isinstance(row_group['MF_Prob'].values[0], str) else row_group['MF_Prob'].values[0])]
                gender =  choices(sex_list, probs)[0] if len(sex_list) == len(probs) else ''

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
                doc_first_name, doc_surname, typ_first_name, typ_surname, NationalityNat, ISO_Travel, IATA_O
                ]
            passengers.append(passenger)
        
        return passengers
    
    def generate_person_data(self, df_HH, df_city, poi_base_clean):
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
            passengers = self.populate_persons(row, df_city, poi_base_clean)
            passenger_data.extend(passengers) 
        
        columns = [
            'P_num', 'HHID', 'P_ID', 'P_AGE', 'AgeRange', 'AgeGroup', 'P_GENDER', 'GenderHOH', 'AgeHOH', 'SizeHH', 'HH_ISO', 'HHType', 'Lang',
            'Surname', 'Address', 'PostCode', 'Country', 'FirstName', 'DOB', 'FreeEmail',
            'PaymentInfo_VendorCode', 'PaymentInfo_ExpiryDate', 'PaymentInfo_AccountNbr',
            'WorkEmail', 'DOCS_ExpiryDate', 'DOC_FirstName', 'DOC_Surname', 'TYP_FirstName',
            'TYP_Surname', 'NationalityNat', 'ISO_Travel', 'IATA_O'
        ]
        df_passengers = pd.DataFrame(passenger_data, columns=columns)
        
        return df_passengers

    def finalize_data(df):
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
        poi_base_clean = self.poi_base.copy()
        for column in ['HH_ISO_List', 'SizeHH', 'SizeHH_Prob', 'IATA_O_List', 'IATA_D_List', 'Lang_List', 'stay_day', 'stay_day_weight']:
            poi_base_clean = self.list_in_column(poi_base_clean, column)
        

        df_HH = self.generate_household(self.hh_count, poi_base_clean)
        df_passengers = self.generate_person_data(df_HH, self.crosswalk, poi_base_clean)
        df_behaviour_complete = bkbeh.generate_behaviour(df_HH, self.df_flight, ['SOI'], [1], 3, self.crosswalk, 1)        
        select_behaviour = 'Behaviour_1'
        df_group = bksoi.grouping_init(df_behaviour_complete, select_behaviour, self.agencies, self.agency_weight, self.route, poi_base_clean, 1)

        print("Saving SOI/POI data...")
        df_HH.to_csv(os.path.join(self.data_dir, 'synthesizedData/HH_SOI.csv'), index=False)
        df_passengers.to_csv(os.path.join(self.data_dir, 'synthesizedData/person_SOI.csv'), index=False)
        df_group.to_csv(os.path.join(self.data_dir, 'synthesizedData/group_SOI.csv'), index=False)
        return df_passengers

