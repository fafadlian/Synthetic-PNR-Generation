from faker import Faker
import random
import datetime

# Fallback function for locale
def safe_locale_gen(locale):
    try:
        return Faker(locale)
    except:
        return Faker('en_US')  # Fallback to 'en_US' if locale is invalid
    

def household_identity(row, available_langs, nationality_list):
    # Decide on language and nationality based on probability
    if random.random() < 0.2:
        lang_P = random.choice(available_langs)
        Nat = random.choice(nationality_list)
    else:
        lang_P = row['Lang']
        Nat = row['HH_ISO']
    
    # Decide on language based on country
    if row['HH_ISO'] == 'CHE':
        lang = random.choices(['fr_CH', 'de_CH'], weights=[0.5, 0.5])[0]
    elif row['HH_ISO'] == 'BEL':
        lang = random.choices(['fr_BE', 'nl_BE'], weights=[0.5, 0.5])[0]
    else:
        lang = row['Lang']

    # Use same Faker instance if languages match to optimize performance
    if lang == lang_P:
        locale_gen = locale_P = safe_locale_gen(lang)
    else:
        locale_gen = safe_locale_gen(lang)
        locale_P = safe_locale_gen(lang_P)

    return {
        'Lang_P': lang_P,
        'Surname': locale_P.last_name(),
        'Address': locale_gen.address(),
        'PostCode': locale_gen.postcode(),
        'Country': row['HH_ISO'],
        'NationalityLP': lang_P,
        'NationalityNat': Nat
    }

def individual_identity(row, available_langs):
    # Assuming locale_gen is correctly initialized based on row['Lang']
    # ... initialization code for locale_gen ...
    lang = row['Lang'] if row['Lang'] in available_langs else 'en_US'  # Fallback to 'en_US'

    locale_gen = safe_locale_gen(lang)

    data = {}

    # FirstName
    if row['P_GENDER'] == 'M':
        data['FirstName'] = locale_gen.first_name_male()
    else:
        data['FirstName'] = locale_gen.first_name_female()

    # Date Of Birth
    YOB = 2021 - row['P_AGE']
    data['DOB'] = str(locale_gen.date_between_dates(date_start=datetime.date(YOB, 1, 1),
                                                date_end=datetime.date(YOB, 12, 31)))

    # FreeEmail
    data['FreeEmail'] = "-" if row['P_AGE'] < 10 else locale_gen.free_email()

    # Payment and Work Information
    if row['P_AGE'] < 18:
        data.update({'PaymentInfo_VendorCode': "-", 'PaymentInfo_ExpiryDate': "-", 
                     'PaymentInfo_AccountNbr': "-", 'WorkEmail': "-"})
    elif 18 <= row['P_AGE'] <= 20:
        num = random.choices(['Y', 'N'], [0.75, 0.25])[0]
        if num == 'N':
            data.update({'PaymentInfo_VendorCode': "-", 'PaymentInfo_ExpiryDate': "-", 
                         'PaymentInfo_AccountNbr': "-", 'WorkEmail': "-"})
        else:
            data.update({'PaymentInfo_VendorCode': locale_gen.credit_card_provider(), 
                         'PaymentInfo_ExpiryDate': locale_gen.date_between_dates(
                             date_start=datetime.date(2022, 1, 1), date_end=datetime.date(2027, 1, 1)), 
                         'PaymentInfo_AccountNbr': locale_gen.credit_card_number(), 
                         'WorkEmail': "-"})
    else:
        data.update({'PaymentInfo_VendorCode': locale_gen.credit_card_provider(), 
                     'PaymentInfo_ExpiryDate': locale_gen.date_between_dates(
                         date_start=datetime.date(2022, 1, 1), date_end=datetime.date(2027, 1, 1)), 
                     'PaymentInfo_AccountNbr': locale_gen.credit_card_number(), 
                     'WorkEmail': locale_gen.company_email()})

    # DOCS_ExpiryDate
    data['DOCS_ExpiryDate'] = locale_gen.date_between_dates(
        date_start=datetime.date(2022, 1, 1), date_end=datetime.date(2027, 1, 1))

    return data

def introduce_typos(text, typo_rate):
    typo_text = list(text)
    for i in range(len(typo_text)-1):
        if random.random() < typo_rate:
            # Introduce a typo (e.g., swap with the next character)
            typo_text[i], typo_text[i+1] = typo_text[i+1], typo_text[i]
    return ''.join(typo_text)

def introduce_dob_typos2(dob, typo_rate):
    dob_parts = dob.split("/")
    old_dob = dob
    day = dob_parts[0]
    month = dob_parts[1]
    year = dob_parts[2]
    
    if random.random() < typo_rate:
        year = list(year)
        digit_to_change = random.randint(0, len(year) - 1)
        year[digit_to_change] = str(random.randint(0, 9))
        year = "".join(year)

    if random.random() < typo_rate:
        month = list(month)
        digit_to_change = random.randint(0, len(month) - 1)
        month[digit_to_change] = str(random.randint(0, 1))
        month = "".join(month)

    if random.random() < typo_rate:
        day = list(day)
        digit_to_change = random.randint(0, len(day) - 1)
        day[digit_to_change] = str(random.randint(0, 3))
        day = "".join(day)
        
    try:
        new_date = datetime(int(year), int(month), int(day))
    except ValueError:
        # Invalid date, correct it to a valid date
        year = dob_parts[0]
        month = dob_parts[1]
        day = dob_parts[2]
    if  (int(year) > 2100 or int(year) < 1900):
        year = dob_parts[0]

    return f"{year}-{month}-{day}"

def docIDs(row):

    data={}
    random_number = random.random()
    if random_number > 0.005:
        data['TYP_FirstName'] = row['DOC_FirstName']
        data['TYP_Surname'] = row['DOC_Surname']
        # data['TYP_DOB'] = str(row['DOB'])
    else:
        data['TYP_FirstName'] = introduce_typos(row['DOC_FirstName'], typo_rate=0.2)
        data['TYP_Surname'] = introduce_typos(row['DOC_Surname'], typo_rate=0.2)
        # data['TYP_DOB'] = introduce_dob_typos2(str(row['DOB']), typo_rate=0.2)
    
    return data

