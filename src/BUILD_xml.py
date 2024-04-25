import xml.etree.ElementTree as ET


import time
import json
import pandas as pd
import numpy as np 
import random
from random import choices
from xml.dom import minidom
import os
from os import listdir
from os.path import isfile, join

def PNRGeneration(i, flight_data, book_data, p_data, complete_dir):
    root = ET.Element("IATA_PNRGOV_NotifRQ")      
    flight_info = flight_data[i]

    a1 = ET.Element("Originator")
    a1.set('AirlineCode', flight_info['IATA_A'])
    a1.set('SystemCode', 'SYSTEMCODE')
    a1.set('AirlineContactInfo', 'AIRLINE CONTACT INFO')
    root.append(a1)
    
    a2 = ET.Element('FlightLeg')
    a2.set('CarrierCode', flight_info['IATA_A'])
    a2.set('FlightNumber', flight_info['flightNbr'])
    a2.set('DepartureDateTime', flight_info['firstseen'])
    a2.set('ArrivalDateTime', flight_info['lastseen'])
    a2.set('DateChangeNbr', str(pd.Timedelta(pd.to_datetime(flight_info['lastseen']) - pd.to_datetime(flight_info['firstseen'])).days))
    
    a2_1 = ET.SubElement(a2, 'DepartureAirport')
    a2_1.set('LocationCode', flight_info['IATA_O'])
    a2_2 = ET.SubElement(a2, 'ArrivalAirport')
    a2_2.set('LocationCode', flight_info['IATA_D'])
    root.append(a2)
    
    a3 = ET.Element('PNRs', NumberOfPnrs = str(flight_info['retPNRs']))
    root.append(a3)

    for j in flight_info['bookings'].split(","):
        # print("flight: ", i, " booking :", j)
        if j in book_data:
            booking_info = book_data[j]
            a3_1 = ET.SubElement(a3, 'PNR', NumberOfPassengers = str(booking_info['num_in_party']), 
                                 PNR_TransDate = str(pd.to_datetime(booking_info['outboundDate']) - pd.Timedelta('10 days ')),
                                 PNR_CreationDate = str(pd.to_datetime(booking_info['outboundDate']) - pd.Timedelta('10 days ')),
                                 LastTktDate = str(pd.to_datetime(booking_info['outboundDate']) - pd.Timedelta('10 days ')))
            
            a3_1_1 = ET.SubElement(a3_1, 'BookingRefID', ID=j)
            a3_1_1_1 = ET.SubElement(a3_1_1, 'CompanyName', 
                                     TravelSector = 'TravelSectorCode', 
                                     Code = flight_info['IATA_A'])
                    
            a3_1_2 = ET.SubElement(a3_1, 'POS')
            a3_1_2_1 = ET.SubElement(a3_1_2, 'Source', AgentSine = flight_info['IATA_A'],
                                    PseudoCityCode = booking_info['City'],
                                    ISOCOuntry = booking_info['HH_ISO'],
                                    ISOCurrency = booking_info['Currency'],
                                    AgentDutyCode = "-",
                                    AirlineVendorID = flight_info['IATA_A'],
                                    AirportCode = booking_info['IATA'])
            a3_1_2_1_1 = ET.SubElement(a3_1_2_1, 'RequestorID', Type = str(booking_info['BookingAgency'])[:-1],
                                       ID = "REQUESTOR ID")
            if booking_info['BookingAgency'] == 'Airlines':
                a3_1_2_1_1_1 = ET.SubElement(a3_1_2_1_1, 'CompanyName', Code = flight_info['IATA_A'])
            else:
                a3_1_2_1_1_1 = ET.SubElement(a3_1_2_1_1, 'CompanyName', Code = str(booking_info['BookingAgency']))

            for k in booking_info['list_of_passengers']:
                print("flight: ", i, " booking :", j, " passenger: ", k)
                p_info = p_data[k]
                a3_1_3 = ET.SubElement(a3_1, 'Passenger', RPH=k,
                                        SurnameRefNumber=p_info['HHID'],
                                        BoardingStatus="63")
                a3_1_3_1 = ET.SubElement(a3_1_3, 'GivenName')
                a3_1_3_1.text = p_info['DOC_FirstName'] #change to TYP_FirstName if error
                a3_1_3_2 = ET.SubElement(a3_1_3, 'Surname')
                a3_1_3_2.text = p_info['DOC_Surname'] #change to TYP_Surname if error
                a3_1_3_3 = ET.SubElement(a3_1_3, 'CustLoyalty', 
                                        ProgramID = 'PROGRAM_ID',
                                        MembershipID = j)
                a3_1_3_4 = ET.SubElement(a3_1_3, 'ExcessBaggage', IssuerCode = str(flight_info['IATA_A']),
                                        SerialNumber = k,
                                        SequenceCount = str(1), BaggagePool = "MP", 
                                        UnitOfMeasureQuantity = str(choices([1,2,3], [0.5, 0.3, 0.2])[0]),
                                        UnitOfMeasureCode = "Kg")
                a3_1_3_5 = ET.SubElement(a3_1_3, 'FareInfo', PTC_Code = 'PTC_CODE',
                                        DiscountedFareType = 'DISC_FARE_TYPE',
                                        DiscountrPercent = str(5), CountryCode = str(booking_info['HH_ISO']),
                                        DiscountedFareClassType = 'CABIN_CLASS',
                                        FareBasis = 'SFLY')
                a3_1_3_6 = ET.SubElement(a3_1_3, 'SSR', SSR_Code = 'OTHS',
                                        ServiceQuantity = str(1),Status = 'HN',
                                        BoardPoint = flight_info ['IATA_O'],
                                        OffPoint = flight_info['IATA_D'],
                                        RPH = str(k),
                                        SurnameRefNumber = str(p_info['HHID']))
                a3_1_3_6_1 = ET.SubElement(a3_1_3_6, 'Airline', Code = str(flight_info['IATA_A']))
                a3_1_3_6_2 = ET.SubElement(a3_1_3_6, 'TEXT')
                a3_1_3_6_2.text = 'MADDOX/MOLLY'
                a3_1_3_7 = ET.SubElement(a3_1_3, 'TicketDocument', TicketDocumentNbr = str(i)+"/"+str(j)+"/"+str(k),
                                        Type = 'TICKET_TYPE',
                                        DateOfIssue = str(flight_info['day']),
                                        TicketLocation = flight_info['IATA_O'],
                                        PrimaryDocInd = str(choices(['TRUE', 'FALSE'], [0.7, 0.3])[0]))
                a3_1_3_7_1 = ET.SubElement(a3_1_3_7, 'TotalFare', 
                                        Amount = str(12345),
                                        CurrencyCode = str(booking_info['Currency']))
                a3_1_3_7_2 = ET.SubElement(a3_1_3_7, 'PricingInfo', Date = str(flight_info['day']),
                                        Time = str(booking_info['outboundDate']),
                                        ISOCountryCode = str(booking_info['HH_ISO']),
                                        LocationCode = str(booking_info['IATA']),
                                        NonEndorsableInd = str(choices(['TRUE', 'FALSE'], [0.7, 0.3])[0]),
                                        NonRefundableInd = str(choices(['TRUE', 'FALSE'], [0.7, 0.3])[0]),
                                        PenaltyRestrictionInd = str(choices(['TRUE', 'FALSE'], [0.7, 0.3])[0]))
                a3_1_3_7_3 = ET.SubElement(a3_1_3_7, 'Taxes')
                a3_1_3_7_3_1 = ET.SubElement(a3_1_3_7_3, 'Tax', Amount = str(123),
                                        ISOCountry = str(booking_info['HH_ISO']),
                                        CurrencyCode = str(booking_info['Currency']),
                                        TaxType = "BK")
                primary_passenger = booking_info['primary_passengers']
                a3_1_3_7_4 = ET.SubElement(a3_1_3_7, 'PaymentInfo', PaymentType = 'CC', PaymentUse = 'NEW',
                                           PaymentAmount = str(12345),
                                           VendorCode = str(p_data[primary_passenger]['PaymentInfo_VendorCode']),
                                           AccountNbr = str(p_data[primary_passenger]['PaymentInfo_AccountNbr']),
                                           ExpiryDate = str(p_data[primary_passenger]['PaymentInfo_ExpiryDate']),
                                       CardHolderName = str(p_data[primary_passenger]['FirstName'])+str(p_data[primary_passenger]['Surname']))
                a3_1_3_8 = ET.SubElement(a3_1_3, 'DOC_SSR', SSR_Code = 'DOCA',
                                     ServiceQuantity = str(1),
                                     Status = 'HK')
                a3_1_3_8_1 = ET.SubElement(a3_1_3_8, 'FlightInfo', Code = str(flight_info['IATA_A']))
                a3_1_3_8_2 = ET.SubElement(a3_1_3_8, 'DOCA', AddressType = "R",
                                      Address = str(p_info['Address']),
                                      CityName = str(booking_info['City']),
                                      PostalCode = str(p_info['PostCode']),
                                      StateProvCounty = 'STATE_PROV_COUNTY',
                                      Country = str(p_info['Country']))
                a3_1_3_9 = ET.SubElement(a3_1_3, 'DOC_SSR', SSR_Code = 'DOCO',ServiceQuantity = str(1),Status = 'HK')
                a3_1_3_9_1 = ET.SubElement(a3_1_3_9, 'DOCO', BirthLocation = str(choices([p_info['Country'], p_info['NationalityNat']], [0.5, 0.5])),
                                       TravelDocType = 'V',
                                       TravelDocNbr = str(k),
                                       PlaceOfIssue = str(p_info['NationalityNat']),
                                       DateOfIssue = "-",
                                       CountryState = str(p_info['NationalityNat']))
                a3_1_3_10 = ET.SubElement(a3_1_3, 'DOC_SSR', SSR_Code = 'DOCS',ServiceQuantity = str(1),Status = 'HK')
                a3_1_3_10_1 = ET.SubElement(a3_1_3_10, 'DOCS', DateOfBirth = str(p_info['DOB']),
                                        ExpiryDate = str(p_info['DOCS_ExpiryDate']),
                                        FirstGivenName = str(p_info['DOC_FirstName']),
                                        SecondGivenName = "",
                                        Surname = str(p_info['DOC_Surname']),
                                        Gender = str(p_info['P_GENDER']),
                                        IssuingLoc = str(p_info['NationalityNat']),
                                        PaxNationality = str(p_info['NationalityNat']))
            fly = 1
            for l in  booking_info['allFlights']:
                flight_list = flight_data[l]
                a3_1_4 = ET.SubElement(a3_1, 'Flight', 
                DepartureDateTime= str(flight_list['firstseen']),
                ArrivalDateTime = str(flight_list['lastseen']),
                ResBookDesigCode = "S",
                NumberInParty = str(booking_info['num_in_party']),
                Status = "HK",
                FlightNumber = str(fly))
                a3_1_4_1 = ET.SubElement(a3_1_4, 'DepartureAirport', LocationCode = str(flight_list['IATA_O']))
                a3_1_4_2 = ET.SubElement(a3_1_4, 'ArrivalAirport', LocationCode = str(flight_list['IATA_D']))
                a3_1_4_3 = ET.SubElement(a3_1_4, 'OperatingAirline', Code = str(flight_list['IATA_A']),
                                         FlightNumber = str(flight_list['flightNbr']),
                                         ResBookDesigCode = "S")
                a3_1_4_4 = ET.SubElement(a3_1_4, 'MarketingAirline', Code = "TX")
            
                fly = fly+1

    tree = ET.ElementTree(root)    
    filename = os.path.join(complete_dir, f'{i}.xml')
      
    with open (filename, "wb") as file :
        file.write(ET.tostring(root,encoding='UTF-8'))