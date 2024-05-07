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

    Originator = ET.Element("Originator")
    Originator.set('AirlineCode', flight_info['IATA_A'])
    Originator.set('SystemCode', 'SYSTEMCODE')
    Originator.set('AirlineContactInfo', 'AIRLINE CONTACT INFO')
    root.append(Originator)
    
    FlightLeg = ET.Element('FlightLeg')
    FlightLeg.set('CarrierCode', flight_info['IATA_A'])
    FlightLeg.set('FlightNumber', flight_info['flightNbr'])
    FlightLeg.set('DepartureDateTime', flight_info['firstseen'])
    FlightLeg.set('ArrivalDateTime', flight_info['lastseen'])
    FlightLeg.set('DateChangeNbr', str(pd.Timedelta(pd.to_datetime(flight_info['lastseen']) - pd.to_datetime(flight_info['firstseen'])).days))
    
    DepartureAirport = ET.SubElement(FlightLeg, 'DepartureAirport')
    DepartureAirport.set('LocationCode', flight_info['IATA_O'])
    ArrivalAirport = ET.SubElement(FlightLeg, 'ArrivalAirport')
    ArrivalAirport.set('LocationCode', flight_info['IATA_D'])
    root.append(FlightLeg)
    
    PNRs = ET.Element('PNRs', NumberOfPnrs = str(flight_info['retPNRs']))
    root.append(PNRs)

    for j in flight_info['bookings'].split(","):
        # print("flight: ", i, " booking :", j)
        if j in book_data:
            booking_info = book_data[j]
            PNR = ET.SubElement(PNRs, 'PNR', NumberOfPassengers = str(booking_info['num_in_party']), 
                                 PNR_TransDate = str(pd.to_datetime(booking_info['outboundDate']) - pd.Timedelta('10 days ')),
                                 PNR_CreationDate = str(pd.to_datetime(booking_info['outboundDate']) - pd.Timedelta('10 days ')),
                                 LastTktDate = str(pd.to_datetime(booking_info['outboundDate']) - pd.Timedelta('10 days ')))
            
            #BookingRefID
            BookingRefID = ET.SubElement(PNR, 'BookingRefID', ID=j)
            BookingRefID_details = ET.SubElement(BookingRefID, 'CompanyName', 
                                     TravelSector = 'TravelSectorCode', 
                                     Code = flight_info['IATA_A'])

            #POS        
            POS = ET.SubElement(PNR, 'POS')
            Source = ET.SubElement(POS, 'Source', AgentSine = flight_info['IATA_A'],
                                    PseudoCityCode = booking_info['City'],
                                    ISOCOuntry = booking_info['HH_ISO'],
                                    ISOCurrency = booking_info['Currency'],
                                    AgentDutyCode = "-",
                                    AirlineVendorID = flight_info['IATA_A'],
                                    AirportCode = booking_info['IATA'])
            POS_Source_details = ET.SubElement(Source, 'RequestorID', Type = str(booking_info['BookingAgency'])[:-1],
                                       ID = "REQUESTOR ID")
            if booking_info['BookingAgency'] == 'Airlines':
                CompanyName = ET.SubElement(POS_Source_details, 'CompanyName', Code = flight_info['IATA_A'])
            else:
                CompanyName = ET.SubElement(POS_Source_details, 'CompanyName', Code = str(booking_info['BookingAgency']))

            primary_passenger = booking_info['primary_passengers']

            #ContactInfo
            PNR_ContactInfo = ET.SubElement(PNR, 'ContactInfo', PhoneNumber = str("-"), PhoneType = str("4"), EmailAddress = str(p_data[primary_passenger]['FreeEmail']))
            StreetNmbr = ET.SubElement(PNR_ContactInfo, 'StreetNmbr', StreetNmbrSuffix = str(123), StreetDirection = str("N/"))
            AddressLine = ET.SubElement(PNR_ContactInfo, 'AddressLine')
            AddressLine.text = str(p_data[primary_passenger]['Address'])
            CityName = ET.SubElement(PNR_ContactInfo, 'CityName')
            CityName.text = str("-")
            PostalCode = ET.SubElement(PNR_ContactInfo, 'PostalCode')
            PostalCode.text = str(p_data[primary_passenger]['PostCode'])
            StateProv = ET.SubElement(PNR_ContactInfo, 'StateProv')
            StateProv.text = str("-")
            CountryName = ET.SubElement(PNR_ContactInfo, 'CountryName')
            CountryName.text = str(p_data[primary_passenger]['Country'])

            #PrepaidBag
            PrepaidBag = ET.SubElement(PNR, 'PrepaidBag', UnitOfMeasureQuantity = str(booking_info['num_luggage']), 
                                       UnitOfMeasureCode = str(700), Amount = str(booking_info['total_luggage_weight']),
                                       CurrencyCode = str(booking_info['Currency']),
                                       BagDestination = str(flight_info['IATA_D']))


            #Passenger
            for k in booking_info['list_of_passengers']:
                print("flight: ", i, " booking :", j, " passenger: ", k)
                p_info = p_data[k]
                Passenger = ET.SubElement(PNR, 'Passenger', RPH=k,
                                        SurnameRefNumber=p_info['HHID'],
                                        BoardingStatus="63")
                GivenName = ET.SubElement(Passenger, 'GivenName')
                GivenName.text = p_info['DOC_FirstName'] #change to TYP_FirstName if error
                Surname = ET.SubElement(Passenger, 'Surname')
                Surname.text = p_info['DOC_Surname'] #change to TYP_Surname if error
                CustLoyalty = ET.SubElement(Passenger, 'CustLoyalty', 
                                        ProgramID = 'PROGRAM_ID',
                                        MembershipID = j)
                ExcessBaggage = ET.SubElement(Passenger, 'ExcessBaggage', IssuerCode = str(flight_info['IATA_A']),
                                        SerialNumber = k,
                                        SequenceCount = str(1), BaggagePool = "MP", 
                                        UnitOfMeasureQuantity = str(choices([1,2,3], [0.5, 0.3, 0.2])[0]),
                                        UnitOfMeasureCode = "Kg")
                Passenger_FareInfo = ET.SubElement(Passenger, 'FareInfo', PTC_Code = 'PTC_CODE',
                                        DiscountedFareType = 'DISC_FARE_TYPE',
                                        DiscountrPercent = str(5), CountryCode = str(booking_info['HH_ISO']),
                                        DiscountedFareClassType = 'CABIN_CLASS',
                                        FareBasis = 'SFLY')
                Passenger_SSR = ET.SubElement(Passenger, 'SSR', SSR_Code = 'OTHS',
                                        ServiceQuantity = str(1),Status = 'HN',
                                        BoardPoint = flight_info ['IATA_O'],
                                        OffPoint = flight_info['IATA_D'],
                                        RPH = str(k),
                                        SurnameRefNumber = str(p_info['HHID']))
                Passenger_SSR_Airline = ET.SubElement(Passenger_SSR, 'Airline', Code = str(flight_info['IATA_A']))
                Passenger_SSR_Text = ET.SubElement(Passenger_SSR, 'TEXT')
                Passenger_SSR_Text.text = 'MADDOX/MOLLY'
                Passenger_TicketDocument = ET.SubElement(Passenger, 'TicketDocument', TicketDocumentNbr = str(i)+"/"+str(j)+"/"+str(k),
                                        Type = 'TICKET_TYPE',
                                        DateOfIssue = str(flight_info['day']),
                                        TicketLocation = flight_info['IATA_O'],
                                        PrimaryDocInd = str(choices(['TRUE', 'FALSE'], [0.7, 0.3])[0]))
                Passenger_TicketDocument_TotalFare = ET.SubElement(Passenger_TicketDocument, 'TotalFare', 
                                        Amount = str(12345),
                                        CurrencyCode = str(booking_info['Currency']))
                Passenger_TicketDocument_PricingInfo = ET.SubElement(Passenger_TicketDocument, 'PricingInfo', Date = str(flight_info['day']),
                                        Time = str(booking_info['outboundDate']),
                                        ISOCountryCode = str(booking_info['HH_ISO']),
                                        LocationCode = str(booking_info['IATA']),
                                        NonEndorsableInd = str(choices(['TRUE', 'FALSE'], [0.7, 0.3])[0]),
                                        NonRefundableInd = str(choices(['TRUE', 'FALSE'], [0.7, 0.3])[0]),
                                        PenaltyRestrictionInd = str(choices(['TRUE', 'FALSE'], [0.7, 0.3])[0]))
                Passenger_TicketDocument_Taxes = ET.SubElement(Passenger_TicketDocument, 'Taxes')
                TicketDocument_Taxes_Tax = ET.SubElement(Passenger_TicketDocument_Taxes, 'Tax', Amount = str(123),
                                        ISOCountry = str(booking_info['HH_ISO']),
                                        CurrencyCode = str(booking_info['Currency']),
                                        TaxType = "BK")
                primary_passenger = booking_info['primary_passengers']
                Passenger_TicketDocument_PaymentInfo = ET.SubElement(Passenger_TicketDocument, 'PaymentInfo', PaymentType = 'CC', PaymentUse = 'NEW',
                                           PaymentAmount = str(12345),
                                           VendorCode = str(p_data[primary_passenger]['PaymentInfo_VendorCode']),
                                           AccountNbr = str(p_data[primary_passenger]['PaymentInfo_AccountNbr']),
                                           ExpiryDate = str(p_data[primary_passenger]['PaymentInfo_ExpiryDate']),
                                       CardHolderName = str(p_data[primary_passenger]['FirstName'])+str(p_data[primary_passenger]['Surname']))
                Passenger_DOCSSR = ET.SubElement(Passenger, 'DOC_SSR', SSR_Code = 'DOCA',
                                     ServiceQuantity = str(1),
                                     Status = 'HK')
                Passenger_DOCSSR_FlightInfo = ET.SubElement(Passenger_DOCSSR, 'FlightInfo', Code = str(flight_info['IATA_A']))
                Passenger_DOCSSR_DOCA = ET.SubElement(Passenger_DOCSSR, 'DOCA', AddressType = "R",
                                      Address = str(p_info['Address']),
                                      CityName = str(booking_info['City']),
                                      PostalCode = str(p_info['PostCode']),
                                      StateProvCounty = 'STATE_PROV_COUNTY',
                                      Country = str(p_info['Country']))
                Passenger_DOCSSR = ET.SubElement(Passenger, 'DOC_SSR', SSR_Code = 'DOCO',ServiceQuantity = str(1),Status = 'HK')
                Passenger_DOCSSR_DOCO = ET.SubElement(Passenger_DOCSSR, 'DOCO', BirthLocation = str(choices([p_info['Country'], p_info['NationalityNat']], [0.5, 0.5])),
                                       TravelDocType = 'V',
                                       TravelDocNbr = str(k),
                                       PlaceOfIssue = str(p_info['NationalityNat']),
                                       DateOfIssue = "-",
                                       CountryState = str(p_info['NationalityNat']))
                passenger_docssr = ET.SubElement(Passenger, 'DOC_SSR', SSR_Code = 'DOCS',ServiceQuantity = str(1),Status = 'HK')
                passenger_docssr_doco = ET.SubElement(passenger_docssr, 'DOCS', DateOfBirth = str(p_info['DOB']),
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
                Flight = ET.SubElement(PNR, 'Flight', 
                DepartureDateTime= str(flight_list['firstseen']),
                ArrivalDateTime = str(flight_list['lastseen']),
                ResBookDesigCode = "S",
                NumberInParty = str(booking_info['num_in_party']),
                Status = "HK",
                FlightNumber = str(fly))
                DepartureAirport = ET.SubElement(Flight, 'DepartureAirport', LocationCode = str(flight_list['IATA_O']))
                ArrivalAirport = ET.SubElement(Flight, 'ArrivalAirport', LocationCode = str(flight_list['IATA_D']))
                OperatingAirline = ET.SubElement(Flight, 'OperatingAirline', Code = str(flight_list['IATA_A']),
                                         FlightNumber = str(flight_list['flightNbr']),
                                         ResBookDesigCode = "S")
                MarketingAirline = ET.SubElement(Flight, 'MarketingAirline', Code = "TX")
            
                fly = fly+1

    tree = ET.ElementTree(root)    
    filename = os.path.join(complete_dir, f'{i}.xml')
      
    with open (filename, "wb") as file :
        file.write(ET.tostring(root,encoding='UTF-8'))