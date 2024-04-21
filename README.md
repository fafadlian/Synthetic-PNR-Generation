# Synthetic PNR Data Project

## Overview
This project simulates the generation and management of synthetic data related to travel bookings. It specifically focuses on creating Passenger Name Records (PNR), determining flight routes, generating booking groups, and selecting flights for those bookings. The goal is to simulate realistic travel behaviors and preferences to support analysis and modeling in travel and aviation-related applications.

## Project Structure
The project is divided into four main scripts, each responsible for a specific part of the travel booking simulation process:

### 1. Synthetic PNR Data Generation
- **Main File:** GeneratePopulation.py
- **Related Files:** src/
- **Purpose:** This script is the cornerstone of our data generation process. It creates a synthetic population database that models potential travelers. Each record includes detailed demographic and geographic information, establishing a realistic base for simulating travel behaviors.
- **Key Features:**
  - **Demographic Profiles:** Generates detailed profiles for synthetic individuals, including age, gender, and household size.
  - **Geographic Distribution:** Assigns geographic locations based on real-world population statistics to reflect plausible living areas for individuals.
  - **Person and Individual Identities:** Outputs a comprehensive dataset that serves as the input for subsequent simulation steps that includes Firstname, Surname, DOB, ID, etc.

### 2. Passenger Behaviour and Grouping/Booking
- **Main File:** GroupingBooking.py
- **Related Files:** src/BOOK_Behaviour.py, src/BOOK_Grouping.py, src/BOOK_Trip.py, 
- **Purpose:** The purpose of GroupingBooking.py is to generate booking groups and simulate passenger behavior and grouping/booking in the travel booking simulation process. It models decisions such as choosing travel destinations, trip purposes, and duration of stay.
- **Key Features:**
  - **:** Generates detailed origin, destination, stay duration, travel type, person in booking, etc.

### 3. Flight Booking
- **Main File:** FlightBooking.py
- **Related Files:** src/FLY_Booking.py
- **Purpose:** The purpose of FlightBooking.py is to handle the flight booking process in the travel booking simulation. It is responsible for generating flight options, selecting flights based on passenger preferences, and managing the booking of flights for the simulated passengers.
- **Key Features:**
  - **Flight Options Generation:** Generates a list of available flight options based on the given origin, destination, and travel dates.
  - **Flight Selection:** Selects the most suitable flights for each passenger based on their preferences, such as price, airline, departure time, etc.
  - **Booking Management:** Handles the booking process by reserving the selected flights for the passengers and updating the booking status.

### 3. Build XML
- **Main File:** BuildXML.py.py
/**
 * @file FlightBooking.py
 * @brief Handles the flight booking process in the travel booking simulation.
 *
 * The FlightBooking.py module is responsible for generating flight options, selecting flights based on passenger preferences, and managing the booking of flights for the simulated passengers.
 *
 * @section Related_Files Related Files
 * - src/BUILD_xml.py
 *
 * @section Key_Features Key Features
 * - Flight Options Generation: Generates a list of available flight options based on the given origin, destination, and travel dates.
 * - Flight Selection: Selects the most suitable flights for each passenger based on their preferences, such as price, airline, departure time, etc.
 * - Booking Management: Handles the booking process by reserving the selected flights for the passengers and updating the booking status.
 */
- **Related Files:** src/BUILD_xml.py
- **Purpose:** The purpose of FlightBooking.py is to handle the flight booking process in the travel booking simulation. It is responsible for generating flight options, selecting flights based on passenger preferences, and managing the booking of flights for the simulated passengers.
- **Key Features:**
  - **Flight Options Generation:** Generates a list of available flight options based on the given origin, destination, and travel dates.
  - **Flight Selection:** Selects the most suitable flights for each passenger based on their preferences, such as price, airline, departure time, etc.
  - **Booking Management:** Handles the booking process by reserving the selected flights for the passengers and updating the booking status.




Execution Order
To simulate the entire process of planning and booking travel accurately, execute the scripts in the following order:

Synthetic PNR Data Generation
Routing Code
Booking Groups Generation
Flight Selection
Dependencies
Python 3.8+
pandas
numpy
scipy
joblib
(any other libraries or frameworks used in the scripts)
Setup and Usage
Ensure Python and all required dependencies are installed. Execute the scripts in the prescribed order, adjusting paths to datasets as necessary. Each script reads input data and generates output datasets that serve as input for the subsequent script in the simulation pipeline.

Contributing
Contributions to the project are welcome. Please ensure to follow best practices for code contributions, including using clear and descriptive commit messages and creating pull requests for review.

License
(Include license information here, if applicable.)


 
