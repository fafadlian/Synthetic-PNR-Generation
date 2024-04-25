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

### 4. Build XML
- **Main File:** BuildXML.py
- **Related Files:** src/BUILD_xml.py
- **Purpose:** The purpose of FlightBooking.py is to handle the flight booking process in the travel booking simulation. It is responsible for generating flight options, selecting flights based on passenger preferences, and managing the booking of flights for the simulated passengers.
- **Key Features:**
  - **Flight Options Generation:** Generates a list of available flight options based on the given origin, destination, and travel dates.
  - **Flight Selection:** Selects the most suitable flights for each passenger based on their preferences, such as price, airline, departure time, etc.
  - **Booking Management:** Handles the booking process by reserving the selected flights for the passengers and updating the booking status.


  ## Prerequisite
  Before running the script, ensure you have the following:
  1. Python 3.x installed on your system.
  2. Anaconda or pip installed.
  3. An Anaconda environment.

  ## Installation
  To install and set up the project, follow these steps:
  1. Download or clone the repository to your local machine.
  2. Download the flight data from this [Google Drive link](https://drive.google.com/drive/folders/1fZSUiiTk_jU4gRSipsJrQ23uyKOqeQOs) and place it inside the `data/flightData` directory in your working directory.
  3. Open a terminal and navigate to your working directory.
  4. Run the following command to install the required dependencies:
  ```
  pip install -r requirements.txt
  ```




  ## Execution Order
  To run the Synthetic PNR Data Project, follow the below execution order:

  1. The first part of code that we need to run is the Synthetic Population Generation
  ```
  python SynthMain.py --data_dir <data_directory> --hh_count <household_count> --num_cores <num_cores>    
  ```
  Replace <data_directory> with the root directory of your data, <household_count> with the estimated number of households to generate, and <num_cores> with the number of cores to use for parallel processing. The --num_cores argument is optional and defaults to -1, which means all available cores will be used.

  For example:
  ```
  python SynthMain.py --data_dir data --hh_count 50 --num_cores 4  
  ```

  2. After generating the population database, run the `GroupingBooking.py` script. This script simulates passenger behavior and generates booking groups based on the generated population database.

  3. Once the booking groups are generated, execute the `FlightBooking.py` script. This script handles the flight booking process by generating flight options, selecting suitable flights for each passenger, and managing the booking of flights.

  4. Finally, run the `BuildXML.py` script. This script is responsible for building XML files based on the generated synthetic PNR data and flight bookings.

  Make sure to execute the scripts in the specified order to ensure the proper functioning of the Synthetic PNR Data Project.



Contributing
Contributions to the project are welcome. Please ensure to follow best practices for code contributions, including using clear and descriptive commit messages and creating pull requests for review.

License
(Include license information here, if applicable.)


 
