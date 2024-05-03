
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