Synthetic Travel Booking Simulation Project
Overview
This project simulates the process of generating and managing synthetic data related to travel bookings, encompassing the generation of synthetic Passenger Name Record (PNR) data, determining flight routing, generating booking groups, and selecting specific flights for bookings. It is designed to simulate realistic travel behaviors and preferences, facilitating analysis or modeling in travel and aviation-related applications.

## Project Structure
The project is divided into four main scripts, each responsible for a specific part of the travel booking simulation process:

### 1. Synthetic PNR Data Generation
- **File:** GeneratePopulation.py
- **Directory:** src/
- **Purpose:** This script is the cornerstone of our data generation process. It creates a synthetic population database that models potential travelers. Each record includes detailed demographic and geographic information, establishing a realistic base for simulating travel behaviors.
- **Key Features:**
  - **Demographic Profiles:** Generates detailed profiles for synthetic individuals, including age, gender, and household size.
  - **Geographic Distribution:** Assigns geographic locations based on real-world population statistics to reflect plausible living areas for individuals.
  - **Travel Preferences Modeling:** Integrates travel preferences tailored to demographic and geographic data, setting the stage for realistic travel scenario simulations.
  - **Person and Individual Identities:** Outputs a comprehensive dataset that serves as the input for subsequent simulation steps that includes Firstname, Surname, DOB, ID, etc.


2. Routing Code
File: Routing.py
Purpose:
Manages the creation of feasible flight routes between airports, including direct and connecting flights. Utilizes algorithms to find the shortest and most realistic paths between locations.
3. Booking Groups Generation
File: Booking_Groups_Gen.py
Purpose:
Simulates the formation of travel groups based on the synthetic population data. It models decisions such as choosing travel destinations, trip purposes, and duration of stay.
4. Flight Selection
File: Flight_Selection.py
Purpose:
Handles the assignment of synthetic booking groups to actual flights, ensuring that the selections adhere to flight schedules, capacity limits, and planned itineraries.

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


 
