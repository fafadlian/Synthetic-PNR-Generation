# Synthetic Travel Booking Simulation Project
Overview
This project simulates the process of generating and managing synthetic data related to travel bookings, encompassing the generation of synthetic Passenger Name Record (PNR) data, determining flight routing, generating booking groups, and selecting specific flights for bookings. It is designed to simulate realistic travel behaviors and preferences, facilitating analysis or modeling in travel and aviation-related applications.

Project Structure
The project is structured into four main scripts, each serving a distinct purpose in the simulation pipeline:

Synthetic PNR Data Generation
Routing Code
Booking Groups Generation
Flight Selection
1. Synthetic PNR Data Generation
File: HH_P_Gen.py

Function: Generates synthetic PNR data, including information about households and their demographic characteristics. It establishes the foundational dataset of potential travelers, their geographic locations, and travel preferences.

2. Routing Code
File: Routing.py

Function: Processes and generates routing information for flights, determining feasible direct and indirect routes between airports. It calculates the shortest paths and manages the availability and selection of direct flights based on geographic constraints and connections.

3. Booking Groups Generation
File: Booking_Groups_Gen.py

Function: Generates synthetic booking groups based on household data and routing information. It simulates the decision-making process for travel, including selecting origin and destination cities, determining travel purposes (e.g., business, leisure), and assigning stay durations.

4. Flight Selection
File: Flight_Selection.py

Function: Selects specific flights for the synthetic booking groups, considering both outbound and return trips. It matches bookings with available flights based on schedules, capacity, and itineraries, updating the flights' occupancy and booking lists accordingly.

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


 
