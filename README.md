# Synthetic PNR Data Project

## Overview
This project simulates the generation and management of synthetic data related to travel bookings. It specifically focuses on creating Passenger Name Records (PNR), determining flight routes, generating booking groups, and selecting flights for those bookings. The goal is to simulate realistic travel behaviors and preferences to support analysis and modeling in travel and aviation-related applications.

## Prerequisite
Before running the script, ensure you have the following:
1. Python 3.9 installed on your system. (It should also work on the later version)
2. Anaconda or pip installed.

## Installation
To install and set up the project, follow these steps:
1. Download or clone the repository to your local machine.
2. Download the data folder from this [Google Drive link](https://drive.google.com/drive/folders/1fZSUiiTk_jU4gRSipsJrQ23uyKOqeQOs) and place it inside your working directory.
3. Open a terminal and navigate to your working directory.
4. Run the following command to install the required dependencies:
```
pip install -r requirements.txt
```




## Execution Order
To run the Synthetic PNR Data Project, follow the below execution order:
### 1. Run Synthetic Population Generation
The first part of code that we need to run is the Synthetic Population Generation
```
python Synth_main.py --data_dir <data_directory> --hh_count <household_count> --num_cores <num_cores>    
```
    
  Replace <data_directory> with the root directory of our data, <household_count> with the estimated number of households to generate, and <num_cores> with the number of cores to use for parallel processing. The --num_cores argument is optional and defaults to -1, which means all available cores will be used.

  For example:
  ```
  python Synth_main.py --data_dir 'data' --hh_count 50 --num_cores 4  
  ```
  The result of your synthesized data can be found inside the `synthesizedData` folder with the names `person_data.csv` and `HH_data.csv`.

  Note: To specify the country's population to be synthesized, please open `data/ISO.csv` and change the list of countries using ISO codes (https://en.wikipedia.org/wiki/ISO_3166-1_alpha-3).

### 2. Run Grouping and Booking Code
```
python ABMGroupBook_main.py --data_dir <data_directory> --num_cores <num_cores>    
```
    
  Replace <data_directory> with the root directory of your data, and <num_cores> with the number of cores to use for parallel processing. However, the current version can only be run with a single core, please use 1 as the num cores. The --num_cores argument is optional and defaults to 1, which means only 1 core will be used.

  For example:
  ```
  python ABMGroupBook_main.py --data_dir 'data' --num_cores 1  
  ```
  The result of you synthesized data can be seen inside `synthesizedData` folder  with name `group.csv` and `behaviour_complete.csv`

### 3. Run POI/SOI Generation Code
```
python SynthSOI_main.py --data_dir <data_directory> --hh_count <household_count> --num_cores <num_cores>    
```
    
  Replace <data_directory> with the root directory of our data, <household_count> with the estimated number of households to generate, and <num_cores> with the number of cores to use for parallel processing. The --num_cores argument is optional and defaults to -1, which means all available cores will be used.

  For example:
  ```
  python SynthSOI_main.py --data_dir 'data' --hh_count 10 --num_cores 4  
  ```
  The result of your synthesized data can be found inside the `synthesizedData` folder with the names `person_SOI.csv`, `group_SOI.csv` and `HH_SOI.csv`.


### 4. Run Flight Selection
```
python ABMFlight_main.py --data_dir <data_directory>     
```
    
  Replace <data_directory> with the root directory of your data

  For example:
  ```
  python ABMFlight_main.py --data_dir 'data' 
  ```
  The result of you synthesized data can be seen inside `synthesizedData` folder  with name `bookings_complete.csv` and `flights_complete.csv`
### 5. Build XML PNR File
```
python BuildXML_main.py --data_dir <data_dir> --xml_dir <xml_dir> --num_cores <num_cores>     
```
Replace <data_directory> with the root directory of our data, <xml_dir> with folder which we stored .XML data, and <num_cores> with the number of cores to use for parallel processing. 
For example:
  ```
  python BuildXML_main.py --data_dir 'data' --xml_dir 'XML_DIR' --num_cores 4 
  ```

  Make sure to execute the scripts in the specified order to ensure the proper functioning of the Synthetic PNR Data Project.

## Additional Information

### 1. Household Identifier
Households are uniquely identified in the 'HHID' column, formatted as `<ISO>_<NUMBER>`. Here, `<ISO>` is the ISO code of the country, and `<NUMBER>` is a distinct number within that country. 
Example: `AUT_1` represents Household no1 in Austria.

### 2. Person Identifier
Individuals within households are identified in the 'P_ID' column, formatted as `<HHID>_<NUMBER>`. Here, `<HHID>` is the household ID (e.g., `AUT_1`) and `<NUMBER>` is a distinct number representing a person in the household. The person with `<NUMBER> = 1` is considered the head of the household. `AUT_1_1` represent a head of household in of no1 household in Austria.

### 3. Household Type (HHType)

| HHType  | Description                                  |
|---------|----------------------------------------------|
| 1A      | Single adult with child/children             |
| 1B      | Single adult without child/children          |
| 2A      | Couple adult with child/children             |
| 2B      | Couple adult without child/children          |
| 3AB     | Other                                        |
| T1 (SOI)| Child travels alone                          |
| T2 (SOI)| Child travels with an adult with different surnames |


### 4. SOI/POI Identifier
SOI/POI can be identified from their P_ID and HHID that should start with 'SOI'

### 5. SOI/POI Generation base file
To generate a new type/personas of SOI/POI can be conducted through editing/adding new rows in `data/POI_base.csv` file. Each row represent a persona of POI/SOI. M/F ratio, age range, nationalities, language, airport of origin, and stay day can be configured through the csv file.


## Contributing
Contributions to the project are welcome. Please ensure to follow best practices for code contributions, including using clear and descriptive commit messages and creating pull requests for review.

## License
This project is licensed under the [MIT License](LICENSE).


 
