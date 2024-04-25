import sys
import argparse
from ABM_FlightSelection import FlightBookingSystem


def main(args):
    data_dir = args.data_dir # e.g., 'data', for our folder

    flight_selection = FlightBookingSystem(data_dir)
    flight_selection.run_analysis()
        

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Run the synthetic population generation process.'
    )
    parser.add_argument('--data_dir', 
                        required=True, 
                        action='store',
                        help='root of the data')
    
    args = parser.parse_args()
    main(args)