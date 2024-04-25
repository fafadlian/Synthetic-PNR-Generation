import sys
import argparse
from ABM_GroupingBooking import GroupBooking

def main(args):
    data_dir = args.data_dir # e.g., 'data', for our folder
    num_cores = args.num_cores # e.g., 4, for number of cores to use for parallel processing
    data_analysis = GroupBooking(data_dir, num_cores)
    data_analysis.run_analysis()
        

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Run the synthetic population generation process.'
    )
    parser.add_argument('--data_dir', 
                        required=True, 
                        action='store',
                        help='root of the data')
    
    parser.add_argument('--num_cores', 
                        type=int,
                        # required=True,
                        default=-1,
                        help='number of cores to use for parallel processing. Default is 0.0, which means all available cores.')
    
    args = parser.parse_args()
    main(args)