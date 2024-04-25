import sys
import argparse
from SYNTH_GeneratePopulation import DataAnalysis


def main(args):
    data_dir = args.data_dir # e.g., 'data', for our folder
    hh_count = args.hh_count # e.g., 50, for estimated number of built HH. might be different especially for smaller number of HH count due to rounding
    num_cores = args.num_cores # e.g., 4, for number of cores to use for parallel processing
    

    data_analysis = DataAnalysis(data_dir, hh_count, num_cores)
    data_analysis.run_analysis()
        

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Run the synthetic population generation process.'
    )
    parser.add_argument('--data_dir', 
                        required=True, 
                        action='store',
                        help='root of the data')
    
    parser.add_argument('--hh_count',
                        type=int, 
                        required=True, 
                        help="estimated number of households to generate")
    
    parser.add_argument('--num_cores', 
                        type=int,
                        # required=True,
                        default=-1,
                        help='number of cores to use for parallel processing. Default is 0.0, which means all available cores.')
    
    args = parser.parse_args()
    main(args)