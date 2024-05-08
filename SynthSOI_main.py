import sys
import argparse
from SYNTH_GenerateSOI import POI_Generation


def main(args):
    data_dir = args.data_dir # e.g., 'data', for our folder
    hh_count = args.hh_count # e.g., 50, for estimated number of built HH. might be different especially for smaller number of HH count due to rounding
    num_cores = args.num_cores # e.g., 4, for number of cores to use for parallel processing
    poi_base = args.poi_base # e.g., 'poi_base.csv', for the base POI/SOI data
    output = args.output # e.g., 'output1', for the output marker if you want to create multiple output version
    

    soi_gen = POI_Generation(data_dir, hh_count, num_cores, poi_base, output)
    soi_gen.generate_POI()
        

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Run the synthetic SOI population generation process.'
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
    
    parser.add_argument('--poi_base', 
                        required=True, 
                        action='store',
                        help='poi_base file')
    
    parser.add_argument('--output', 
                        required=True, 
                        action='store',
                        help='output file marker')
    
    args = parser.parse_args()
    main(args)