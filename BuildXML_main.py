import sys
import argparse
from BuildXML import BuildXML


def main(args):
    data_dir = args.data_dir # e.g., 'data', for our folder
    xml_dir = args.xml_dir # e.g., 'xml', for our folder
    num_cores = args.num_cores # e.g., 4

    build_xml = BuildXML(data_dir, xml_dir, num_cores)
    build_xml.build_pnr()
        

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Run the synthetic population generation process.'
    )
    parser.add_argument('--data_dir', 
                        required=True, 
                        action='store',
                        help='root of the data')
    parser.add_argument('--xml_dir',
                        required=True,
                        action='store',
                        help='root of the xml')
    parser.add_argument('--num_cores',
                        type=int,
                        required=True,
                        action='store',
                        help='number of cores to use')
    
    args = parser.parse_args()
    main(args)