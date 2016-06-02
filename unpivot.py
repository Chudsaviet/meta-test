#!/usr/bin/env python3

############################################################################################
# Python 3, sorry.
# Chosen raw file read instead of 'csv' module because of performance.
# This script is not intended for use with bad input files.
# If we will encounter complex or bad csv files, 'csv' module will be much better solution
############################################################################################

import argparse
import logging
from operator import itemgetter


def get_columns_positions(unpivot_columns, all_columns):

    key_columns_positions = list()
    unpivot_columns_positions = list()

    i = 0
    for column in all_columns:
        if column in unpivot_columns:
            unpivot_columns_positions.append(i)
        else:
            key_columns_positions.append(i)
        i += 1

    return key_columns_positions,unpivot_columns_positions


def main():
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(
        description='Unpivot for META team'
    )
    parser.add_argument('input_file', type=argparse.FileType('r'),
                        help='Input file name')
    parser.add_argument('unpivot_columns',
                        help='Columns to unpivot')
    parser.add_argument('output_file', type=argparse.FileType('w'),
                        help='Output file name')
    args = parser.parse_args()

    logging.info('Files opened successfully')

    # First line is columns definition line
    first_line = args.input_file.readline()

    if not first_line:
        logging.warning('Empty input file. Exiting.')
        return

    input_columns = first_line.strip('\n\r').split(',')

    logging.info('Input columns: %s' % input_columns)

    unpivot_columns_names = args.unpivot_columns.split(',')

    key_columns_positions, unpivot_columns_positions = get_columns_positions(unpivot_columns_names, input_columns)
    key_columns_operator = itemgetter(*key_columns_positions)

    # write first line (columns definition) to output file
    key_columns_names = key_columns_operator(input_columns)
    args.output_file.write(','.join(key_columns_names) + ',' + 'unpivotted\n')

    processed_lines = 0
    for line in args.input_file:
        # Skip empty lines
        stripped_line = line.strip('\n\r')
        if stripped_line:
            line_splitted = stripped_line.split(',')
            key_columns = ','.join(key_columns_operator(line_splitted))
            for unpivot_position in unpivot_columns_positions:
                # Multiple write calls are fast because output files in Python are buffered by default
                args.output_file.write(key_columns)
                args.output_file.write(',')
                args.output_file.write(line_splitted[unpivot_position])
                args.output_file.write('\n')
            processed_lines += 1

    args.input_file.close()
    args.output_file.close()

    logging.info('Processed %d lines', processed_lines)

if __name__ == '__main__':
    main()
