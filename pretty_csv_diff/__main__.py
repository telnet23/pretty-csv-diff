import argparse

from .pretty_csv_diff import PrettyCsvDiff

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('path', nargs=2, help='paths to the two csv files to be compared')
    parser.add_argument('pk', nargs='+', help='name or index of primary key column. multiple columns are allowed')
    args = vars(parser.parse_args())

    for formatted_row in PrettyCsvDiff(**args).do():
        print('  '.join(formatted_row))


if __name__ == '__main__':
    main()
