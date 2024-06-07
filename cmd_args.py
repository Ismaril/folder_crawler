import argparse
from datetime import datetime


def command_line_arguments_parser():
    """
    Parse arguments which are passed from the command line.
    """
    parser = argparse.ArgumentParser(description="Python argument parser from command line")

    # Add arguments
    parser.add_argument('-p', '--path', type=str, help="Path to the folder you want to crawl.")
    parser.add_argument('-c', '--crawl', action='store_true', help="Enable crawling of specified folder.")
    parser.add_argument('-s', '--shallow', action='store_true', help="Do not crawl deep into sub-folders.")
    parser.add_argument('-v', '--visualize', action='store_true', help="Print the results in the console.")
    parser.add_argument('-e', '--excluded', action='store_true', help="Print skipped items.")

    parser.add_argument('--fpath', type=str, help="Filter by path.")
    parser.add_argument('--fsize', type=int, help="Filter by size.")
    parser.add_argument('--fsizesgn', type=str, help="Sign to filter by size. Supported signs: >=, <=")
    parser.add_argument('--fchanged', type=str, help="Filter by changed date. Supported formats: YYYY-MM-DD hh:mm:ss or anything from the left: YYYY, YYYY-MM, YYYY-MM-DD, ...")
    parser.add_argument('--fchangedsgn', type=str, help="Sign to filter by changed date. Supported signs: >=, <=")

    return parser.parse_args()


def resolve_default_values(cmd_args) -> tuple:
    """
    Resolve default values for the filters.
    """
    fpath = "" if cmd_args.fpath is None else cmd_args.fpath
    fsize = 0 if cmd_args.fsize is None else cmd_args.fsize
    fsizesgn = ">=" if cmd_args.fsizesgn is None else cmd_args.fsizesgn
    fchanged = str(datetime.min) if cmd_args.fchanged is None else cmd_args.fchanged
    fchangedsgn = ">=" if cmd_args.fchangedsgn is None else cmd_args.fchangedsgn

    return fpath, fsize, fsizesgn, fchanged, fchangedsgn
