from folder_crawler import FolderCrawler
from cmd_args import command_line_arguments_parser, resolve_default_values

# PERFORMANCE (JUST FOR REFERENCE):
# With multiprocessing implemented in this code, you can crawl bunch of data.
# 4.57 GHz 8Core (equals 100% utilization at my machine) will crawl and save all paths from 715GB in ~4.5 minutes.
# Extracted data after the crawl of the data mentioned above were ~260MB.

# HOW TO ADD A NEW COLUMN INTO TABLE:
# 1. In "folder_crawler.py", add a new item into "COLUMN_NAMES" at last position.
# 2. Create a new method in "FolderCrawler" class which will compute the new property.
# 3. In "folder_crawler.py", put the result of the above method at last position into "data_complete" tuple in method
#  "_get_path_with_properties".

if __name__ == '__main__':
    ####################################################################################################################
    # COMMAND LINE USAGE
    # cmd_args = command_line_arguments_parser()
    # default_values = resolve_default_values(cmd_args)
    #
    # cr = FolderCrawler(path=fr"{cmd_args.path}")
    # cr.main(
    #     crawl=cmd_args.crawl,
    #     crawl_deep=not cmd_args.shallow,
    #     print_files=cmd_args.visualize,
    #     print_folders=cmd_args.visualize,
    #     print_skipped_items=cmd_args.excluded,
    #     filter_path=default_values[0],
    #     filter_size=default_values[1],
    #     filter_size_sign=default_values[2],
    #     filter_date=default_values[3],
    #     filter_date_sign=default_values[4],
    # )
    ####################################################################################################################
    # NORMAL USAGE WITHIN THE IDE
    cr = FolderCrawler(path=r"C:\\Users\\lazni\\Downloads")
    cr.main(
        crawl=True,
        crawl_deep=True,
        print_files=False,
        print_folders=False,
        print_skipped_items=False,
    )

    # todo: put this into main
    # cr.read_content_of_one_file(r"C:\Users\lazni\Downloads\test.txt", filter_file_content="1")
    cr.read_content_of_multiple_files(filter_path_name=".txt", filter_file_content="1")

    # todo: for filtering of strings, implement regex or use like for filter in (filter1, filter2, filter3)...