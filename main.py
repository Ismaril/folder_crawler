from folder_crawler import FolderCrawler
from cmd_args import command_line_arguments_parser, resolve_default_values
from structures import SavedCrawls

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
    cr = FolderCrawler(path=r"C:\\")
    cr.main(
        crawl=True,
        crawl_deep=False,
        print_files=True,
        print_folders=True,
        print_skipped_items=True,
        filter_path="",
        filter_size=1024**3,
        filter_size_sign=">=",
        filter_date="1900",
        filter_date_sign=">=",
        read_out_file_contents=False,
        filter_file_content=""
    )

    # This is the only public method except the main method which you can use.
    # cr.compare_saved_crawls(SavedCrawls.FILES, path2=r"saved_crawls\second_file_with_crawled_data.txt")
