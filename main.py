from folder_crawler import FolderCrawler
import argparse

########################################################################################################################
# HOW TO CRAWL AND PRINT ITEMS:
# Start with specifying the path of the folder you want to crawl in ctor. Then chose if you want to crawl deep or not.
# Call method crawl.
# Call method print_items with parameters you want to print. You can also filter the output.
# In case you performed time-consuming crawl already, comment out the crawl method and just call print_items method.
#   The previous crawl will be instantly read out from the saved txt files.


# HOW TO READ OUT SAVED FILES:
# Call method read_content_of_file with the path of the file you want to read out.


# HOW TO COMPARE SAVED CRAWLS:
# Perform a crawl in a first desired folder and then rename the output txt file in saved_crawls folder.
# Perform a crawl in a second desired folder.
# Call method compare_saved_crawls with the paths of the two files you want to compare.
# The method will return the difference between the two files.


# PERFORMANCE:
# For you info, you can also crawl complete disk. With multiprocessing implemented in this code, it will be quite fast.
# 4.57 GHz 8Core utilised therefore at 100% will crawl and save all paths from 715GB in 3 minutes.
########################################################################################################################
if __name__ == '__main__':
    # COMMAND LINE ARGUMENTS
    # Create the parser
    parser = argparse.ArgumentParser(description="Python argument parser from command line")

    # Add arguments
    parser.add_argument('-p', '--path', type=str, help="Path to the folder you want to crawl.")
    parser.add_argument('-c', '--crawl', action='store_true', help="Enable crawling of specified folder.")
    parser.add_argument('-s', '--shallow', action='store_true', help="Do not crawl deep into sub-folders.")
    parser.add_argument('-f', '--files', action='store_true', help="Print files.")
    parser.add_argument('-d', '--dirs', action='store_true', help="Print directories.")
    parser.add_argument('-e', '--excluded', action='store_true', help="Print skipped items.")
    args = parser.parse_args()
    cr = FolderCrawler(path=fr"{args.path}")
    cr.main(
        crawl=args.crawl,
        crawl_deep=not args.shallow,
        print_files=args.files,
        print_folders=args.dirs,
        print_skipped_items=args.excluded,
    )
    ####################################################################################################################
    # NORMAL USAGE WITHIN THE IDE
    # cr = FolderCrawler(path=r"C:\\Users\lazni\Downloads")
    # cr.main(
    #     crawl=True,
    #     crawl_deep=False,
    #     print_files=True,
    #     print_folders=True,
    #     print_skipped_items=True,
    # )
