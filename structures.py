from dataclasses import dataclass
import os
from colorama import Fore


@dataclass
class ItemType:
    FILES = "files"
    FOLDERS = "folders"
    PARAMETERS = "parameters"
    SKIPPED = "skipped_items"


@dataclass
class SavedCrawls:
    SAVED_CRAWLS_FOLDER = "saved_crawls"
    EXTENSION = ".txt"
    FILES = os.path.join(SAVED_CRAWLS_FOLDER, f"{ItemType.FILES}{EXTENSION}")
    FOLDERS = os.path.join(SAVED_CRAWLS_FOLDER, f"{ItemType.FOLDERS}{EXTENSION}")
    SKIPPED = os.path.join(SAVED_CRAWLS_FOLDER, f"{ItemType.SKIPPED}{EXTENSION}")
    PARAMETERS = os.path.join(SAVED_CRAWLS_FOLDER, f"{ItemType.PARAMETERS}{EXTENSION}")


@dataclass
class Messages:
    DEEP_CRAWL = "Option chosen: DEEP CRAWL -> Going deep into sub-folders."
    SHALLOW_CRAWL = "Option chosen: SHALLOW CRAW -> Staying in the inputted folder."
    WHOLE_PROCES_TOOK = "THE WHOLE PROCESS TOOK:"
    NR_OF_CRAWLED_DATA = "SUMMARY OF CRAWLED DATA:"
    SAVING_RESULTS = "Saving into csv file:"
    SAVING_RESULTS_DONE = "Saving done:"
    DATAFRAME_PREPARATION = "Preparing dataframes."
    DATAFRAME_PREPARATION_DONE = "Preparation of dataframe is done:"
    STARTING_MULTI_PROCESSING = "Starting multi-processing pool. The crawling starts now."
    PRINT_ENDING = f"\n{'-' * 150}\n"


@dataclass
class FileOps:
    ENCODING = "UTF-8"
    READ_MODE = "r"
    APPEND_MODE = "a"


class ColorFormatting:
    COLORS = [Fore.RED, Fore.YELLOW, Fore.GREEN, Fore.BLUE, Fore.CYAN]
    UNITS = ["B", "KB", "MB", "GB", "TB"]

class ByteSize:
    BYTE = 1
    KILOBYTE = 1024
    MEGABYTE = 1024 ** 2
    GIGABYTE = 1024 ** 3
    TERABYTE = 1024 ** 4
