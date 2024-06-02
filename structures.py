from dataclasses import dataclass
import os

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
    NR_OF_CRAWLED_DATA = "NUMBER OF CRAWLED DATA:"
    SAVING_RESULTS = "Saving into csv file:"
    SAVING_RESULTS_DONE = "Saving done:"
    DATAFRAME_PREPARATION = "Preparing dataframes."
    DATAFRAME_PREPARATION_DONE = "Preparation of dataframe is done:"
    STARTING_MULTI_PROCESSING = "Starting multi-processing pool."
    PRINT_ENDING = f"\n{'-' * 150}\n"


@dataclass
class FileOps:
    ENCODING = "UTF-8"
    READ_MODE = "r"
    APPEND_MODE = "a"
