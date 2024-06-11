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
    ROOT = "saved_crawls"
    EXTENSION = ".txt"
    FILES = os.path.join(ROOT, f"{ItemType.FILES}{EXTENSION}")
    FOLDERS = os.path.join(ROOT, f"{ItemType.FOLDERS}{EXTENSION}")
    SKIPPED = os.path.join(ROOT, f"{ItemType.SKIPPED}{EXTENSION}")


@dataclass
class Messages:
    DEEP_CRAWL = "Option chosen: DEEP CRAWL -> Going deep into sub-folders."
    SHALLOW_CRAWL = "Option chosen: SHALLOW CRAW -> Staying in the inputted folder."
    WHOLE_PROCES_TOOK = "THE WHOLE PROCESS TOOK:"
    NR_OF_CRAWLED_DATA = "TOTAL CRAWLED DATA:"
    SAVING_RESULTS = "Saving into csv file:"
    DATAFRAME_PREPARATION = "Preparing dataframes."
    STARTING_MULTI_PROCESSING = "Starting multi-processing pool. The crawling starts now."


@dataclass
class FileOps:
    ENCODING = "UTF-8"
    READ_MODE = "r"
    APPEND_MODE = "a"
    WRITE_MODE = "w"


@dataclass
class ByteUnit:
    BYTE = "B"
    KILOBYTE = "KB"
    MEGABYTE = "MB"
    GIGABYTE = "GB"
    TERABYTE = "TB"


@dataclass
class ByteSize:
    BYTE = 1
    KILOBYTE = 1024
    MEGABYTE = 1048576
    GIGABYTE = 1073741824
    TERABYTE = 1099511627776


@dataclass
class ColorFormatting:
    COLORS = [Fore.RED, Fore.YELLOW, Fore.GREEN, Fore.BLUE, Fore.CYAN]
    UNITS = [ByteUnit.BYTE, ByteUnit.KILOBYTE, ByteUnit.MEGABYTE, ByteUnit.GIGABYTE, ByteUnit.TERABYTE]


@dataclass
class ColoredBytes:
    """
    This dataclass was created mainly for testing purposes.
    """
    ONE_KB_PRETTY = "\x1b[33m1.00KB\x1b[0m"
    ONE_KB_RAW = "\x1b[33m 1024 \x1b[0m"
    TWO_KB_PRETTY = "\x1b[33m2.00KB\x1b[0m"
    TWO_KB_RAW = "\x1b[33m 2048 \x1b[0m"
    THREE_KB_PRETTY = "\x1b[33m3.00KB\x1b[0m"
    THREE_KB_RAW = "\x1b[33m 3072 \x1b[0m"
    THIRTYSEVEN_BYTES_RAW = "\x1b[31m 37 \x1b[0m"
