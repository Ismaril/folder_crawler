import datetime
import os
import time
import pandas as pd
import numpy as np

from tabulate import tabulate
from structures import ItemType, SavedCrawls, Messages, FileOps
from multiprocessing import Pool
from colorama import init, Fore, Back, Style

# region Constants
NONE = np.nan

# todo: navod jak pridat dalsi sloupec dat. Pridat nazev sloupce do COLUMN_NAMES a pridat do INITIAL_DATAFRAME dalsi
#  pozici. Pote v process item vytvorit novou promenou a dat ji do returnu.
COLUMN_NAMES = ["Path", "Changed", "Size readable", "Size bytes"]

# TODO: tOHLE UDELAT JAKO PARAMETER DO PRINTU
switched_column_names = ["Path", "Size bytes", "Size readable", "Changed"]

INITIAL_DATAFRAME = {
    COLUMN_NAMES[0]: [],
    COLUMN_NAMES[1]: [],
    COLUMN_NAMES[2]: [],
    COLUMN_NAMES[3]: []
}
TABLE_HEADER = "keys"
TABLE_FORMAT = "psql"


# endregion

class FolderCrawler:
    """
    The FolderCrawler class is used to crawl through a folder and its sub-folders and prints the paths with sizes.
    """

    # region Constructor
    def __init__(self, path: str):
        """
        This is the constructor method for the FolderCrawler class.

        :param path: The path of the folder that needs to be crawled.
        """

        self.path = path
        self.timer = time.perf_counter()  # start the timer

        # create a dataframe with column names, but no data
        self.files = pd.DataFrame(INITIAL_DATAFRAME)
        self.folders = pd.DataFrame(INITIAL_DATAFRAME)
        self.skipped = pd.DataFrame(INITIAL_DATAFRAME)

        # Initialize colorama
        init(autoreset=True)
        print(Fore.WHITE, Back.BLACK, Style.RESET_ALL)

    # endregion

    # todo: refactor
    def main(self, print_folders=True, print_files=True, print_skipped_items=True,
             crawl=True, crawl_deep=True):
        self._initialize_files()
        if crawl:
            # CRAWL
            dataframe = self._crawl_items(self.path, crawl_deep)

            # PREPARE DATAFRAMES
            self.folders = self._get_crawled_data(dataframe, is_folder=True)
            self.files = self._get_crawled_data(dataframe, is_folder=False)
            self.files, self.folders, self.skipped = self._filter_data(
                self.files, self.folders, empty_dataframe=INITIAL_DATAFRAME, column=COLUMN_NAMES[3])

            # SAVE DATAFRAMES
            item_types = (ItemType.FILES, ItemType.FOLDERS, ItemType.SKIPPED)
            containers = (self.files, self.folders, self.skipped)
            paths = (SavedCrawls.FILES, SavedCrawls.FOLDERS, SavedCrawls.SKIPPED)
            for item_type, container, path in zip(item_types, containers, paths):
                print(self._get_current_time(), Messages.SAVING_RESULTS, item_type.upper())
                self._save_crawl_result(path, container)

        # LOAD DATAFRAMES
        self.files = self.load_crawled_data(container=self.files, item_type=ItemType.FILES)
        self.folders = self.load_crawled_data(container=self.folders, item_type=ItemType.FOLDERS)
        self.skipped = self.load_crawled_data(container=self.skipped, item_type=ItemType.SKIPPED)

        # PRINT DATAFRAMES
        self.print_items(print_folders, print_files, print_skipped_items, filter_path="", filter_sign=">=",
                         filter_size=0, crawl_deep=crawl_deep)

        # PRINT TIME PERFORMANCE
        time_performance = self._get_time_performance(self.timer)
        print("\n" + Messages.WHOLE_PROCES_TOOK, self._format_timestamp(time_performance))

    def print_items(self, print_folders=True, print_files=True, print_skipped_items=True,
                    filter_path="", filter_sign=">=", filter_size=0, crawl_deep=True):
        """
        This method is used to print the files and folders that were found during the crawling process.
        It also prints the number of files and folders that were listed.
        The method can be customized to print only files, only folders, or both.
        It can also filter the files and folders based on text and size.
        At the end of the method, it calls the _show_time method to print the time taken for the crawling process.

        :param print_folders: A boolean value that determines whether to print the folders or not. Default is True.
        :param print_files: A boolean value that determines whether to print the files or not. Default is True.
        :param filter_path: A string value that is used to filter the files and folders. Default is an empty string.
        :param filter_sign: A string value that is used to compare the sizes of the files and folders. Default is ">=".
        :param filter_size: An integer value that is used to filter the files and folders based on their sizes. Default is 0.
        :param print_sizes: A boolean value that determines whether to work with sizes or not. Default is False.
        :param read_out_saved_files: A boolean value that determines whether to read out the saved files and folders from the txt files. Default is False.

        :returns: None
        """

        if print_files:
            self._print_data(self.files, filter_path, filter_size,
                             item_type=ItemType.FILES, sign=filter_sign, crawl_deep=crawl_deep)
        if print_folders:
            self._print_data(self.folders, filter_path, filter_size,
                             item_type=ItemType.FOLDERS, sign=filter_sign, crawl_deep=crawl_deep)
        if print_skipped_items:
            self._print_data(self.skipped, filter_path, filter_size,
                             item_type=ItemType.SKIPPED, sign=filter_sign, crawl_deep=crawl_deep)

    # region implement later

    def read_content_of_file(self, path: str, filter_: str = "", print_=True):
        """
        This function reads the content of a file and prints the lines that contain the filter string.

        :param path: The path of the file that needs to be read.
        :param filter_: Filter string that filters out the lines.
        :return: None
        """

        array_ = []

        with open(path, FileOps.READ_MODE, encoding=FileOps.ENCODING) as file:
            for line in file:
                if filter_ in line:
                    if print_:
                        print(line.strip())
                    array_.append(line.strip())

        return array_

    def compare_saved_crawls(self, path1: str, path2: str, print_=True):
        set1 = set(self.read_content_of_file(path1, print_=False))
        set2 = set(self.read_content_of_file(path2, print_=False))

        array_difference = list(set1.symmetric_difference(set2))

        # Optionally print the differences
        if print_:
            for item in array_difference:
                print(item)

        return array_difference

    # endregion

    def _process_item_for_multiprocessing_pool(self, path_tuple: tuple[str, bool]) -> [tuple, bool]:
        """
        This method is used to process the items in the multiprocessing pool.

        :param path_tuple: Tuple containing path and boolean which determines if the path is a file or a folder.
        """
        path, is_file = path_tuple
        is_folder = not is_file
        item_path = os.path.join(self.path, path) if self.path not in path else path
        last_change = self._get_last_change_of_item(item_path)
        size = self._get_size_of_item(item_path, get_size_folder=is_folder)
        size_readable, size_total = self._resolve_sizes(size)
        data_complete = (item_path, last_change, size_readable, size_total)

        return data_complete, is_folder

    def _resolve_sizes(self, size: int | float) -> tuple[str, str] | tuple[float, float]:
        """
        This method is used to resolve the sizes of the files and folders.
        Either get readable sizes or get nan values.

        :param size: Data to be resolved.
        """
        if size is not NONE:
            size_readable, size_total = self._convert_bytes_to_readable_format(size)
        else:
            size_readable, size_total = NONE, NONE
        return size_readable, size_total

    def _crawl_items(self, path: str, go_deep: bool):
        """
        Crawls through the folder at the given path, with the option to go deeper into subdirectories,
        using multiprocessing to calculate the sizes of files and folders.

        :param path: The path of the folder that needs to be crawled.
        :param go_deep: A boolean value that determines whether to go deep into subdirectories or not.
        """
        if go_deep:
            print(self._get_current_time(), Messages.DEEP_CRAWL)
            paths = self._crawl_deep(path)
        else:
            print(self._get_current_time(), Messages.SHALLOW_CRAWL)
            paths = self._crawl_shallow(path)

        # Use multiprocessing Pool to handle item processing
        print(self._get_current_time(), Messages.STARTING_MULTI_PROCESSING)

        # todo: check what is returned from the pool
        with Pool() as pool:
            results = pool.map(self._process_item_for_multiprocessing_pool, paths)

        print(self._get_current_time(), Messages.DATAFRAME_PREPARATION)
        return pd.DataFrame(results)

    # todo: implement filtering, refactor
    def _print_data(self, container: pd.DataFrame, filter_path: str, filter_size: int, item_type: str,
                    sign: str = ">=", filter_date=None, crawl_deep=True):
        if container.empty:
            return

        print_total_size = not (crawl_deep and item_type == ItemType.FOLDERS)

        if item_type == ItemType.SKIPPED:
            container = self._filter_subdirectories(container=container, column=COLUMN_NAMES[0])

        # self._print_in_tabular_format(container[self.switched_column_names])
        print(self._tabulate_data(container))

        if not item_type == ItemType.SKIPPED:
            split = container[COLUMN_NAMES[3]].str.split(" ").str[1]
            sum_of_bytes = split.astype(np.int64).sum()
            size_short, size_long = self._convert_bytes_to_readable_format(sum_of_bytes)
            print(*self._get_crawl_summary(print_total_size, size_short, size_long))

    # region Static Methods
    @staticmethod
    def load_crawled_data(container: pd.DataFrame, item_type: str) -> pd.DataFrame:
        """
        This will read out saved files if no crawling was done in current run of a program.

        :param container: Dataframe that is going to be checked if it is empty.
        :param item_type: A string value that determines which file to read out.
        """
        if container.empty:
            item_path = os.path.join(SavedCrawls.SAVED_CRAWLS_FOLDER, f"{item_type}{SavedCrawls.EXTENSION}")
            return pd.read_csv(item_path)
        return container

    @staticmethod
    def _get_crawl_summary(print_total_size: bool, size_short, size_long) -> tuple:
        """
        This method is used to print the summary of the crawled data.

        :param size_long: Raw size of the crawled data in bytes.
        :param print_total_size: This will filter out dataframe which has recursive paths.
        :return:
        """
        if print_total_size:
            return Messages.NR_OF_CRAWLED_DATA, size_short, size_long, "\n\n"
        else:
            return "\n",

    @staticmethod
    def _filter_data(files: pd.DataFrame, folders: pd.DataFrame, empty_dataframe: dict, column: str) -> tuple[
        pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        This method is used to prepare the skipped items dataframe out of the files and folders dataframes.
        """
        nan_files = pd.DataFrame(empty_dataframe)
        nan_folders = pd.DataFrame(empty_dataframe)

        if not files.empty:
            is_na_filter = files[column].isna()
            nan_files = files[is_na_filter]
            files = files[~is_na_filter].copy()

        if not folders.empty:
            is_na_filter = folders[column].isna()
            nan_folders = folders[is_na_filter]
            folders = folders[~is_na_filter].copy()

        skipped_items = pd.concat([nan_files, nan_folders], ignore_index=True)
        return files, folders, skipped_items

    @staticmethod
    def _get_crawled_data(dataframe: pd.DataFrame, is_folder: bool):
        column_with_bools = dataframe[1]
        filter_ = column_with_bools == is_folder
        items = dataframe[filter_].copy()
        items = items.drop(columns=1)
        unpacked = items[0].apply(pd.Series)
        unpacked.columns = COLUMN_NAMES
        return pd.DataFrame(unpacked)

    @staticmethod
    def _crawl_shallow(path: str) -> list[tuple[str, bool]]:
        """
        Crawls through the folder at the given path without going into subfolders.
        """
        result = []
        items = os.listdir(path)
        for item in items:
            item_path = os.path.join(path, item)
            is_folder = os.path.isdir(item_path)
            result.append((item_path, not is_folder))
        return result

    @staticmethod
    def _crawl_deep(path: str) -> list[tuple[str, bool]]:
        """
        Crawls through the folder at the given path and its subfolders.
        """
        result = []
        for root, folders, files in os.walk(path):
            for file in files:
                file_path = os.path.join(root, file)
                result.append((file_path, True))
            for folder in folders:
                folder_path = os.path.join(root, folder)
                result.append((folder_path, False))
        return result

    @staticmethod
    def _save_crawl_result(path: str, container: pd.DataFrame) -> None:
        """
        This method is used to save the crawled dataframe into a csv file.
        :param path: The path of the file where the dataframe needs to be saved.
        :param container: Dataframe that needs to be saved.
        """

        if os.path.exists(path):
            os.remove(path)
        container.to_csv(path, index=False)

    @staticmethod
    def _get_size_of_item(path: str, get_size_folder: bool) -> int | float:
        """
        Calculate the size of a file or a folder. If get_size_folder is True, it calculates
        the size of the folder at the given path by summing the sizes of all files in the
        folder and its subfolders. Otherwise, it calculates the size of the file.
        The sizes are returned as bytes in int type.

        :param path: The path of the file or folder whose size needs to be calculated.
        :param get_size_folder: Boolean indicating whether the size of a folder (True) or file (False) should be calculated.
        """
        size_bytes = 0
        try:
            if get_size_folder:
                for root, _, files in os.walk(path):
                    for file in files:
                        file_path = os.path.join(root, file)
                        size_bytes += os.path.getsize(file_path)
            else:
                # get size of file
                size_bytes = os.path.getsize(path)
        except FileNotFoundError:
            return NONE
        return size_bytes

    @staticmethod
    def _tabulate_data(container: pd.DataFrame) -> str:
        """
        This method is used to return the pandas container in a tabular format.
        :param container: Dataframe that is going to be printed in a pretty tabular format.
        """

        return tabulate(container, headers=TABLE_HEADER, tablefmt=TABLE_FORMAT)

    @staticmethod
    def _get_last_change_of_item(path: str) -> datetime.datetime | float:
        """
        This private method is used to get the last change of a file.

        :param path: The path of the file whose last change needs to be calculated.
        """
        try:
            return datetime.datetime.fromtimestamp(os.path.getmtime(path))
        except Exception:
            return NONE

    @staticmethod
    def _filter_subdirectories(container: pd.DataFrame, column: str) -> pd.DataFrame:
        """
        This method filters a given row if a path at that row is contained
        as a substring in any other path in the dataframe.

        Example when we want to filter entries in column "Path":
        index, Path, Column2, Column3, ...
        0, C:/Users, X, X, ...                      <- This row will be filtered.
        1, C:/Users/Subfolder, X, X, ...            <- This row will be filtered.
        2, C:/Users/Subfolder/Subfolder2, X, X, ...


        :param container: The dataframe to filter.
        """

        # Sort dataframe by paths column to ensure that shorter paths come before their potential subdirectories
        container = container.sort_values(by=column, ascending=True)

        # get only the paths from the dataframe as a list
        paths = container[column].tolist().copy()

        # Remove paths that are subdirectories of other paths
        for i, path in enumerate(paths):
            path: str
            # Here we are checking if a previous path is a substring of any following path in the list
            for path_following in paths[i + 1:]:
                if path in path_following:
                    # Drop complete row in original dataframe
                    filter_ = container[column] == path
                    index = container[filter_].index
                    container.drop(index, inplace=True)

        return container

    @staticmethod
    def _get_time_performance(timer_start) -> float:
        """
        This method is used to calculate the difference between the start and end time.
        """
        timer_end = time.perf_counter()
        return timer_end - timer_start

    @staticmethod
    def _get_current_time() -> datetime.datetime:
        """
        This method is used to get the current time.
        """
        return datetime.datetime.now()

    @staticmethod
    def _convert_bytes_to_readable_format(size: int | float) -> tuple[str, str]:
        """
        This private method is used to convert the size from bytes to a more readable format.
        The size is converted to the highest unit that is less than 1024.
        The units used are Bytes (B), Kilobytes (KB), Megabytes (MB), Gigabytes (GB), and Terabytes (TB).
        The colors used are red for B, yellow for KB, green for MB, blue for GB, and cyan for TB.
        The sizes are returned as strings with the color formatting and the unit.

        Example of returned values:
        1.00KB 1024B

        Example of color formatting:
        \033[0;30;10m{your_text}\033[0m
        \033[0 - Escape character to start the sequence and reset all text formatting attributes.
        30 - Sets the text color.(Can be a different number for different colors)
        10 - Sets the background color. (Can be a different number for different colors)
        m - Ends the control sequence.

        :param size: The size in bytes that needs to be converted.
        """

        size_adjusted = size
        COLORS = [Fore.RED, Fore.YELLOW, Fore.GREEN, Fore.BLUE, Fore.CYAN]
        UNITS = ["B", "KB", "MB", "GB", "TB"]

        for color, unit in zip(COLORS, UNITS):
            if size_adjusted < 1024:
                size_short = f"{color}{size_adjusted:.2f}{unit}{Style.RESET_ALL}"
                size_long = f"{color} {size} {Style.RESET_ALL}"

                return size_short, size_long
            size_adjusted /= 1024

    @staticmethod
    def _format_timestamp(seconds: float) -> str:
        """
        This method is used to format the duration in seconds to a more readable format.

        :param seconds: The duration in seconds that needs to be formatted.
        """
        SEC_PER_YEAR = 31536000  # 365*24*60*60
        SEC_PER_DAY = 86400  # 24*60*60
        SEC_PER_HR = 3600  # 60*60
        SEC_PER_MIN = 60

        years, seconds = divmod(seconds, SEC_PER_YEAR)
        days, seconds = divmod(seconds, SEC_PER_DAY)
        hours, seconds = divmod(seconds, SEC_PER_HR)
        minutes, seconds = divmod(seconds, SEC_PER_MIN)

        LETTER_S = "s"
        EMPTY_STRING = ""
        yrs_s = LETTER_S if years > 1 else EMPTY_STRING
        dys_s = LETTER_S if days > 1 else EMPTY_STRING
        hrs_s = LETTER_S if hours > 1 else EMPTY_STRING
        min_s = LETTER_S if minutes > 1 else EMPTY_STRING
        sec_s = LETTER_S if seconds > 1 else EMPTY_STRING

        years = f"{years:.0f} Year{yrs_s} " if years > 0 else EMPTY_STRING
        days = f"{days:.0f} Day{dys_s} " if days > 0 else EMPTY_STRING
        hours = f"{hours:.0f} Hour{hrs_s} " if hours > 0 else EMPTY_STRING
        minutes = f"{minutes:.0f} Minute{min_s} " if minutes > 0 else EMPTY_STRING
        seconds = f"{seconds:.4f} Second{sec_s}" if seconds > 0 else EMPTY_STRING

        return years + days + hours + minutes + seconds

    @staticmethod
    def _initialize_files() -> None:
        """
        Ensure all necessary directories and files are created.
        """
        os.makedirs(SavedCrawls.SAVED_CRAWLS_FOLDER, exist_ok=True)
        for txt_file in (SavedCrawls.FILES, SavedCrawls.FOLDERS, SavedCrawls.SKIPPED):
            # Create empty txt files if they don't exist or just append empty string if they do exist
            open(txt_file, FileOps.APPEND_MODE, encoding=FileOps.ENCODING).close()
    # endregion

    # todo: check documentation
    # todo: create a readout option with filter across multiple files
    # todo: check that private methods do each only one thing. Isolate as much outside elements as possible.



