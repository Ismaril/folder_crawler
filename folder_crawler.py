import datetime
import os
import time
import pandas as pd
import numpy as np

from tabulate import tabulate
from structures import ItemType, SavedCrawls, Messages, FileOps, ColorFormatting, ByteSize
from multiprocessing import Pool
from colorama import init, Fore, Back, Style

# region Constants
NONE = np.nan

COLUMN_NAMES = ["Path", "Changed", "Size readable", "Size bytes"]

# TODO: You can switch the column names if you want to have a different order in the table.
#  Just look with ctrl+f for "SWITCHED_COLUMN_NAMES" and uncomment the line. Comment then the original one.
SWITCHED_COLUMN_NAMES = ["Path", "Size bytes", "Size readable", "Changed"]

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
    The FolderCrawler class is used to crawl through a folder and its sub-folders and prints the paths with its properties.
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

    def main(self, print_folders=True, print_files=True, print_skipped_items=True,
             crawl=True, crawl_deep=True, filter_path="", filter_size=0, filter_size_sign=">=",
             filter_date=datetime.datetime.min, filter_date_sign=">="):
        """
        This method is the main entry point into the FolderCrawler.

        :param print_folders: A boolean value that determines whether to print the folders or not.
        :param print_files: A boolean value that determines whether to print the files or not.
        :param print_skipped_items: A boolean value that determines whether to print the skipped items or not.
        :param filter_path: A string value that is used to filter the files and folders.
        :param filter_size: An integer value that is used to filter the files and folders based on their sizes.
        :param filter_size_sign: A string value that is used together with parameter filter_size.
        :param filter_date: A datetime value that is used to filter the files and folders based on their last change date.
        :param filter_date_sign: A string value that is used together with parameter filter_date.
        :param crawl_deep: A boolean value that determines whether to go deep into subdirectories or not.
        :param crawl: A boolean value that determines whether to crawl or not.
        """

        # INITIALIZE THE STORAGE FOR CRAWLING RESULTS
        self.initialize_storage(SavedCrawls.ROOT,
                                *(SavedCrawls.FILES, SavedCrawls.FOLDERS, SavedCrawls.SKIPPED))
        if crawl:
            # CRAWL
            dataframe = self.crawl_items(self.path, crawl_deep)

            # PREPARE DATAFRAMES
            self.prepare_dataframes(dataframe)

            # SAVE DATAFRAMES
            self.save_dataframes()

        # LOAD DATAFRAMES
        self.load_dataframes()

        # PRINT DATAFRAMES
        self.print_dataframes(print_folders, print_files, print_skipped_items, filter_path, filter_size,
                              filter_size_sign, filter_date, filter_date_sign, crawl_deep)

        # PRINT TIME PERFORMANCE
        time_performance = self.get_time_performance(self.timer)
        print("\n" + Messages.WHOLE_PROCES_TOOK, self._format_timestamp(time_performance))

    def prepare_dataframes(self, dataframe):
        """
        This high-level wrapper method is used to prepare the crawled data into the dataframes.
        """
        self.folders = self._get_crawled_data(dataframe, is_folder=True)
        self.files = self._get_crawled_data(dataframe, is_folder=False)
        self.files, self.folders, self.skipped = self._filter_data(
            self.files, self.folders, empty_dataframe=INITIAL_DATAFRAME, column=COLUMN_NAMES[3])

    def save_dataframes(self):
        """
        This high-level wrapper method is used to save the crawled data into the files.
        """
        item_types = (ItemType.FILES, ItemType.FOLDERS, ItemType.SKIPPED)
        containers = (self.files, self.folders, self.skipped)
        paths = (SavedCrawls.FILES, SavedCrawls.FOLDERS, SavedCrawls.SKIPPED)

        for item_type, container, path in zip(item_types, containers, paths):
            print(self._get_current_time(), Messages.SAVING_RESULTS, item_type.upper())
            self._save_result(path, container)

    def load_dataframes(self):
        """
        This high-level wrapper method is used to load the crawled data.
        """
        self.files = self.load_crawled_data(self.files, ItemType.FILES,
                                            SavedCrawls.ROOT, SavedCrawls.EXTENSION)
        self.folders = self.load_crawled_data(self.folders, ItemType.FOLDERS,
                                              SavedCrawls.ROOT, SavedCrawls.EXTENSION)
        self.skipped = self.load_crawled_data(self.skipped, ItemType.SKIPPED,
                                              SavedCrawls.ROOT, SavedCrawls.EXTENSION)

    def print_dataframes(self, print_folders: bool, print_files: bool, print_skipped_items: bool,
                         filter_path: str, filter_size: int, filter_size_sign: str,
                         filter_date: datetime.datetime, filter_date_sign: str, crawl_deep: bool):
        """
        This high-level wrapper method is used to print the files and folders that were found during the crawling process.
        Regarding parameter description, check out the main method.
        """

        if print_files:
            self._print_data(self.files, filter_path, filter_size, filter_size_sign,
                             filter_date, filter_date_sign, ItemType.FILES, crawl_deep)
        if print_folders:
            self._print_data(self.folders, filter_path, filter_size, filter_size_sign,
                             filter_date, filter_date_sign, ItemType.FOLDERS, crawl_deep)
        if print_skipped_items:
            self._print_data(self.skipped, filter_path, filter_size, filter_size_sign,
                             filter_date, filter_date_sign, ItemType.SKIPPED, crawl_deep)

    def _get_path_with_properties(self, path_tuple: tuple[str, bool]) -> tuple[tuple, bool]:
        """
        This method gets all the properties of the path and returns them as a tuple with a boolean value.

        :param path_tuple: Tuple containing path+it's properties and boolean which determines if the path is a file or
        a folder.
        """
        path, is_folder = path_tuple
        item_path = os.path.join(self.path, path) if self.path not in path else path
        last_change = self._get_last_change_of_item(item_path)
        size = self._get_size_of_item(item_path, get_size_folder=is_folder)
        size_readable, size_total = self._convert_bytes_to_readable_format(size, ColorFormatting.COLORS,
                                                                           ColorFormatting.UNITS, Style.RESET_ALL,
                                                                           function=self._color_format_string)
        data_complete = (item_path, last_change, size_readable, size_total)

        return data_complete, is_folder

    def crawl_items(self, path: str, go_deep: bool):
        """
        Crawls through the folder at the given path, with the option to go deeper into subdirectories,
        using multiprocessing.

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

        # How this works:
        # Into the pool.map method we pass the method which performs the operations and as a second parameter items to
        # pass into function at first parameter.
        # The pool then distributes the items to the available cores and processes them in parallel.
        # Result will be just as if you normally put the items into the function.
        with Pool() as pool:
            results = pool.map(self._get_path_with_properties, paths)

        print(self._get_current_time(), Messages.DATAFRAME_PREPARATION)
        return pd.DataFrame(results)

    def _print_data(self, container: pd.DataFrame, filter_path: str, filter_size: int, filter_size_sign: str,
                    filter_date: datetime.datetime, filter_date_sign: str, item_type: str, crawl_deep: bool):
        """
        This method is used to print the data from a given dataframe.
        For parameter description, check out the main method.
        """

        if container.empty:
            return
        path_sizes = self._get_ints_from_str_dataframe_column(container, COLUMN_NAMES[3])
        container = self._global_dataframe_filter(container, filter_date, filter_date_sign, filter_path,
                                                  filter_size, filter_size_sign, item_type, path_sizes)

        print(item_type.upper())
        # self._tabulate_data(container[SWITCHED_COLUMN_NAMES])
        print(self._tabulate_data(container))
        self._print_crawl_summary(crawl_deep, item_type, path_sizes)

    def _print_crawl_summary(self, crawl_deep, item_type: str, path_sizes: pd.Series):
        """
        This method is used to print the summary of the crawled data.

        :param crawl_deep: This parameter switches the summary printing on or off.
        :param item_type: The type of the item that is being printed.
        :param path_sizes: The size corresponding to each path in dataframe.
        """
        if not item_type == ItemType.SKIPPED:
            print_summary = not (crawl_deep and item_type == ItemType.FOLDERS)
            sum_of_bytes = path_sizes.astype(np.int64).sum()
            size_readable, size_raw = self._convert_bytes_to_readable_format(
                sum_of_bytes, ColorFormatting.COLORS, ColorFormatting.UNITS, Style.RESET_ALL, self._color_format_string)
            print(*self._get_crawl_summary(print_summary, Messages.NR_OF_CRAWLED_DATA, size_readable, size_raw))

    def _global_dataframe_filter(self, container: pd.DataFrame, filter_date: datetime.datetime,
                                 filter_date_sign: str, filter_path: str, filter_size: int,
                                 filter_size_sign: str, item_type: str, path_sizes: pd.Series):
        """
        This is a helper method to group all the filters in one place.
        """
        if item_type == ItemType.SKIPPED:
            container = self._filter_subdirectories(container, COLUMN_NAMES[0])

        container = self._filter_paths(container, filter_path, COLUMN_NAMES[0])

        if item_type in (ItemType.FILES, ItemType.FOLDERS):
            container = self._filter_sizes(container, filter_size, filter_size_sign, path_sizes)
            container = self._filter_last_change(container, filter_date, filter_date_sign, COLUMN_NAMES[1])

        return container

    # region Static Methods
    @staticmethod
    def _filter_paths(container: pd.DataFrame, filter_path: str, column: str) -> pd.DataFrame:
        """
        This method is used to filter the paths in the dataframe based on the filter path.

        :param container: The dataframe that needs to be filtered.
        :param filter_path: The path that is used to filter the dataframe.
        :param column: The column in which a filter is used to filter the dataframe.
        """

        filter_ = container[column].str.contains(filter_path)
        return container[filter_]

    @staticmethod
    def _filter_sizes(container: pd.DataFrame, filter_size: int, sign: str, numbers: pd.Series) -> pd.DataFrame:
        """
        This method is used to filter the paths in the dataframe based on the filter size.

        :param container: The dataframe that needs to be filtered.
        :param filter_size: The size that is used to filter the dataframe.
        :param sign: The sign that is used to compare the sizes.
        :param numbers: The numbers that are used to filter the dataframe.
        """

        filter_ = object  # I just put it here because at return it does not see the filter_ variable

        if sign == ">=":
            filter_ = numbers >= filter_size
        elif sign == "<=":
            filter_ = numbers <= filter_size

        return container.loc[filter_]

    @staticmethod
    def _filter_last_change(container: pd.DataFrame, filter_date: datetime.datetime,
                            sign: str, column: str) -> pd.DataFrame:
        """
        This method is used to filter the paths in the dataframe based on the filter date.

        :param container: The dataframe that needs to be filtered.
        :param filter_date: The date that is used to filter the dataframe.
        :param sign: The sign that is used to compare the dates.
        :param column: The column in which a filter is used to filter the dataframes.
        """

        filter_ = object  # I just put it here because at return it does not see the filter_ variable

        if sign == ">=":
            filter_ = container[column] >= filter_date
        elif sign == "<=":
            filter_ = container[column] <= filter_date

        return container[filter_]

    @staticmethod
    def _get_ints_from_str_dataframe_column(container: pd.DataFrame, column: str) -> pd.Series:
        """
        This method is used to extract integers from the strings in a column of a given dataframe.

        :param container: The dataframe that contains the data.
        :param column: The column in which the integers are extracted.
        """
        split_formated_strings = container[column].str.split(" ")
        get_string_digits = split_formated_strings.str[1]
        numbers = get_string_digits.astype(np.int64)
        return numbers

    @staticmethod
    def load_crawled_data(container: pd.DataFrame, item_type: str, path: str, extension: str) -> pd.DataFrame:
        """
        This will read out a data from csv.

        :param container: Dataframe that is going to be checked if it is empty.
        :param item_type: A string value that determines which file to read out.
        :param path: The path of the file that needs to be read.
        :param extension: The extension of the file that needs to be read.
        """
        if container.empty:
            item_path = os.path.join(path, f"{item_type}{extension}")
            return pd.read_csv(item_path)
        return container

    @staticmethod
    def _get_crawl_summary(print_: bool, message: str, size_readable: str | int, size_raw: str | int) -> tuple:
        """
        This method is used to print the summary of the crawled data.

        :param print_: A boolean value that determines whether to print the summary or not.
        :param message: The message that is printed.
        :param size_readable: The size readable which that is printed.
        :param size_raw: The raw size that is printed.
        """
        if print_:
            return message, size_readable, size_raw, "\n\n"
        else:
            return "\n",

    @staticmethod
    def _filter_data(files: pd.DataFrame, folders: pd.DataFrame,
                     empty_dataframe: dict, column: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        This method filters out the rows in both files and folders dataframes and returns the filtered dataframes,
        where the first dataframes no longer have NaN values and the third dataframe contains only the NaN items.

        :param files: The dataframe that contains the files' data.
        :param folders: The dataframe that contains the folders' data.
        :param empty_dataframe: An empty dataframe that is used to create a new dataframe.
        :param column: The column in which a filter is used to filter the dataframes.
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
        """
        This method is used to unpack the dataframe that contains the crawled data.

        :param dataframe: The dataframe that contains the crawled data.
        :param is_folder: A boolean value that determines whether the dataframe should filter folders or files.
        """
        column_with_bools = dataframe[1]
        filter_ = column_with_bools == is_folder
        items = dataframe[filter_].copy()
        items = items.drop(columns=1)
        unpacked = items[0].apply(pd.Series)
        unpacked.columns = COLUMN_NAMES
        return pd.DataFrame(unpacked).reset_index(drop=True)

    @staticmethod
    def _crawl_shallow(path: str) -> list[tuple[str, bool]]:
        """
        Crawls through the folder at the given path without going into subfolders.

        :param path: The path of the folder that needs to be crawled.
        """
        result = []
        items = os.listdir(path)
        for item in items:
            item_path = os.path.join(path, item)
            is_folder = os.path.isdir(item_path)
            result.append((item_path, is_folder))
        return result

    @staticmethod
    def _crawl_deep(path: str) -> list[tuple[str, bool]]:
        """
        Crawls through the folder at the given path and its subfolders.

        :param path: The path of the folder that needs to be crawled.
        """
        result = []
        for root, folders, files in os.walk(path):
            for file in files:
                file_path = os.path.join(root, file)
                result.append((file_path, False))
            for folder in folders:
                folder_path = os.path.join(root, folder)
                result.append((folder_path, True))
        return result

    @staticmethod
    def _save_result(path: str, container: pd.DataFrame) -> None:
        """
        This method is used to save the dataframe into a csv file.

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
    def get_time_performance(timer_start) -> float:
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
    def _convert_bytes_to_readable_format(size: int | float,
                                          colors: list[str],
                                          units: list[str],
                                          reset_formatting: str,
                                          function) -> tuple[str, str] | tuple[float, float]:
        """
        This private method is used to convert the size from bytes to a more readable format.
        The sizes are returned as strings with the color formatting and the unit.

        :param size: The size in bytes that needs to be converted.
        :param colors: A list of colors that are used to format the size.
        :param units: A list of units that are used to format the size.
        :param reset_formatting: Resetting str sequence to return formatting back to default.
        :param function: Function that is used to format the item with the given color and reset formatting.
        """
        if size is NONE:
            return NONE, NONE

        size_adjusted = size
        for color, unit in zip(colors, units):
            if size_adjusted < ByteSize.KILOBYTE:
                size_readable = function(*(color, size_adjusted, unit, reset_formatting, False))
                size_raw = function(*(color, size, None, reset_formatting, True))

                return size_readable, size_raw
            size_adjusted /= ByteSize.KILOBYTE

    @staticmethod
    def _color_format_string(*args) -> str:
        """
        This method is used to format the item with the given color and reset formatting.
        Since it is planned to use this method as an argument in other methods, I decided to use *args here.
        Currently, after unpacking we expect:
        color, size, unit, reset_formatting, format_long, *_ = args
        where *_ is the rest of the arguments that are not used.
        """
        color, size, unit, reset_formatting, format_long, *_ = args

        if format_long:
            # LONG NUMBER (The bytes in their original digit form are kept the same here)
            # The spaces around the size are here as a separator for splitting into array.
            # Example: colorFormating 1024 resetFormatting
            return f"{color} {size} {reset_formatting}"
        else:
            # TRANSFORMED TO SHORT NUMBER
            # Example: colorFormating1.00KBresetFormatting
            return f"{color}{size:.2f}{unit}{reset_formatting}"

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
    def initialize_storage(root_folder: str, *paths: str) -> None:
        """
        Ensure all necessary directories and files are created.

        :param root_folder: Path to root folder that needs to be created if it does not exist.
        :param paths: Paths to files that need to be created if they do not exist.
        """
        os.makedirs(root_folder, exist_ok=True)
        for file in paths:
            # Create empty files if they don't exist or just append empty string if they do exist
            open(file, FileOps.APPEND_MODE, encoding=FileOps.ENCODING).close()

    # endregion

    # region Work in progress
    # todo: work in progress, refactor, create tests, call this from main
    @staticmethod
    def read_content_of_one_file(path: str, filter_file_content: str = "", print_=True):
        """
        This function reads the content of a file and prints the lines that contain the filter string.

        :param path: The path of the file that needs to be read.
        :param filter_file_content: Filter string that filters out the lines in a given file.
        :param print_: Boolean value that determines whether to print the lines or just return.
        :return: None
        """

        file_lines_that_passed_filter = []
        with open(path, FileOps.READ_MODE, encoding=FileOps.ENCODING) as file:
            for line in file:
                if filter_file_content in line:
                    if print_:
                        print(line.strip())
                    file_lines_that_passed_filter.append(line.strip())
        return file_lines_that_passed_filter

    def read_content_of_multiple_files(self, filter_path_name="", filter_file_content="", print_=True):
        """
        This function reads the content of multiple files and prints the lines that contain the filter string.
        Try to run this function on as few files as possible, because you can get a lot of data into console.

        :param filter_path_name: Filter string that filters out array of file paths.
        :param filter_file_content: Filter string that filters out the lines in each file that passes the filter 'filter_path_name'.
        :param print_: Boolean value that determines whether to print the lines.
        """
        paths_to_read_out = []
        file_contents_from_all_filtered_paths = []

        # Get relevant paths
        for path in self.files[COLUMN_NAMES[0]]:
            if filter_path_name in path:
                paths_to_read_out.append(path)

        # Read out the content of the files
        for path in paths_to_read_out:
            content_of_one_file = self.read_content_of_one_file(path, filter_file_content, print_=False)
            file_contents_from_all_filtered_paths.append(content_of_one_file)

            # Optionally print the content of the files
            if print_:
                for line in content_of_one_file:
                    print(line)
                print(Messages.SEPARATOR)

        return file_contents_from_all_filtered_paths

    def compare_saved_crawls(self, path1: str, path2: str, print_=True):
        """
        This method compares two saved crawls and prints the differences.

        :param path1: The path of the first saved crawl.
        :param path2: The path of the second saved crawl.
        :param print_: Boolean value that determines whether to print the differences or just return.
        """
        set1 = set(self.read_content_of_one_file(path1, print_=False))
        set2 = set(self.read_content_of_one_file(path2, print_=False))

        array_difference = list(set1.symmetric_difference(set2))

        # Optionally print the differences
        if print_:
            for item in array_difference:
                print(item)

        return array_difference

    # endregion
