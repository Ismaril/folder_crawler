import datetime
import os
import time
import pandas as pd
import numpy as np

from tabulate import tabulate
from structures import ItemType, SavedCrawls, Messages, FileOps
from multiprocessing import Pool


class FolderCrawler:
    """
    The FolderCrawler class is used to crawl through a folder and its sub-folders and prints the paths with sizes.
    """
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

    # region Constructor
    def __init__(self, path: str, crawl_deep: bool):
        """
        This is the constructor method for the FolderCrawler class.

        :param path: The path of the folder that needs to be crawled.
        """

        self.path = path
        self.crawl_deep = crawl_deep
        self.timer = time.perf_counter()  # start the timer

        # create a dataframe with column names, but no data
        self.files = pd.DataFrame(self.INITIAL_DATAFRAME)
        self.folders = pd.DataFrame(self.INITIAL_DATAFRAME)
        self.skipped = pd.DataFrame(self.INITIAL_DATAFRAME)

    # endregion

    # region Public Methods
    def crawl(self):
        """
        This method decides whether to perform a deep crawl or a shallow crawl based on the crawl_deep parameter.
        This means that the crawler will either stay in inputted folder or go deeper into the sub-folders.
        The result will be extracted either file of folder. Both of them will also contain its size.

        :param crawl_deep: A boolean value that determines whether to perform a deep crawl or a shallow crawl. Default is False.

        :return: None
        """
        self._initialize_files()

        if self.crawl_deep:
            self._crawl_items(go_deep=True)
        else:
            self._crawl_items(go_deep=False)

        # todo: move this into main or somehow fuse it with other dataframe preparations
        self._prepare_skipped_items_dataframe()

        # todo: move this into main
        self._save_crawl_result(path=SavedCrawls.FILES, container=self.files, item_type=ItemType.FILES)
        self._save_crawl_result(path=SavedCrawls.FOLDERS, container=self.folders, item_type=ItemType.FOLDERS)
        self._save_crawl_result(path=SavedCrawls.SKIPPED, container=self.skipped, item_type=ItemType.SKIPPED)

    def print_items(self, print_folders=True, print_files=True, print_skipped_items=True,
                    filter_path="", filter_sign=">=", filter_size=0):
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
            self._read_out_saved_files(container=self.files, item_type=ItemType.FILES)
            self._print_file_or_folder(self.files, filter_path, filter_size, item_type=ItemType.FILES, sign=filter_sign)
        if print_folders:
            self._read_out_saved_files(container=self.folders, item_type=ItemType.FOLDERS)
            self._print_file_or_folder(self.folders, filter_path, filter_size, item_type=ItemType.FOLDERS,
                                       sign=filter_sign)
        if print_skipped_items:
            self._read_out_saved_files(container=self.skipped, item_type=ItemType.SKIPPED)
            self._print_skipped_items(self.skipped)

        self._show_programs_time_performance()

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

    # region Private Methods

    # todo: get rid of the print inside of this method?
    def _prepare_skipped_items_dataframe(self) -> None:
        """
        This method is used to prepare the skipped items dataframe out of the files and folders dataframes.
        """
        nan_files = pd.DataFrame(self.INITIAL_DATAFRAME)
        nan_folders = pd.DataFrame(self.INITIAL_DATAFRAME)

        if not self.files.empty:
            nan_files = self.files[self.files[self.COLUMN_NAMES[3]].isna()]
            self.files = self.files[~self.files[self.COLUMN_NAMES[3]].isna()].copy()

        if not self.folders.empty:
            nan_folders = self.folders[self.folders[self.COLUMN_NAMES[3]].isna()]
            self.folders = self.folders[~self.folders[self.COLUMN_NAMES[3]].isna()].copy()

        self.skipped = pd.concat([nan_files, nan_folders], ignore_index=True)
        print(self._get_current_time(), Messages.DATAFRAME_PREPARATION_DONE, ItemType.SKIPPED.upper())

    def _process_item_for_multiprocessing_pool(self, path_tuple) -> [tuple, bool]:
        """
        This method is used to process the items in the multiprocessing pool.

        :param path_tuple: Tuple containing path and boolean which determines if the path is a file or a folder.
        """
        path, is_file = path_tuple
        is_folder = not is_file
        item_path = os.path.join(self.path, path) if self.path not in path else path
        last_change = self._get_last_change_of_item(item_path)
        size = self._get_size(item_path, get_size_folder=is_folder)
        size_readable, size_total = self._resolve_sizes(size)

        return self._arguments(item_path, last_change, size_readable, size_total), is_folder

    def _resolve_sizes(self, size: int | float) -> tuple[str, str] | tuple[float, float]:
        """
        This method is used to resolve the sizes of the files and folders.
        Either get readable sizes or get nan values.

        :param size: Data to be resolved.
        """
        if size is not self.NONE:
            size_readable, size_total = self._convert_bytes_to_readable_format(size)
        else:
            size_readable, size_total = self.NONE, self.NONE
        return size_readable, size_total

    def _crawl_items(self, go_deep=False):
        """
        Crawls through the folder at the given path, with the option to go deeper into subdirectories,
        using multiprocessing to calculate the sizes of files and folders.
        """
        if go_deep:
            print(self._get_current_time(), Messages.DEEP_CRAWL)
            paths = self._crawl_deep()
        else:
            print(self._get_current_time(), Messages.SHALLOW_CRAWL)
            paths = self._crawl_shallow()

        # Use multiprocessing Pool to handle item processing
        print(self._get_current_time(), Messages.STARTING_MULTI_PROCESSING)

        # todo: check what is returned from the pool
        with Pool() as pool:
            results = pool.map(self._process_item_for_multiprocessing_pool, paths)

        print(self._get_current_time(), Messages.DATAFRAME_PREPARATION)
        dataframe = pd.DataFrame(results)

        # todo: perhpas create a method for this
        for is_folder in [False, True]:
            items = dataframe[dataframe[1] == is_folder].copy()
            items = items.drop(columns=1)
            unpacked = items[0].apply(pd.Series)
            unpacked.columns = self.COLUMN_NAMES
            if is_folder:
                self.folders = pd.DataFrame(unpacked)
                print(self._get_current_time(), Messages.DATAFRAME_PREPARATION_DONE, ItemType.FOLDERS.upper())
            else:
                # is file
                self.files = pd.DataFrame(unpacked)
                print(self._get_current_time(), Messages.DATAFRAME_PREPARATION_DONE, ItemType.FILES.upper())



    def _crawl_shallow(self) -> list[tuple[str, bool]]:
        """
        Crawls through the folder at the given path without going into subfolders.
        """
        result = []
        items = os.listdir(self.path)
        for item in items:
            item_path = os.path.join(self.path, item)
            is_folder = os.path.isdir(item_path)
            result.append((item_path, not is_folder))
        return result

    def _crawl_deep(self) -> list[tuple[str, bool]]:
        """
        Crawls through the folder at the given path and its subfolders.
        """
        result = []
        for root, folders, files in os.walk(self.path):
            for file in files:
                file_path = os.path.join(root, file)
                result.append((file_path, True))
            for folder in folders:
                folder_path = os.path.join(root, folder)
                result.append((folder_path, False))
        return result

    # todo: think about how to resolve prints
    def _save_crawl_result(self, path: str, container: pd.DataFrame, item_type: str) -> None:
        """
        This method is used to save the crawled dataframe into a csv file.
        :param path: The path of the file where the dataframe needs to be saved.
        :param container: Dataframe that needs to be saved.
        :param item_type: Item type to resolve print message.
        """
        print(self._get_current_time(), Messages.SAVING_RESULTS, item_type.upper())
        if os.path.exists(path):
            os.remove(path)
        container.to_csv(path, index=False)
        print(self._get_current_time(), Messages.SAVING_RESULTS_DONE, item_type.upper())

    def _get_size(self, path: str, get_size_folder: bool) -> int | float:
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
            return self.NONE
        return size_bytes

    def _read_out_saved_files(self, container: pd.DataFrame, item_type: str) -> None:
        """
        This will read out saved files if no crawling was done in current run of a program.

        :param container: Dataframe that is going to be checked if it is empty.
        :param item_type: A string value that determines which file to read out.
        """
        if container.empty:
            item_path = os.path.join(SavedCrawls.SAVED_CRAWLS_FOLDER, f"{item_type}{SavedCrawls.EXTENSION}")

            if item_type is ItemType.FILES:
                self.files = pd.read_csv(item_path)
            elif item_type is ItemType.FOLDERS:
                self.folders = pd.read_csv(item_path)
            elif item_type is ItemType.SKIPPED:
                self.skipped = pd.read_csv(item_path)

    def _print_crawl_summary(self, size_long: int, print_total_size: bool) -> None:
        """
        This method is used to print the summary of the crawled data.

        :param size_long: Raw size of the crawled data in bytes.
        :param print_total_size: This will filter out dataframe which has recursive paths.
        :return:
        """
        if print_total_size:
            size_short, size_long = self._convert_bytes_to_readable_format(size_long)
            print(Messages.NR_OF_CRAWLED_DATA, size_short, size_long, end="\n\n")
        else:
            print("\n")

    def _print_skipped_items(self, container: pd.DataFrame) -> None:
        """
        This method is used to print the skipped items in a tabular format.

        :param container: Dataframe that is going to be printed in a pretty tabular format.
        """
        if container.empty:
            return
        container = self._filter_subdirectories(container=container)
        self._print_in_tabular_format(container)

    # todo: implement filtering
    def _print_file_or_folder(self, container: pd.DataFrame, filter_path: str, filter_size: int, item_type: str,
                              sign: str = ">=", filter_date=None):
        if container.empty:
            return

        print_total_size = not (self.crawl_deep and item_type == ItemType.FOLDERS)

        # In the column Size bytes, split the string and get the second element which is the size in bytes. The
        # positions before and after are the color formatting.
        split = container[self.COLUMN_NAMES[3]].str.split(" ").str[1]
        sum_of_bytes = split.astype(np.int64).sum()
        # self._print_in_tabular_format(container[self.switched_column_names])
        self._print_in_tabular_format(container)
        self._print_crawl_summary(sum_of_bytes, print_total_size)

    def _print_in_tabular_format(self, container: pd.DataFrame) -> None:
        """
        This method is used to print the files and folders in a tabular format.
        :param container: Dataframe that is going to be printed in a pretty tabular format.
        """

        print(tabulate(container, headers=self.TABLE_HEADER, tablefmt=self.TABLE_FORMAT))

    def _show_programs_time_performance(self) -> None:
        """
        This method is used to calculate the time since initialization
        of class till the call of this method.
        """
        resulting_time = self._format_duration(time.perf_counter() - self.timer)
        print("\n" + Messages.WHOLE_PROCES_TOOK, resulting_time)

    def _get_last_change_of_item(self, path: str) -> datetime.datetime | float:
        """
        This private method is used to get the last change of a file.

        :param path: The path of the file whose last change needs to be calculated.
        :return: The last change of the file as a datetime object.
        """
        try:
            return datetime.datetime.fromtimestamp(os.path.getmtime(path))
        except FileNotFoundError:
            return self.NONE

    def _filter_subdirectories(self, container: pd.DataFrame) -> pd.DataFrame:
        """
        This method filters a given row if a path at that row is contained
        as a substring in any other path in the dataframe. The relevant column
        is called "Path".

        Example:
        index, Path, Column2, Column3, ...
        0, C:/Users, X, X, ...                      <- This row will be filtered.
        1, C:/Users/Subfolder, X, X, ...            <- This row will be filtered.
        2, C:/Users/Subfolder/Subfolder2, X, X, ...


        :param container: The dataframe to filter.
        """

        # Sort dataframe by paths column to ensure that shorter paths come before their potential subdirectories
        container = container.sort_values(by=self.COLUMN_NAMES[0], ascending=True)

        # get only the paths from the dataframe as a list
        paths = container[self.COLUMN_NAMES[0]].tolist().copy()

        # Remove paths that are subdirectories of other paths
        for i, path in enumerate(paths):
            path: str
            # Here we are checking if a previous path is a substring of any following path in the list
            for path_following in paths[i + 1:]:
                if path in path_following:
                    # Drop complete row in original dataframe
                    container.drop(container[container[self.COLUMN_NAMES[0]] == path].index, inplace=True)

        return container

    # endregion

    # region Static Methods
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
        COLORS = (31, 33, 32, 34, 36)
        UNITS = ["B", "KB", "MB", "GB", "TB"]

        for color, unit in zip(COLORS, UNITS):
            if size_adjusted < 1024:
                size_short = f"\033[0;{color};10m{size_adjusted:.2f}{unit}\033[0m"
                size_long = f"\033[0;{color};10m {size} \033[0m"
                return size_short, size_long
            size_adjusted /= 1024

    @staticmethod
    def _format_duration(seconds: float) -> str:
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

        years = f"{years:.0f} Years, " if years > 0 else ""
        days = f"{days:.0f} Days, " if days > 0 else ""
        hours = f"{hours:.0f} Hours, " if hours > 0 else ""
        minutes = f"{minutes:.0f} Minutes, " if minutes > 0 else ""
        seconds = f"{seconds:.4f} Seconds" if seconds > 0 else ""

        return years + days + hours + minutes + seconds

    @staticmethod
    def _arguments(*args) -> tuple:
        """
        This method is used to return the parameters passed to it.
        This helps us to pass and get back dynamic number of parameters.

        :param args: The unlimited number of parameters that need to be returned.
        """
        return args

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

    # todo: check all the tests, they were written by AI
    # todo: check documentation
    # todo: create a interface executable from the command line
    # todo: create a readout option with filter across multiple files
    # todo: check that private methods do each only one thing. Isolate as much outside elements as possible.
    # todo: create a main method
    # todo: create a new repo for this project


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
    # cr = FolderCrawler(path=r"C:\Users\lazni\Downloads", crawl_deep=False)
    cr = FolderCrawler(path=r"C:\\", crawl_deep=False)
    cr.crawl()
    cr.print_items(True, True, True, filter_path="", filter_sign=">=", filter_size=0)
