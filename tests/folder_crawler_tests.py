import os
import time
import unittest
import datetime
import pandas as pd

from tabulate import tabulate
from colorama import Style, Fore
from test_helper import TestHelper
from folder_crawler import FolderCrawler, NONE, COLUMN_NAMES, TABLE_HEADER, TABLE_FORMAT
from structures import SavedCrawls, Messages, ColorFormatting, ByteSize, ItemType, FileOps, ByteUnit, ColoredBytes

# region constants
TEMP_DIR = "temp_dir"
SUB_DIR_1 = "sub_dir1"
SUB_DIR_2 = "sub_dir2"
TEMP_FILE_1 = "temp_file1.txt"
TEMP_FILE_2 = "temp_file2.txt"
TEST_TEXT = "This is a temporary file for testing."
CURRENT_DIRECTORY = "."

TEST_DICT = {
    COLUMN_NAMES[0]: ['C:/Users', 'C:/Users/Subfolder', 'C:/Users/Subfolder/Subfolder2'],
    COLUMN_NAMES[1]: [datetime.datetime(2022, 1, 1), datetime.datetime(2022, 2, 1), datetime.datetime(2022, 3, 1)],
    COLUMN_NAMES[2]: [ColoredBytes.ONE_KB_PRETTY, ColoredBytes.TWO_KB_PRETTY, ColoredBytes.THREE_KB_PRETTY],
    COLUMN_NAMES[3]: [ColoredBytes.ONE_KB_RAW, ColoredBytes.TWO_KB_RAW, ColoredBytes.THREE_KB_RAW]
}
TEST_DATAFRAME = pd.DataFrame(TEST_DICT)
RAW_INTEGERS_SERIES = pd.Series([1024, 2048, 3072], dtype='int64', name=COLUMN_NAMES[3])


# endregion

# region Integration tests
class FolderCrawlerTestsMain(unittest.TestCase):
    def setUp(self):
        # The path is here set in advance. During the initialization here, it does not exist yet.
        self.fc = FolderCrawler(path=TEMP_DIR)

    def test_main_1(self):
        # Prepare the test environment
        test_helper = TestHelper(TEMP_DIR, os.path.join(TEMP_DIR, TEMP_FILE_1))
        test_helper.create_test_paths(TEST_TEXT)

        # Run test
        self.fc.main()
        path_result = self.fc.files[COLUMN_NAMES[0]][0]
        size_raw_result = self.fc.files[COLUMN_NAMES[3]][0]

        # Clean up the test environment
        test_helper.delete_test_paths()
        test_helper.delete_saved_crawls()

        # Evaluate
        self.assertEqual(path_result, os.path.join(TEMP_DIR, TEMP_FILE_1))
        self.assertEqual(size_raw_result, ColoredBytes.THIRTYSEVEN_BYTES_RAW)

    def test_main_2(self):
        # Prepare the test environment
        test_helper = TestHelper(TEMP_DIR,
                                 os.path.join(TEMP_DIR, SUB_DIR_1),
                                 os.path.join(TEMP_DIR, SUB_DIR_1, TEMP_FILE_1))
        test_helper.create_test_paths(TEST_TEXT)

        # Run test
        self.fc.main()
        file_path_result = self.fc.files[COLUMN_NAMES[0]][0]
        file_size_raw_result = self.fc.files[COLUMN_NAMES[3]][0]
        folder_path_result = self.fc.folders[COLUMN_NAMES[0]][0]
        folder_size_raw_result = self.fc.folders[COLUMN_NAMES[3]][0]

        # Clean up the test environment
        test_helper.delete_test_paths()
        test_helper.delete_saved_crawls()

        # Evaluate
        self.assertEqual(file_path_result, os.path.join(TEMP_DIR, SUB_DIR_1, TEMP_FILE_1))
        self.assertEqual(file_size_raw_result, ColoredBytes.THIRTYSEVEN_BYTES_RAW)
        self.assertEqual(folder_path_result, os.path.join(TEMP_DIR, SUB_DIR_1))
        self.assertEqual(folder_size_raw_result, ColoredBytes.THIRTYSEVEN_BYTES_RAW)
        self.assertTrue(self.fc.skipped.empty)


class FolderCrawlerTestsGetPathWithProperties(unittest.TestCase):
    """
    In these tests we are not really focusing on details which the method returns.
    We just focus if the method returns the correct number of items and if the second item is boolean.
    """

    def setUp(self):
        self.fc = FolderCrawler(path=CURRENT_DIRECTORY)

    def test_get_path_with_properties_with_no_subdirectories1(self):
        # Prepare the test environment
        test_helper = TestHelper(TEMP_DIR, os.path.join(TEMP_DIR, TEMP_FILE_1))
        test_helper.create_test_paths(TEST_TEXT)
        IS_DIRECTORY = True

        # Run test
        result_tuple = self.fc._get_path_with_properties((TEMP_DIR, IS_DIRECTORY))
        nr_of_properties = len(result_tuple[0])
        is_directory_ = result_tuple[1]
        resulting_tuple = (nr_of_properties, is_directory_)
        expected_tuple = (len(COLUMN_NAMES), IS_DIRECTORY)

        # Clean up the test environment
        test_helper.delete_test_paths()

        # Evaluate
        self.assertEqual(resulting_tuple, expected_tuple)

    def test_get_path_with_properties_with_no_subdirectories2(self):
        # Prepare the test environment
        test_helper = TestHelper(TEMP_FILE_1)
        test_helper.create_test_paths(TEST_TEXT)
        is_directory = False

        # Run test
        result_tuple = self.fc._get_path_with_properties((TEMP_FILE_1, is_directory))
        nr_of_properties = len(result_tuple[0])
        is_directory_ = result_tuple[1]
        resulting_tuple = (nr_of_properties, is_directory_)
        expected_tuple = (len(COLUMN_NAMES), is_directory)

        # Clean up the test environment
        test_helper.delete_test_paths()

        # Evaluate
        self.assertEqual(resulting_tuple, expected_tuple)


# endregion

# region Unit tests

class FolderCrawlerTestsGetIntsFromStrDataFrameColumn(unittest.TestCase):
    def setUp(self):
        self.fc = FolderCrawler(path=CURRENT_DIRECTORY)
        self.fc.files = TEST_DATAFRAME

    def test_extract_integers_from_string(self):
        result = self.fc._get_ints_from_str_dataframe_column(self.fc.files, COLUMN_NAMES[3])
        pd.testing.assert_series_equal(result, RAW_INTEGERS_SERIES)


class FolderCrawlerTestsFilterPath(unittest.TestCase):
    def test_filter_paths_with_matching_substring(self):
        FILTER_PATH = 'Users'
        result = FolderCrawler._filter_paths(TEST_DATAFRAME, FILTER_PATH, COLUMN_NAMES[0])
        EXPECTED_NUMBER_OF_FILTERED_PATHS = 3

        self.assertEqual(len(result), EXPECTED_NUMBER_OF_FILTERED_PATHS)

    def test_filter_paths_with_no_matching_substring(self):
        FILTER_PATH = 'nonexistent'
        result = FolderCrawler._filter_paths(TEST_DATAFRAME, FILTER_PATH, COLUMN_NAMES[0])
        EXPECTED_NUMBER_OF_FILTERED_PATHS = 0

        self.assertEqual(len(result), EXPECTED_NUMBER_OF_FILTERED_PATHS)


class FolderCrawlerTestsFilterSizes(unittest.TestCase):

    def test_filter_sizes_greater_than_equal(self):
        result = FolderCrawler._filter_sizes(TEST_DATAFRAME, ByteSize.KILOBYTE, ">=", RAW_INTEGERS_SERIES)
        EXPECTED_NUMBER_OF_FILTERED_INTEGERS = 3

        self.assertEqual(len(result), EXPECTED_NUMBER_OF_FILTERED_INTEGERS)

    def test_filter_sizes_less_than_equal(self):
        result = FolderCrawler._filter_sizes(TEST_DATAFRAME, ByteSize.KILOBYTE, "<=", RAW_INTEGERS_SERIES)
        EXPECTED_NUMBER_OF_FILTERED_INTEGERS = 1

        self.assertEqual(len(result), EXPECTED_NUMBER_OF_FILTERED_INTEGERS)


class FolderCrawlerTestsFilterLastChange(unittest.TestCase):
    def setUp(self):
        self.folder_crawler = FolderCrawler(path=CURRENT_DIRECTORY)

    def test_filter_last_change_greater_than_equal(self):
        FILTER_DATE = datetime.datetime(2022, 1, 1)
        result = FolderCrawler._filter_last_change(TEST_DATAFRAME, FILTER_DATE, ">=", COLUMN_NAMES[1])
        EXPECTED_NUMBER_OF_FILTERED_DATES = 3

        self.assertEqual(len(result), EXPECTED_NUMBER_OF_FILTERED_DATES)

    def test_filter_last_change_less_than_equal(self):
        FILTER_DATE = datetime.datetime(2022, 1, 1)
        result = FolderCrawler._filter_last_change(TEST_DATAFRAME, FILTER_DATE, "<=", COLUMN_NAMES[1])
        EXPECTED_NUMBER_OF_FILTERED_DATES = 1

        self.assertEqual(len(result), EXPECTED_NUMBER_OF_FILTERED_DATES)


class FolderCrawlerTestsLoadCrawledData(unittest.TestCase):
    def setUp(self):
        self.folder_crawler = FolderCrawler(path=CURRENT_DIRECTORY)

    def test_load_crawled_data_with_empty_container(self):
        # Prepare the test environment
        test_helper = TestHelper(SavedCrawls.ROOT, SavedCrawls.FILES)
        text_to_write = (",".join(COLUMN_NAMES) + "\n")
        test_helper.create_test_paths(text_to_write)
        empty_df = pd.DataFrame()
        expected_path = os.path.join(SavedCrawls.ROOT, f"{ItemType.FILES}{SavedCrawls.EXTENSION}")
        expected_df = pd.read_csv(expected_path)

        # Run test
        result = FolderCrawler.load_crawled_data(empty_df, ItemType.FILES, SavedCrawls.ROOT, SavedCrawls.EXTENSION)

        # Clean up the test environment
        test_helper.delete_test_paths()

        # Evaluate
        pd.testing.assert_frame_equal(result, expected_df)

    def test_load_crawled_data_with_non_empty_container(self):
        result = FolderCrawler.load_crawled_data(TEST_DATAFRAME, ItemType.FILES, SavedCrawls.ROOT, SavedCrawls.EXTENSION)
        pd.testing.assert_frame_equal(result, TEST_DATAFRAME)


class FolderCrawlerTestsGetCrawlSummary(unittest.TestCase):
    def test_summary_with_total_size(self):
        result = FolderCrawler._get_crawl_summary(True, Messages.NR_OF_CRAWLED_DATA, ColoredBytes.ONE_KB_PRETTY,
                                                  ColoredBytes.ONE_KB_RAW)
        expected_summary = (Messages.NR_OF_CRAWLED_DATA, ColoredBytes.ONE_KB_PRETTY, ColoredBytes.ONE_KB_RAW, "\n\n")
        self.assertEqual(result, expected_summary)

    def test_summary_without_total_size(self):
        result = FolderCrawler._get_crawl_summary(False, Messages.NR_OF_CRAWLED_DATA, ColoredBytes.ONE_KB_PRETTY,
                                                  ColoredBytes.ONE_KB_RAW)
        expected_summary = ("\n",)
        self.assertEqual(result, expected_summary)


class FolderCrawlerTestsFilterData(unittest.TestCase):
    def setUp(self):
        self.folder_crawler = FolderCrawler(path=CURRENT_DIRECTORY)

    def test_filter_data_with_nan_values(self):
        files = pd.DataFrame({COLUMN_NAMES[0]: ['file1', 'file2'], 'Changed': ['change1', NONE]})
        folders = pd.DataFrame({COLUMN_NAMES[0]: ['folder1', 'folder2'], 'Changed': ['change1', NONE]})
        empty_dataframe = {COLUMN_NAMES[0]: [], 'Changed': []}
        column = 'Changed'
        files, folders, skipped = FolderCrawler._filter_data(files, folders, empty_dataframe, column)
        self.assertEqual(len(files), 1)
        self.assertEqual(len(folders), 1)
        self.assertEqual(len(skipped), 2)

    def test_filter_data_without_nan_values(self):
        files = pd.DataFrame({COLUMN_NAMES[0]: ['file1', 'file2'], 'Changed': ['change1', 'change2']})
        folders = pd.DataFrame({COLUMN_NAMES[0]: ['folder1', 'folder2'], 'Changed': ['change1', 'change2']})
        empty_dataframe = {COLUMN_NAMES[0]: [], 'Changed': []}
        column = 'Changed'
        files, folders, skipped = FolderCrawler._filter_data(files, folders, empty_dataframe, column)
        self.assertEqual(len(files), 2)
        self.assertEqual(len(folders), 2)
        self.assertEqual(len(skipped), 0)


class FolderCrawlerTestsGetCrawledData(unittest.TestCase):
    def setUp(self):
        self.folder_crawler = FolderCrawler(path=CURRENT_DIRECTORY)

    def test_get_crawled_data_with_folder(self):
        data = [(('path1', 'change1', 'size1', 'bytes1'), True),
                (('path2', 'change2', 'size2', 'bytes2'), False)]
        df = pd.DataFrame(data)
        result = FolderCrawler._get_crawled_data(df, True)
        expected = pd.DataFrame({COLUMN_NAMES[0]: ['path1'], 'Changed': ['change1'],
                                 'Size readable': ['size1'], 'Size bytes': ['bytes1']})

        pd.testing.assert_frame_equal(result.reset_index(), expected.reset_index())

    def test_get_crawled_data_with_file(self):
        data = [(('path2', 'change2', 'size2', 'bytes2'), False),
                (('path3', 'change3', 'size3', 'bytes3'), True)]
        df = pd.DataFrame(data)
        result = FolderCrawler._get_crawled_data(df, False)
        expected = pd.DataFrame({COLUMN_NAMES[0]: ['path2'], 'Changed': ['change2'],
                                 'Size readable': ['size2'], 'Size bytes': ['bytes2']})
        pd.testing.assert_frame_equal(result, expected)


class FolderCrawlerTestsCrawlShallow(unittest.TestCase):
    def setUp(self):
        self.folder_crawler = FolderCrawler(path=CURRENT_DIRECTORY)

    def test_shallow_crawl_with_no_subdirectories(self):
        os.mkdir(TEMP_DIR)
        result = FolderCrawler._crawl_shallow(TEMP_DIR)
        os.rmdir(TEMP_DIR)
        self.assertEqual(len(result), 0)

    def test_shallow_crawl_with_one_subdirectory(self):
        os.mkdir(TEMP_DIR)
        os.mkdir(os.path.join(TEMP_DIR, SUB_DIR_1))
        result = FolderCrawler._crawl_shallow(TEMP_DIR)
        os.rmdir(os.path.join(TEMP_DIR, SUB_DIR_1))
        os.rmdir(TEMP_DIR)
        self.assertEqual(len(result), 1)

    def test_shallow_crawl_with_multiple_subdirectories(self):
        os.mkdir(TEMP_DIR)
        os.mkdir(os.path.join(TEMP_DIR, SUB_DIR_1))
        os.mkdir(os.path.join(TEMP_DIR, SUB_DIR_2))
        result = FolderCrawler._crawl_shallow(TEMP_DIR)
        os.rmdir(os.path.join(TEMP_DIR, SUB_DIR_1))
        os.rmdir(os.path.join(TEMP_DIR, SUB_DIR_2))
        os.rmdir(TEMP_DIR)
        self.assertEqual(len(result), 2)

    def test_shallow_crawl_with_no_subdirectories_and_files(self):
        os.mkdir(TEMP_DIR)
        with open(os.path.join(TEMP_DIR, TEMP_FILE_1), FileOps.WRITE_MODE) as f:
            f.write(TEST_TEXT)
        result = FolderCrawler._crawl_shallow(TEMP_DIR)
        os.remove(os.path.join(TEMP_DIR, TEMP_FILE_1))
        os.rmdir(TEMP_DIR)
        self.assertEqual(len(result), 1)

    def test_shallow_crawl_with_one_subdirectory_and_files(self):
        os.mkdir(TEMP_DIR)
        os.mkdir(os.path.join(TEMP_DIR, SUB_DIR_1))
        with open(os.path.join(TEMP_DIR, SUB_DIR_1, TEMP_FILE_1), FileOps.WRITE_MODE) as f:
            f.write(TEST_TEXT)
        result = FolderCrawler._crawl_shallow(TEMP_DIR)
        os.remove(os.path.join(TEMP_DIR, SUB_DIR_1, TEMP_FILE_1))
        os.rmdir(os.path.join(TEMP_DIR, SUB_DIR_1))
        os.rmdir(TEMP_DIR)
        self.assertEqual(len(result), 1)

    def test_shallow_crawl_with_multiple_subdirectories_and_files(self):
        os.mkdir(TEMP_DIR)
        os.mkdir(os.path.join(TEMP_DIR, SUB_DIR_1))
        os.mkdir(os.path.join(TEMP_DIR, SUB_DIR_2))

        with open(os.path.join(TEMP_DIR, SUB_DIR_1, TEMP_FILE_1), FileOps.WRITE_MODE) as f:
            f.write(TEST_TEXT)
        with open(os.path.join(TEMP_DIR, SUB_DIR_2, TEMP_FILE_2), FileOps.WRITE_MODE) as f:
            f.write(TEST_TEXT)
        result = FolderCrawler._crawl_shallow(TEMP_DIR)
        os.remove(os.path.join(TEMP_DIR, SUB_DIR_1, TEMP_FILE_1))
        os.remove(os.path.join(TEMP_DIR, SUB_DIR_2, TEMP_FILE_2))
        os.rmdir(os.path.join(TEMP_DIR, SUB_DIR_1))
        os.rmdir(os.path.join(TEMP_DIR, SUB_DIR_2))
        os.rmdir(TEMP_DIR)
        self.assertEqual(len(result), 2)

    def test_shallow_crawl_with_multiple_subdirectories_and_files_2(self):
        os.mkdir(TEMP_DIR)
        os.mkdir(os.path.join(TEMP_DIR, SUB_DIR_1))
        os.mkdir(os.path.join(TEMP_DIR, SUB_DIR_2))
        with open(os.path.join(TEMP_DIR, TEMP_FILE_1), FileOps.WRITE_MODE) as f:
            f.write(TEST_TEXT)
        with open(os.path.join(TEMP_DIR, TEMP_FILE_2), FileOps.WRITE_MODE) as f:
            f.write(TEST_TEXT)
        result = FolderCrawler._crawl_shallow(TEMP_DIR)
        os.remove(os.path.join(TEMP_DIR, TEMP_FILE_1))
        os.remove(os.path.join(TEMP_DIR, TEMP_FILE_2))
        os.rmdir(os.path.join(TEMP_DIR, SUB_DIR_1))
        os.rmdir(os.path.join(TEMP_DIR, SUB_DIR_2))
        os.rmdir(TEMP_DIR)
        self.assertEqual(len(result), 4)


class FolderCrawlerTestsCrawlDeep(unittest.TestCase):
    def setUp(self):
        self.folder_crawler = FolderCrawler(path=CURRENT_DIRECTORY)

    def test_crawl_deep_with_no_subdirectories(self):
        os.mkdir(TEMP_DIR)
        result = FolderCrawler._crawl_deep(TEMP_DIR)
        os.rmdir(TEMP_DIR)
        self.assertEqual(len(result), 0)

    def test_crawl_deep_with_one_subdirectory(self):
        os.mkdir(TEMP_DIR)
        os.mkdir(os.path.join(TEMP_DIR, SUB_DIR_1))
        result = FolderCrawler._crawl_deep(TEMP_DIR)
        os.rmdir(os.path.join(TEMP_DIR, SUB_DIR_1))
        os.rmdir(TEMP_DIR)
        self.assertEqual(len(result), 1)

    def test_crawl_deep_with_multiple_subdirectories(self):
        os.mkdir(TEMP_DIR)
        os.mkdir(os.path.join(TEMP_DIR, SUB_DIR_1))
        os.mkdir(os.path.join(TEMP_DIR, SUB_DIR_2))
        result = FolderCrawler._crawl_deep(TEMP_DIR)
        os.rmdir(os.path.join(TEMP_DIR, SUB_DIR_1))
        os.rmdir(os.path.join(TEMP_DIR, SUB_DIR_2))
        os.rmdir(TEMP_DIR)
        self.assertEqual(len(result), 2)

    def test_crawl_deep_with_no_subdirectories_and_files(self):
        os.mkdir(TEMP_DIR)
        with open(os.path.join(TEMP_DIR, TEMP_FILE_1), FileOps.WRITE_MODE) as f:
            f.write(TEST_TEXT)
        result = FolderCrawler._crawl_deep(TEMP_DIR)
        os.remove(os.path.join(TEMP_DIR, TEMP_FILE_1))
        os.rmdir(TEMP_DIR)
        self.assertEqual(len(result), 1)

    def test_crawl_deep_with_one_subdirectory_and_files(self):
        os.mkdir(TEMP_DIR)
        os.mkdir(os.path.join(TEMP_DIR, SUB_DIR_1))
        with open(os.path.join(TEMP_DIR, SUB_DIR_1, TEMP_FILE_1), FileOps.WRITE_MODE) as f:
            f.write(TEST_TEXT)
        result = FolderCrawler._crawl_deep(TEMP_DIR)
        os.remove(os.path.join(TEMP_DIR, SUB_DIR_1, TEMP_FILE_1))
        os.rmdir(os.path.join(TEMP_DIR, SUB_DIR_1))
        os.rmdir(TEMP_DIR)
        self.assertEqual(len(result), 2)

    def test_crawl_deep_with_multiple_subdirectories_and_files(self):
        os.mkdir(TEMP_DIR)
        os.mkdir(os.path.join(TEMP_DIR, SUB_DIR_1))
        os.mkdir(os.path.join(TEMP_DIR, SUB_DIR_2))
        with open(os.path.join(TEMP_DIR, SUB_DIR_1, TEMP_FILE_1), FileOps.WRITE_MODE) as f:
            f.write(TEST_TEXT)
        with open(os.path.join(TEMP_DIR, SUB_DIR_2, TEMP_FILE_2), FileOps.WRITE_MODE) as f:
            f.write(TEST_TEXT)
        result = FolderCrawler._crawl_deep(TEMP_DIR)
        os.remove(os.path.join(TEMP_DIR, SUB_DIR_1, TEMP_FILE_1))
        os.remove(os.path.join(TEMP_DIR, SUB_DIR_2, TEMP_FILE_2))
        os.rmdir(os.path.join(TEMP_DIR, SUB_DIR_1))
        os.rmdir(os.path.join(TEMP_DIR, SUB_DIR_2))
        os.rmdir(TEMP_DIR)
        self.assertEqual(len(result), 4)


class FolderCrawlerTestsSaveCrawlResults(unittest.TestCase):
    def setUp(self):
        self.folder_crawler = FolderCrawler(path=CURRENT_DIRECTORY)

    def test_save_crawl_result_with_existing_path(self):
        path = 'temp_file.csv'
        data = {COLUMN_NAMES[0]: ['C:/Users', 'C:/Downloads', 'C:/Documents']}
        df = pd.DataFrame(data)
        df.to_csv(path, index=False)
        FolderCrawler._save_result(path, df)
        self.assertTrue(os.path.exists(path))
        os.remove(path)

    def test_save_crawl_result_with_non_existing_path(self):
        path = 'temp_file.csv'
        data = {COLUMN_NAMES[0]: ['C:/Users', 'C:/Downloads', 'C:/Documents']}
        df = pd.DataFrame(data)
        FolderCrawler._save_result(path, df)
        self.assertTrue(os.path.exists(path))
        os.remove(path)


class FolderCrawlerTestsGetSizeOfItem(unittest.TestCase):
    def setUp(self):
        self.folder_crawler = FolderCrawler(path=CURRENT_DIRECTORY)

    def test_size_of_existing_file(self):
        with open(TEMP_FILE_1, FileOps.WRITE_MODE) as f:
            f.write(TEST_TEXT)
        result = FolderCrawler._get_size_of_item(TEMP_FILE_1, False)
        os.remove(TEMP_FILE_1)
        self.assertEqual(result, 37)

    def test_size_of_non_existing_file(self):
        result = FolderCrawler._get_size_of_item('non_existing_file.txt', False)
        self.assertIs(result, NONE)

    def test_size_of_existing_directory(self):
        os.mkdir(TEMP_DIR)
        with open(os.path.join(TEMP_DIR, TEMP_FILE_1), FileOps.WRITE_MODE) as f:
            f.write(TEST_TEXT)
        result = FolderCrawler._get_size_of_item(TEMP_DIR, True)
        os.remove(os.path.join(TEMP_DIR, TEMP_FILE_1))
        os.rmdir(TEMP_DIR)
        self.assertEqual(result, 37)


class FolderCrawlerTestsTabulateData(unittest.TestCase):
    def setUp(self):
        self.folder_crawler = FolderCrawler(path=CURRENT_DIRECTORY)

    def test_tabulate_data_with_empty_dataframe(self):
        df = pd.DataFrame()
        result = FolderCrawler._tabulate_data(df)
        expected = tabulate(df, headers=TABLE_HEADER, tablefmt=TABLE_FORMAT)
        self.assertEqual(result, expected)

    def test_tabulate_data_with_non_empty_dataframe(self):
        data = {COLUMN_NAMES[0]: ['C:/Users', 'C:/Downloads', 'C:/Documents']}
        df = pd.DataFrame(data)
        result = FolderCrawler._tabulate_data(df)
        expected = tabulate(df, headers=TABLE_HEADER, tablefmt=TABLE_FORMAT)
        self.assertEqual(result, expected)


class FolderCrawlerTestsGetLastChangeOfItem(unittest.TestCase):
    def setUp(self):
        self.folder_crawler = FolderCrawler(path=CURRENT_DIRECTORY)

    def test_last_change_of_existing_file(self):
        with open(TEMP_FILE_1, FileOps.WRITE_MODE) as f:
            f.write(TEST_TEXT)
        result = FolderCrawler._get_last_change_of_item(TEMP_FILE_1)
        os.remove(TEMP_FILE_1)
        self.assertIsInstance(result, datetime.datetime)

    def test_last_change_of_non_existing_file(self):
        result = FolderCrawler._get_last_change_of_item('non_existing_file.txt')
        self.assertIs(result, NONE)


class FolderCrawlerTestsFilterSubdirectories(unittest.TestCase):
    def setUp(self):
        self.folder_crawler = FolderCrawler(path=CURRENT_DIRECTORY)

    def test_filter_subdirectories_with_no_subdirectories(self):
        data = {COLUMN_NAMES[0]: ['C:/Users', 'C:/Downloads', 'C:/Documents']}
        df = pd.DataFrame(data)
        result = FolderCrawler._filter_subdirectories(df, COLUMN_NAMES[0])
        self.assertEqual(len(result), 3)

    def test_filter_subdirectories_with_subdirectories(self):
        data = {COLUMN_NAMES[0]: ['C:/Users', 'C:/Users/Subfolder', 'C:/Users/Subfolder/Subfolder2']}
        df = pd.DataFrame(data)
        result = FolderCrawler._filter_subdirectories(df, 'Path')
        self.assertEqual(len(result), 1)

    def test_filter_subdirectories_with_mixed_paths(self):
        data = {COLUMN_NAMES[0]: ['C:/Users', 'C:/Users/Subfolder', 'C:/Downloads']}
        df = pd.DataFrame(data)
        result = FolderCrawler._filter_subdirectories(df, COLUMN_NAMES[0])
        self.assertEqual(len(result), 2)


class FolderCrawlerTestsGetTimePerformance(unittest.TestCase):
    def setUp(self):
        self.fc = FolderCrawler(path=CURRENT_DIRECTORY)

    def test_time_performance_returns_positive(self):
        TIME = 0.1
        start_time = time.perf_counter()
        time.sleep(TIME)
        time_result = self.fc.get_time_performance(start_time)
        time_expected = TIME

        self.assertGreater(time_result, time_expected)

    def test_time_performance_returns_zero_for_same_time(self):
        start_time = time.perf_counter()
        time_result = self.fc.get_time_performance(start_time).__floor__()
        time_expected = 0

        self.assertEqual(time_result, time_expected)


class FolderCrawlerTestsGetCurrentTime(unittest.TestCase):
    def setUp(self):
        self.fc = FolderCrawler(path=CURRENT_DIRECTORY)

    def test_current_time_returns_now(self):
        result = self.fc._get_current_time()
        now = datetime.datetime.now()
        self.assertEqual(result.year, now.year)
        self.assertEqual(result.month, now.month)
        self.assertEqual(result.day, now.day)
        self.assertEqual(result.hour, now.hour)
        self.assertEqual(result.minute, now.minute)
        self.assertAlmostEqual(result.second, now.second)


class FolderCrawlerTestsColorFormatString(unittest.TestCase):
    def setUp(self):
        self.fc = FolderCrawler(path=CURRENT_DIRECTORY)

    def test_color_format_string_short(self):
        size = 1
        args = Fore.YELLOW, size, ByteUnit.KILOBYTE, Style.RESET_ALL, False
        result = self.fc._color_format_string(*args)
        self.assertEqual(result, ColoredBytes.ONE_KB_PRETTY)

    def test_color_format_string_long(self):
        args = Fore.YELLOW, ByteSize.KILOBYTE, None, Style.RESET_ALL, True
        result = self.fc._color_format_string(*args)
        self.assertEqual(result, ColoredBytes.ONE_KB_RAW)


class FolderCrawlerTestsConvertBytesToReadableFormat(unittest.TestCase):

    def setUp(self):
        self.fc = FolderCrawler(path=CURRENT_DIRECTORY)

    def convert_bytes_test_helper(self, fore_color: str, size_long: int, size_short: int, byte_unit: str):
        """
        Helper method for testing the conversion of bytes to readable format.

        :param fore_color: Foreground color of text when it is printed into console.
        :param size_long: Size in bytes which we want to convert to readable format.
        :param size_short: Size which we expect after formatting. If size_long is 1024, put here 1 and into byte_unit "KB".
        :param byte_unit: Unit which we expect after the formating. For example "KB" for kilobytes if size_long is 1024.
        """
        result_short, result_long = self.fc._convert_bytes_to_readable_format(size_long,
                                                                              ColorFormatting.COLORS,
                                                                              ColorFormatting.UNITS,
                                                                              Style.RESET_ALL,
                                                                              self.fc._color_format_string)

        # Due to hassle with formatted strings I decided to use the method from the production code to get the expected
        # results. This is not probably the best way to test, but it saves my nerves. If you are unsure about the
        # methods output, double check how it behaves when it is tested with unit test.
        expected_short = self.fc._color_format_string(*(fore_color, size_short, byte_unit, Style.RESET_ALL, False))
        expected_long = self.fc._color_format_string(*(fore_color, size_long, None, Style.RESET_ALL, True))

        return result_short, result_long, expected_short, expected_long

    def test_bytes_to_readable_format_zero_bytes(self):
        byte_size_short = 0
        byte_size_long = 0
        args = Fore.RED, byte_size_long, byte_size_short, ByteUnit.BYTE
        result_short, result_long, expected_short, expected_long = self.convert_bytes_test_helper(*args)

        self.assertEqual(result_short, expected_short)
        self.assertEqual(result_long, expected_long)

    def test_bytes_to_readable_format_one_byte(self):
        byte_size_short = 1
        args = Fore.RED, ByteSize.BYTE, byte_size_short, ByteUnit.BYTE
        result_short, result_long, expected_short, expected_long = self.convert_bytes_test_helper(*args)

        self.assertEqual(result_short, expected_short)
        self.assertEqual(result_long, expected_long)

    def test_bytes_to_readable_format_one_kilobyte(self):
        byte_size_short = 1
        args = Fore.YELLOW, ByteSize.KILOBYTE, byte_size_short, ByteUnit.KILOBYTE
        result_short, result_long, expected_short, expected_long = self.convert_bytes_test_helper(*args)

        self.assertEqual(result_short, expected_short)
        self.assertEqual(result_long, expected_long)

    def test_bytes_to_readable_format_one_megabyte(self):
        byte_size_short = 1
        args = Fore.GREEN, ByteSize.MEGABYTE, byte_size_short, ByteUnit.MEGABYTE
        result_short, result_long, expected_short, expected_long = self.convert_bytes_test_helper(*args)

        self.assertEqual(result_short, expected_short)
        self.assertEqual(result_long, expected_long)

    def test_bytes_to_readable_format_one_gigabyte(self):
        byte_size_short = 1
        args = Fore.BLUE, ByteSize.GIGABYTE, byte_size_short, ByteUnit.GIGABYTE
        result_short, result_long, expected_short, expected_long = self.convert_bytes_test_helper(*args)

        self.assertEqual(result_short, expected_short)
        self.assertEqual(result_long, expected_long)

    def test_bytes_to_readable_format_one_terabyte(self):
        byte_size_short = 1
        args = Fore.CYAN, ByteSize.TERABYTE, byte_size_short, ByteUnit.TERABYTE
        result_short, result_long, expected_short, expected_long = self.convert_bytes_test_helper(*args)

        self.assertEqual(result_short, expected_short)
        self.assertEqual(result_long, expected_long)


class TestFolderCrawlerFormatTimeStamp(unittest.TestCase):
    def setUp(self):
        self.fc = FolderCrawler(path=CURRENT_DIRECTORY)

    def test_format_timestamp_for_zero_seconds(self):
        result = self.fc._format_timestamp(seconds=0)
        self.assertEqual(result, "")

    def test_format_timestamp_for_one_second(self):
        result = self.fc._format_timestamp(seconds=1)
        self.assertEqual(result, "1.0000 Second")

    def test_format_timestamp_for_multiple_seconds(self):
        result = self.fc._format_timestamp(seconds=5)
        self.assertEqual(result, "5.0000 Seconds")

    def test_format_timestamp_for_one_minute(self):
        result = self.fc._format_timestamp(seconds=60)
        self.assertEqual(result, "1 Minute ")

    def test_format_timestamp_for_multiple_minutes(self):
        result = self.fc._format_timestamp(seconds=120)
        self.assertEqual(result, "2 Minutes ")

    def test_format_timestamp_for_multiple_minutes_one_second(self):
        result = self.fc._format_timestamp(seconds=121)
        self.assertEqual(result, "2 Minutes 1.0000 Second")

    def test_format_timestamp_for_multiple_minutes_multiple_seconds(self):
        result = self.fc._format_timestamp(seconds=122)
        self.assertEqual(result, "2 Minutes 2.0000 Seconds")

    def test_format_timestamp_for_one_hour(self):
        result = self.fc._format_timestamp(seconds=3600)
        self.assertEqual(result, "1 Hour ")

    def test_format_timestamp_for_multiple_hours(self):
        result = self.fc._format_timestamp(seconds=7200)
        self.assertEqual(result, "2 Hours ")

    def test_format_timestamp_for_multiple_hours_one_minute(self):
        result = self.fc._format_timestamp(seconds=7260)
        self.assertEqual(result, "2 Hours 1 Minute ")

    def test_format_timestamp_for_multiple_hours_one_minute_one_second(self):
        result = self.fc._format_timestamp(seconds=7261)
        self.assertEqual(result, "2 Hours 1 Minute 1.0000 Second")

    def test_format_timestamp_for_one_day(self):
        result = self.fc._format_timestamp(seconds=86400)
        self.assertEqual(result, "1 Day ")

    def test_format_timestamp_for_multiple_days(self):
        result = self.fc._format_timestamp(seconds=172800)
        self.assertEqual(result, "2 Days ")

    def test_format_timestamp_for_one_year(self):
        result = self.fc._format_timestamp(seconds=31536000)
        self.assertEqual(result, "1 Year ")

    def test_format_timestamp_for_multiple_years(self):
        result = self.fc._format_timestamp(seconds=63072000)
        self.assertEqual(result, "2 Years ")


class TestFolderCrawlerInitializeStorage(unittest.TestCase):

    def setUp(self):
        self.fc = FolderCrawler(path=CURRENT_DIRECTORY)

    def test_initialize_files_creates_directories_and_files(self):
        # Run test
        self.fc.initialize_storage(SavedCrawls.ROOT,
                                   *(SavedCrawls.FILES, SavedCrawls.FOLDERS, SavedCrawls.SKIPPED))

        # Evaluate
        self.assertTrue(os.path.exists(SavedCrawls.ROOT))
        self.assertTrue(os.path.exists(SavedCrawls.FILES))
        self.assertTrue(os.path.exists(SavedCrawls.FOLDERS))
        self.assertTrue(os.path.exists(SavedCrawls.SKIPPED))

        # Clean up
        for file in (SavedCrawls.FILES, SavedCrawls.FOLDERS, SavedCrawls.SKIPPED):
            os.remove(file)
        os.rmdir(SavedCrawls.ROOT)


# endregion


if __name__ == '__main__':
    unittest.main()
