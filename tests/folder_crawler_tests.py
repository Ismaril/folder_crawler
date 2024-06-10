import os
import time
import unittest
import datetime
import pandas as pd

from tabulate import tabulate
from colorama import Style, Fore
from structures import SavedCrawls, Messages, ColorFormatting, ByteSize, ItemType, FileOps, ByteUnit, ColoredBytes
from folder_crawler import FolderCrawler, NONE, COLUMN_NAMES, TABLE_HEADER, TABLE_FORMAT
from unittest.mock import patch, mock_open
from test_helper import TestHelper

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
    COLUMN_NAMES[2]: [ColoredBytes.ONE_KB_SHORT, ColoredBytes.TWO_KB_SHORT, ColoredBytes.THREE_KB_SHORT],
    COLUMN_NAMES[3]: [ColoredBytes.ONE_KB_LONG, ColoredBytes.TWO_KB_LONG, ColoredBytes.THREE_KB_LONG]
}
TEST_DATAFRAME = pd.DataFrame(TEST_DICT)


# region Integration tests
class FolderCrawlerTestsMain(unittest.TestCase):
    def setUp(self):
        self.folder_crawler = FolderCrawler(path=CURRENT_DIRECTORY)

    def test_main_1(self):
        # Prepare the test environment
        test_helper = TestHelper(TEMP_DIR, os.path.join(TEMP_DIR, TEMP_FILE_1))
        test_helper.create_test_paths(TEST_TEXT)
        self.folder_crawler.path = TEMP_DIR

        # Run test
        self.folder_crawler.main()

        # Clean up the test environment
        test_helper.delete_test_paths()
        test_helper.delete_saved_crawls()

        # Evaluate
        self.assertTrue(self.folder_crawler.files[COLUMN_NAMES[0]][0] == os.path.join(TEMP_DIR, TEMP_FILE_1))
        self.assertTrue(self.folder_crawler.files[COLUMN_NAMES[3]][0] == ColoredBytes.THIRTYSEVEN_BYTES_LONG)

    def test_main_2(self):
        # Prepare the test environment
        test_helper = TestHelper(TEMP_DIR,
                                 os.path.join(TEMP_DIR, SUB_DIR_1),
                                 os.path.join(TEMP_DIR, SUB_DIR_1, TEMP_FILE_1))
        test_helper.create_test_paths(TEST_TEXT)
        self.folder_crawler.path = TEMP_DIR

        # Run test
        self.folder_crawler.main()

        # Clean up the test environment
        test_helper.delete_test_paths()
        test_helper.delete_saved_crawls()

        # Evaluate
        self.assertTrue(self.folder_crawler.files[COLUMN_NAMES[0]][0] == os.path.join(TEMP_DIR, SUB_DIR_1, TEMP_FILE_1))
        self.assertTrue(self.folder_crawler.files[COLUMN_NAMES[3]][0] == ColoredBytes.THIRTYSEVEN_BYTES_LONG)
        self.assertTrue(self.folder_crawler.folders[COLUMN_NAMES[0]][0] == os.path.join(TEMP_DIR, SUB_DIR_1))
        self.assertTrue(self.folder_crawler.folders[COLUMN_NAMES[3]][0] == ColoredBytes.THIRTYSEVEN_BYTES_LONG)
        self.assertTrue(self.folder_crawler.skipped.empty)


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
        is_directory = True

        # Run test
        result_tuple = self.fc._get_path_with_properties((TEMP_DIR, is_directory))
        nr_of_properties = len(result_tuple[0])
        is_directory_ = result_tuple[1]

        # Clean up the test environment
        test_helper.delete_test_paths()

        # Evaluate
        result = (nr_of_properties, is_directory_)
        expected = (len(COLUMN_NAMES), is_directory)
        self.assertEqual(result, expected)

    def test_get_path_with_properties_with_no_subdirectories2(self):
        # Prepare the test environment
        test_helper = TestHelper(TEMP_FILE_1)
        test_helper.create_test_paths(TEST_TEXT)
        is_directory = False

        # Run test
        result_tuple = self.fc._get_path_with_properties((TEMP_FILE_1, is_directory))
        nr_of_properties = len(result_tuple[0])
        is_directory_ = result_tuple[1]

        # Clean up the test environment
        test_helper.delete_test_paths()

        # Evaluate
        result = (nr_of_properties, is_directory_)
        expected = (len(COLUMN_NAMES), is_directory)
        self.assertEqual(result, expected)


class FolderCrawlerTestsResolveSizes(unittest.TestCase):
    def setUp(self):
        self.fc = FolderCrawler(path=CURRENT_DIRECTORY)

    def test_resolve_sizes_with_valid_size(self):
        result = self.fc._resolve_sizes(size=ByteSize.KILOBYTE)
        self.assertEqual(result, (ColoredBytes.ONE_KB_SHORT, ColoredBytes.ONE_KB_LONG))

    def test_resolve_sizes_with_none(self):
        result = self.fc._resolve_sizes(size=NONE)
        self.assertEqual(result, (NONE, NONE))


# endregion

# region Unit tests

class FolderCrawlerTestsGetIntsFromStrDataFrameColumn(unittest.TestCase):
    def setUp(self):
        self.folder_crawler = FolderCrawler(path=CURRENT_DIRECTORY)
        self.folder_crawler.files = TEST_DATAFRAME

    def test_extract_integers_from_string(self):
        result = self.folder_crawler._get_ints_from_str_dataframe_column(self.folder_crawler.files, COLUMN_NAMES[3])
        expected_integers = pd.Series([1024, 2048, 3072], dtype='int64', name=COLUMN_NAMES[3])
        pd.testing.assert_series_equal(result, expected_integers)


class FolderCrawlerTestsFilterPath(unittest.TestCase):
    def test_filter_paths_with_matching_substring(self):
        filter_path = 'Users'
        result = FolderCrawler._filter_paths(TEST_DATAFRAME, filter_path, COLUMN_NAMES[0])
        expected_number_of_filtered_paths = 3
        self.assertEqual(len(result), expected_number_of_filtered_paths)

    def test_filter_paths_with_no_matching_substring(self):
        filter_path = 'nonexistent'
        result = FolderCrawler._filter_paths(TEST_DATAFRAME, filter_path, COLUMN_NAMES[0])
        expected_number_of_filtered_paths = 0
        self.assertEqual(len(result), expected_number_of_filtered_paths)


class FolderCrawlerTestsFilterSizes(unittest.TestCase):

    def test_filter_sizes_greater_than_equal(self):
        result = FolderCrawler._filter_sizes(TEST_DATAFRAME, 1024, ">=", pd.Series([1024, 2048, 3072]))
        expected_number_of_filtered_integers = 3
        self.assertEqual(len(result), expected_number_of_filtered_integers)

    def test_filter_sizes_less_than_equal(self):
        result = FolderCrawler._filter_sizes(TEST_DATAFRAME, 1024, "<=", pd.Series([1024, 2048, 3072]))
        expected_number_of_filtered_integers = 1
        self.assertEqual(len(result), expected_number_of_filtered_integers)


class FolderCrawlerTestsFilterLastChange(unittest.TestCase):
    def setUp(self):
        self.folder_crawler = FolderCrawler(path=CURRENT_DIRECTORY)

    def test_filter_last_change_greater_than_equal(self):
        filter_date = datetime.datetime(2022, 1, 1)
        result = FolderCrawler._filter_last_change(TEST_DATAFRAME, filter_date, ">=", COLUMN_NAMES[1])
        expected_number_of_filtered_dates = 3
        self.assertEqual(len(result), expected_number_of_filtered_dates)

    def test_filter_last_change_less_than_equal(self):
        filter_date = datetime.datetime(2022, 1, 1)
        result = FolderCrawler._filter_last_change(TEST_DATAFRAME, filter_date, "<=", COLUMN_NAMES[1])
        expected_number_of_filtered_dates = 1
        self.assertEqual(len(result), expected_number_of_filtered_dates)


class FolderCrawlerTestsLoadCrawledData(unittest.TestCase):
    def setUp(self):
        self.folder_crawler = FolderCrawler(path=CURRENT_DIRECTORY)

    def test_load_crawled_data_with_empty_container(self):
        # Prepare the test environment
        test_helper = TestHelper(SavedCrawls.ROOT, SavedCrawls.FILES)
        test_helper.create_test_paths(",".join(COLUMN_NAMES) + "\n")
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
        item_type = "files"
        result = FolderCrawler.load_crawled_data(TEST_DATAFRAME, item_type, SavedCrawls.ROOT, SavedCrawls.EXTENSION)
        pd.testing.assert_frame_equal(result, TEST_DATAFRAME)


class FolderCrawlerTestsGetCrawlSummary(unittest.TestCase):
    def test_summary_with_total_size(self):
        result = FolderCrawler._get_crawl_summary(True, Messages.NR_OF_CRAWLED_DATA, ColoredBytes.ONE_KB_SHORT, ColoredBytes.ONE_KB_LONG)
        expected_summary = (Messages.NR_OF_CRAWLED_DATA, ColoredBytes.ONE_KB_SHORT, ColoredBytes.ONE_KB_LONG, "\n\n")
        self.assertEqual(result, expected_summary)

    def test_summary_without_total_size(self):
        result = FolderCrawler._get_crawl_summary(False, Messages.NR_OF_CRAWLED_DATA, ColoredBytes.ONE_KB_SHORT, ColoredBytes.ONE_KB_LONG)
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
        self.assertAlmostEqual(result.year, now.year)
        self.assertAlmostEqual(result.month, now.month)
        self.assertAlmostEqual(result.day, now.day)
        self.assertAlmostEqual(result.hour, now.hour)
        self.assertAlmostEqual(result.minute, now.minute)


class FolderCrawlerTestsColorFormatString(unittest.TestCase):
    def setUp(self):
        self.fc = FolderCrawler(path=CURRENT_DIRECTORY)

    def test_color_format_string_short(self):
        size = 1
        result = self.fc._color_format_string(Fore.YELLOW, size, ByteUnit.KILOBYTE, Style.RESET_ALL, False)
        self.assertEqual(result, ColoredBytes.ONE_KB_SHORT)

    def test_color_format_string_long(self):
        result = self.fc._color_format_string(Fore.YELLOW, ByteSize.KILOBYTE, None, Style.RESET_ALL, True)
        self.assertEqual(result, ColoredBytes.ONE_KB_LONG)


class FolderCrawlerTestsConvertBytesToReadableFormat(unittest.TestCase):

    def setUp(self):
        self.fc = FolderCrawler(path=CURRENT_DIRECTORY)

    def convert_bytes_test_helper(self, fore_color: str, size_long: int, size_short: int, byte_unit: str):
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
        result_short, result_long, expected_short, expected_long \
            = self.convert_bytes_test_helper(Fore.RED, byte_size_long, byte_size_short, ByteUnit.BYTE)
        self.assertEqual(result_short, expected_short)
        self.assertEqual(result_long, expected_long)

    def test_bytes_to_readable_format_one_byte(self):
        byte_size_short = 1
        result_short, result_long, expected_short, expected_long \
            = self.convert_bytes_test_helper(Fore.RED, ByteSize.BYTE, byte_size_short, ByteUnit.BYTE)
        self.assertEqual(result_short, expected_short)
        self.assertEqual(result_long, expected_long)

    def test_bytes_to_readable_format_one_kilobyte(self):
        byte_size_short = 1
        result_short, result_long, expected_short, expected_long \
            = self.convert_bytes_test_helper(Fore.YELLOW, ByteSize.KILOBYTE, byte_size_short, ByteUnit.KILOBYTE)
        self.assertEqual(result_short, expected_short)
        self.assertEqual(result_long, expected_long)

    def test_bytes_to_readable_format_one_megabyte(self):
        byte_size_short = 1
        result_short, result_long, expected_short, expected_long \
            = self.convert_bytes_test_helper(Fore.GREEN, ByteSize.MEGABYTE, byte_size_short, ByteUnit.MEGABYTE)
        self.assertEqual(result_short, expected_short)
        self.assertEqual(result_long, expected_long)

    def test_bytes_to_readable_format_one_gigabyte(self):
        byte_size_short = 1
        result_short, result_long, expected_short, expected_long \
            = self.convert_bytes_test_helper(Fore.BLUE, ByteSize.GIGABYTE, byte_size_short, ByteUnit.GIGABYTE)
        self.assertEqual(result_short, expected_short)
        self.assertEqual(result_long, expected_long)

    def test_bytes_to_readable_format_one_terabyte(self):
        byte_size_short = 1
        result_short, result_long, expected_short, expected_long \
            = self.convert_bytes_test_helper(Fore.CYAN, ByteSize.TERABYTE, byte_size_short, ByteUnit.TERABYTE)
        self.assertEqual(result_short, expected_short)
        self.assertEqual(result_long, expected_long)


class TestFolderCrawlerFormatTimespan(unittest.TestCase):
    def setUp(self):
        self.folder_crawler = FolderCrawler(path=CURRENT_DIRECTORY)

    def test_format_timestamp_for_zero_seconds(self):
        result = self.folder_crawler._format_timestamp(0)
        self.assertEqual(result, "")

    def test_format_timestamp_for_one_second(self):
        result = self.folder_crawler._format_timestamp(1)
        self.assertEqual(result, "1.0000 Second")

    def test_format_timestamp_for_multiple_seconds(self):
        result = self.folder_crawler._format_timestamp(5)
        self.assertEqual(result, "5.0000 Seconds")

    def test_format_timestamp_for_one_minute(self):
        result = self.folder_crawler._format_timestamp(60)
        self.assertEqual(result, "1 Minute ")

    def test_format_timestamp_for_multiple_minutes(self):
        result = self.folder_crawler._format_timestamp(120)
        self.assertEqual(result, "2 Minutes ")

    def test_format_timestamp_for_multiple_minutes_one_second(self):
        result = self.folder_crawler._format_timestamp(121)
        self.assertEqual(result, "2 Minutes 1.0000 Second")

    def test_format_timestamp_for_multiple_minutes_multiple_seconds(self):
        result = self.folder_crawler._format_timestamp(122)
        self.assertEqual(result, "2 Minutes 2.0000 Seconds")

    def test_format_timestamp_for_one_hour(self):
        result = self.folder_crawler._format_timestamp(3600)
        self.assertEqual(result, "1 Hour ")

    def test_format_timestamp_for_multiple_hours(self):
        result = self.folder_crawler._format_timestamp(7200)
        self.assertEqual(result, "2 Hours ")

    def test_format_timestamp_for_multiple_hours_one_minute(self):
        result = self.folder_crawler._format_timestamp(7260)
        self.assertEqual(result, "2 Hours 1 Minute ")

    def test_format_timestamp_for_multiple_hours_one_minute_one_second(self):
        result = self.folder_crawler._format_timestamp(7261)
        self.assertEqual(result, "2 Hours 1 Minute 1.0000 Second")

    def test_format_timestamp_for_one_day(self):
        result = self.folder_crawler._format_timestamp(86400)
        self.assertEqual(result, "1 Day ")

    def test_format_timestamp_for_multiple_days(self):
        result = self.folder_crawler._format_timestamp(172800)
        self.assertEqual(result, "2 Days ")

    def test_format_timestamp_for_one_year(self):
        result = self.folder_crawler._format_timestamp(31536000)
        self.assertEqual(result, "1 Year ")

    def test_format_timestamp_for_multiple_years(self):
        result = self.folder_crawler._format_timestamp(63072000)
        self.assertEqual(result, "2 Years ")


class TestFolderCrawlerInitializeFiles(unittest.TestCase):
    @patch('os.makedirs')
    @patch('builtins.open', new_callable=mock_open)
    def test_initialize_files_creates_directories_and_files(self, mock_open, mock_makedirs):
        # Mock the constants used in the method
        mock_saved_crawls_folder = "mock_folder"
        mock_files = "mock_files"
        mock_folders = "mock_folders"
        mock_skipped = "mock_skipped"
        mock_append_mode = "mock_append_mode"
        mock_encoding = "mock_encoding"

        with patch('folder_crawler.SavedCrawls.ROOT', new=mock_saved_crawls_folder), \
                patch('folder_crawler.SavedCrawls.FILES', new=mock_files), \
                patch('folder_crawler.SavedCrawls.FOLDERS', new=mock_folders), \
                patch('folder_crawler.SavedCrawls.SKIPPED', new=mock_skipped), \
                patch('folder_crawler.FileOps.APPEND_MODE', new=mock_append_mode), \
                patch('folder_crawler.FileOps.ENCODING', new=mock_encoding):
            # Call the method under test
            FolderCrawler.initialize_storage(SavedCrawls.ROOT,
                                             *(SavedCrawls.FILES, SavedCrawls.FOLDERS, SavedCrawls.SKIPPED))

        # Assert that the directory was created
        mock_makedirs.assert_called_once_with(mock_saved_crawls_folder, exist_ok=True)

        # Assert that the files were created
        mock_open.assert_any_call(mock_files, mock_append_mode, encoding=mock_encoding)
        mock_open.assert_any_call(mock_folders, mock_append_mode, encoding=mock_encoding)
        mock_open.assert_any_call(mock_skipped, mock_append_mode, encoding=mock_encoding)
        self.assertEqual(mock_open.call_count, 3)


# endregion


if __name__ == '__main__':
    unittest.main()
