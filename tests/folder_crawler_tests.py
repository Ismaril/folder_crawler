import os
import time
import unittest
import datetime
import pandas as pd

from tabulate import tabulate
from colorama import Style, Fore
from test_helper import TestHelper
from folder_crawler import FolderCrawler, NONE, COLUMN_NAMES, TABLE_HEADER, TABLE_FORMAT
from structures import SavedCrawls, Messages, ColorFormatting, ByteSize, ItemType, ByteUnit, ColoredBytes

# region constants
TEMP_DIR = "temp_dir"
SUB_DIR_1 = "sub_dir1"
SUB_DIR_2 = "sub_dir2"
TEMP_FILE_1 = "temp_file1.txt"
TEMP_FILE_2 = "temp_file2.txt"
TEST_TEXT = "This is a temporary file for testing."
TEST_TEXT_2 = "This is a temporary file for testing.2"
NOT_EXISTING_FILE = 'non_existing_file.txt'
NOT_EXISTING_FILE_CONTENT = 'non_existing_file_content'
NOT_EXISTING_FOLDER = 'non_existing_folder'
CURRENT_DIRECTORY = "."

TEST_DATAFRAME = pd.DataFrame({
    COLUMN_NAMES[0]: ['C:/Users', 'C:/Users/Subfolder', 'C:/Users/Subfolder/Subfolder2'],
    COLUMN_NAMES[1]: [datetime.datetime(2022, 1, 1), datetime.datetime(2022, 2, 1), datetime.datetime(2022, 3, 1)],
    COLUMN_NAMES[2]: [ColoredBytes.ONE_KB_READABLE, ColoredBytes.TWO_KB_READABLE, ColoredBytes.THREE_KB_READABLE],
    COLUMN_NAMES[3]: [ColoredBytes.ONE_KB_RAW, ColoredBytes.TWO_KB_RAW, ColoredBytes.THREE_KB_RAW]
})
RAW_INTEGERS_SERIES = pd.Series([1024, 2048, 3072], dtype='int64', name=COLUMN_NAMES[3])
EMPTY_DATAFRAME = pd.DataFrame()


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

    def test_main_non_existing_path(self):
        self.fc.path = NOT_EXISTING_FOLDER

        # Run test and evaluate at the same time.
        with self.assertRaises(FileNotFoundError):
            self.fc.main()


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
        resulting_tuple = (nr_of_properties, is_directory_)
        expected_tuple = (len(COLUMN_NAMES), is_directory)

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
        expected_number_of_filtered_paths = 3
        self.assertEqual(len(result), expected_number_of_filtered_paths)

    def test_filter_paths_with_no_matching_substring(self):
        FILTER_PATH = 'nonexistent'
        result = FolderCrawler._filter_paths(TEST_DATAFRAME, FILTER_PATH, COLUMN_NAMES[0])
        expected_number_of_filtered_paths = 0
        self.assertEqual(len(result), expected_number_of_filtered_paths)


class FolderCrawlerTestsFilterSizes(unittest.TestCase):
    def setUp(self):
        self.fc = FolderCrawler(path=CURRENT_DIRECTORY)

    def test_filter_sizes_greater_than_equal(self):
        result = self.fc._filter_sizes(TEST_DATAFRAME, ByteSize.KILOBYTE, ">=", RAW_INTEGERS_SERIES)
        expected_number_of_filtered_integers = 3

        self.assertEqual(len(result), expected_number_of_filtered_integers)

    def test_filter_sizes_less_than_equal(self):
        result = self.fc._filter_sizes(TEST_DATAFRAME, ByteSize.KILOBYTE, "<=", RAW_INTEGERS_SERIES)
        expected_number_of_filtered_integers = 1

        self.assertEqual(len(result), expected_number_of_filtered_integers)


class FolderCrawlerTestsFilterLastChange(unittest.TestCase):
    def setUp(self):
        self.folder_crawler = FolderCrawler(path=CURRENT_DIRECTORY)
        self.FILTER_DATE = datetime.datetime(2022, 1, 1)

    def test_filter_last_change_greater_than_equal(self):
        result = FolderCrawler._filter_last_change(TEST_DATAFRAME, self.FILTER_DATE, ">=", COLUMN_NAMES[1])
        expected_number_of_filtered_dates = 3
        self.assertEqual(len(result), expected_number_of_filtered_dates)

    def test_filter_last_change_less_than_equal(self):
        result = FolderCrawler._filter_last_change(TEST_DATAFRAME, self.FILTER_DATE, "<=", COLUMN_NAMES[1])
        expected_number_of_filtered_dates = 1
        self.assertEqual(len(result), expected_number_of_filtered_dates)


class FolderCrawlerTestsLoadCrawledData(unittest.TestCase):
    def setUp(self):
        self.fc = FolderCrawler(path=CURRENT_DIRECTORY)

    def test_load_crawled_data_with_empty_container(self):
        # Prepare the test environment
        test_helper = TestHelper(SavedCrawls.ROOT, SavedCrawls.FILES)
        text_to_write = (",".join(COLUMN_NAMES) + "\n")
        test_helper.create_test_paths(text_to_write)
        empty_df = pd.DataFrame()
        expected_path = os.path.join(SavedCrawls.ROOT, f"{ItemType.FILES}{SavedCrawls.EXTENSION}")
        expected_df = pd.read_csv(expected_path)

        # Run test
        result = self.fc.load_crawled_data(empty_df, ItemType.FILES, SavedCrawls.ROOT, SavedCrawls.EXTENSION)

        # Clean up the test environment
        test_helper.delete_test_paths()

        # Evaluate
        pd.testing.assert_frame_equal(result, expected_df)

    def test_load_crawled_data_with_non_empty_container(self):
        result = self.fc.load_crawled_data(TEST_DATAFRAME, ItemType.FILES, SavedCrawls.ROOT, SavedCrawls.EXTENSION)
        pd.testing.assert_frame_equal(result, TEST_DATAFRAME)


class FolderCrawlerTestsGetCrawlSummary(unittest.TestCase):

    def setUp(self):
        self.fc = FolderCrawler(path=CURRENT_DIRECTORY)

    def test_summary_with_total_size(self):
        result = self.fc._get_crawl_summary(True, Messages.NR_OF_CRAWLED_DATA,
                                            ColoredBytes.ONE_KB_READABLE, ColoredBytes.ONE_KB_RAW)
        expected_summary = (Messages.NR_OF_CRAWLED_DATA, ColoredBytes.ONE_KB_READABLE, ColoredBytes.ONE_KB_RAW, "\n\n")
        self.assertEqual(result, expected_summary)

    def test_summary_without_total_size(self):
        result = self.fc._get_crawl_summary(False, Messages.NR_OF_CRAWLED_DATA,
                                            ColoredBytes.ONE_KB_READABLE, ColoredBytes.ONE_KB_RAW)
        expected_summary = ("\n",)
        self.assertEqual(result, expected_summary)


class FolderCrawlerTestsFilterData(unittest.TestCase):

    def setUp(self):
        self.fc = FolderCrawler(path=CURRENT_DIRECTORY)
        self.EMPTY_DICT = {COLUMN_NAMES[0]: [], COLUMN_NAMES[1]: []}

    def test_filter_data_with_nan_values(self):
        files = pd.DataFrame({COLUMN_NAMES[0]: ['file1', 'file2'], COLUMN_NAMES[1]: ['change1', NONE]})
        folders = pd.DataFrame({COLUMN_NAMES[0]: ['folder1', 'folder2'], COLUMN_NAMES[1]: ['change1', NONE]})

        files, folders, skipped = self.fc._filter_data(files, folders, self.EMPTY_DICT, COLUMN_NAMES[1])

        self.assertEqual(len(files), 1)
        self.assertEqual(len(folders), 1)
        self.assertEqual(len(skipped), 2)

    def test_filter_data_without_nan_values(self):
        files = pd.DataFrame({COLUMN_NAMES[0]: ['file1', 'file2'], COLUMN_NAMES[1]: ['change1', 'change2']})
        folders = pd.DataFrame({COLUMN_NAMES[0]: ['folder1', 'folder2'], COLUMN_NAMES[1]: ['change1', 'change2']})

        files, folders, skipped = self.fc._filter_data(files, folders, self.EMPTY_DICT, COLUMN_NAMES[1])

        self.assertEqual(len(files), 2)
        self.assertEqual(len(folders), 2)
        self.assertEqual(len(skipped), 0)


class FolderCrawlerTestsGetCrawledData(unittest.TestCase):
    def setUp(self):
        self.folder_crawler = FolderCrawler(path=CURRENT_DIRECTORY)
        self.unprocessedDataframe = pd.DataFrame([(('path1', 'change1', 'size1', 'bytes1'), True),
                                                  (('path2', 'change2', 'size2', 'bytes2'), False)])

    # do not put @staticmethod decorator here, else the test will not work
    def test_get_crawled_data_with_folder(self):
        result = FolderCrawler._get_crawled_data(self.unprocessedDataframe, is_folder=True)
        expected = pd.DataFrame({COLUMN_NAMES[0]: ['path1'], COLUMN_NAMES[1]: ['change1'],
                                 COLUMN_NAMES[2]: ['size1'], COLUMN_NAMES[3]: ['bytes1']})

        pd.testing.assert_frame_equal(result.reset_index(), expected.reset_index())

    # do not put @staticmethod decorator here, else the test will not work
    def test_get_crawled_data_with_file(self):
        result = FolderCrawler._get_crawled_data(self.unprocessedDataframe, is_folder=False)
        expected = pd.DataFrame({COLUMN_NAMES[0]: ['path2'], COLUMN_NAMES[1]: ['change2'],
                                 COLUMN_NAMES[2]: ['size2'], COLUMN_NAMES[3]: ['bytes2']})
        pd.testing.assert_frame_equal(result, expected)


class FolderCrawlerTestsCrawlShallow(unittest.TestCase):
    def setUp(self):
        self.fc = FolderCrawler(path=CURRENT_DIRECTORY)

    def test_shallow_crawl_with_no_subdirectories(self):
        os.mkdir(TEMP_DIR)
        result = self.fc._crawl_shallow(TEMP_DIR)
        os.rmdir(TEMP_DIR)
        nr_of_found_items_expected = 0

        self.assertEqual(len(result), nr_of_found_items_expected)

    def test_shallow_crawl_with_one_subdirectory(self):
        # Prepare the test environment
        test_helper = TestHelper(TEMP_DIR,
                                 os.path.join(TEMP_DIR, SUB_DIR_1))
        test_helper.create_test_paths(TEST_TEXT)

        # Run test
        result = self.fc._crawl_shallow(TEMP_DIR)

        # Clean up the test environment
        test_helper.delete_test_paths()

        # Evaluate
        nr_of_found_items_expected = 1
        self.assertEqual(len(result), nr_of_found_items_expected)

    def test_shallow_crawl_with_multiple_subdirectories(self):
        # Prepare the test environment
        test_helper = TestHelper(TEMP_DIR,
                                 os.path.join(TEMP_DIR, SUB_DIR_1),
                                 os.path.join(TEMP_DIR, SUB_DIR_2))
        test_helper.create_test_paths(TEST_TEXT)

        # Run test
        result = self.fc._crawl_shallow(TEMP_DIR)

        # Clean up the test environment
        test_helper.delete_test_paths()

        # Evaluate
        nr_of_found_items_expected = 2
        self.assertEqual(len(result), nr_of_found_items_expected)

    def test_shallow_crawl_with_no_subdirectories_and_files(self):
        # Prepare the test environment
        test_helper = TestHelper(TEMP_DIR,
                                 os.path.join(TEMP_DIR, TEMP_FILE_1))
        test_helper.create_test_paths(TEST_TEXT)

        # Run test
        result = self.fc._crawl_shallow(TEMP_DIR)

        # Clean up the test environment
        test_helper.delete_test_paths()

        # Evaluate
        nr_of_found_items_expected = 1
        self.assertEqual(len(result), nr_of_found_items_expected)

    def test_shallow_crawl_with_one_subdirectory_and_files(self):
        # Prepare the test environment
        test_helper = TestHelper(TEMP_DIR,
                                 os.path.join(TEMP_DIR, SUB_DIR_1),
                                 os.path.join(TEMP_DIR, SUB_DIR_1, TEMP_FILE_1))
        test_helper.create_test_paths(TEST_TEXT)

        # Run test
        result = self.fc._crawl_shallow(TEMP_DIR)

        # Clean up the test environment
        test_helper.delete_test_paths()

        # Evaluate
        nr_of_found_items_expected = 1
        self.assertEqual(len(result), nr_of_found_items_expected)

    def test_shallow_crawl_with_multiple_subdirectories_and_files(self):
        # Prepare the test environment
        test_helper = TestHelper(TEMP_DIR,
                                 os.path.join(TEMP_DIR, SUB_DIR_1),
                                 os.path.join(TEMP_DIR, SUB_DIR_2),
                                 os.path.join(TEMP_DIR, SUB_DIR_1, TEMP_FILE_1),
                                 os.path.join(TEMP_DIR, SUB_DIR_2, TEMP_FILE_2))
        test_helper.create_test_paths(TEST_TEXT)

        # Run test
        result = self.fc._crawl_shallow(TEMP_DIR)

        # Clean up the test environment
        test_helper.delete_test_paths()

        # Evaluate
        nr_of_found_items_expected = 2
        self.assertEqual(len(result), nr_of_found_items_expected)

    def test_shallow_crawl_with_multiple_subdirectories_and_files_2(self):
        # Prepare the test environment
        test_helper = TestHelper(TEMP_DIR,
                                 os.path.join(TEMP_DIR, SUB_DIR_1),
                                 os.path.join(TEMP_DIR, SUB_DIR_2),
                                 os.path.join(TEMP_DIR, TEMP_FILE_1),
                                 os.path.join(TEMP_DIR, TEMP_FILE_2))
        test_helper.create_test_paths(TEST_TEXT)

        # Run test
        result = self.fc._crawl_shallow(TEMP_DIR)

        # Clean up the test environment
        test_helper.delete_test_paths()

        # Evaluate
        nr_of_found_items_expected = 4
        self.assertEqual(len(result), nr_of_found_items_expected)


class FolderCrawlerTestsCrawlDeep(unittest.TestCase):
    def setUp(self):
        self.fc = FolderCrawler(path=CURRENT_DIRECTORY)

    def test_crawl_deep_with_no_subdirectories(self):
        os.mkdir(TEMP_DIR)
        result = self.fc._crawl_deep(TEMP_DIR)
        os.rmdir(TEMP_DIR)
        nr_of_found_items_expected = 0
        self.assertEqual(len(result), nr_of_found_items_expected)

    def test_crawl_deep_with_one_subdirectory(self):
        # Prepare the test environment
        test_helper = TestHelper(TEMP_DIR, os.path.join(TEMP_DIR, SUB_DIR_1))
        test_helper.create_test_paths(TEST_TEXT)

        # Run test
        result = self.fc._crawl_deep(TEMP_DIR)

        # Clean up the test environment
        test_helper.delete_test_paths()

        # Evaluate
        nr_of_found_items_expected = 1
        self.assertEqual(len(result), nr_of_found_items_expected)

    def test_crawl_deep_with_multiple_subdirectories(self):
        # Prepare the test environment
        test_helper = TestHelper(TEMP_DIR,
                                 os.path.join(TEMP_DIR, SUB_DIR_1),
                                 os.path.join(TEMP_DIR, SUB_DIR_2))
        test_helper.create_test_paths(TEST_TEXT)

        # Run test
        result = self.fc._crawl_deep(TEMP_DIR)

        # Clean up the test environment
        test_helper.delete_test_paths()

        # Evaluate
        nr_of_found_items_expected = 2
        self.assertEqual(len(result), nr_of_found_items_expected)

    def test_crawl_deep_with_no_subdirectories_and_files(self):
        # Prepare the test environment
        test_helper = TestHelper(TEMP_DIR,
                                 os.path.join(TEMP_DIR, TEMP_FILE_1))
        test_helper.create_test_paths(TEST_TEXT)

        # Run test
        result = self.fc._crawl_deep(TEMP_DIR)

        # Clean up the test environment
        test_helper.delete_test_paths()

        # Evaluate
        nr_of_found_items_expected = 1
        self.assertEqual(len(result), nr_of_found_items_expected)

    def test_crawl_deep_with_one_subdirectory_and_files(self):
        # Prepare the test environment
        test_helper = TestHelper(TEMP_DIR,
                                 os.path.join(TEMP_DIR, SUB_DIR_1),
                                 os.path.join(TEMP_DIR, SUB_DIR_1, TEMP_FILE_1))
        test_helper.create_test_paths(TEST_TEXT)

        # Run test
        result = self.fc._crawl_deep(TEMP_DIR)

        # Clean up the test environment
        test_helper.delete_test_paths()

        # Evaluate
        nr_of_found_items_expected = 2
        self.assertEqual(len(result), nr_of_found_items_expected)

    def test_crawl_deep_with_multiple_subdirectories_and_files(self):
        # Prepare the test environment
        test_helper = TestHelper(TEMP_DIR,
                                 os.path.join(TEMP_DIR, SUB_DIR_1),
                                 os.path.join(TEMP_DIR, SUB_DIR_2),
                                 os.path.join(TEMP_DIR, SUB_DIR_1, TEMP_FILE_1),
                                 os.path.join(TEMP_DIR, SUB_DIR_2, TEMP_FILE_2))
        test_helper.create_test_paths(TEST_TEXT)

        # Run test
        result = self.fc._crawl_deep(TEMP_DIR)

        # Clean up the test environment
        test_helper.delete_test_paths()

        # Evaluate
        nr_of_found_items_expected = 4
        self.assertEqual(len(result), nr_of_found_items_expected)


class FolderCrawlerTestsSaveCrawlResults(unittest.TestCase):
    def setUp(self):
        self.fc = FolderCrawler(path=CURRENT_DIRECTORY)

    def test_save_crawl_result_with_existing_path(self):
        # Prepare the test environment
        TEST_DATAFRAME.to_csv(TEMP_FILE_1, index=False)
        # Run test
        self.fc._save_result(TEMP_FILE_1, TEST_DATAFRAME)
        # Evaluate
        self.assertTrue(os.path.exists(TEMP_FILE_1))
        # Clean up the test environment
        os.remove(TEMP_FILE_1)

    def test_save_crawl_result_with_non_existing_path(self):
        # Run test
        self.fc._save_result(TEMP_FILE_1, TEST_DATAFRAME)
        # Evaluate
        self.assertTrue(os.path.exists(TEMP_FILE_1))
        # Clean up the test environment
        os.remove(TEMP_FILE_1)


class FolderCrawlerTestsGetSizeOfItem(unittest.TestCase):
    def setUp(self):
        self.fc = FolderCrawler(path=CURRENT_DIRECTORY)

    def test_size_of_existing_file(self):
        # Prepare the test environment
        test_helper = TestHelper(TEMP_FILE_1)
        test_helper.create_test_paths(TEST_TEXT)

        # Run test
        result = self.fc._get_size_of_item(TEMP_FILE_1, get_size_folder=False)

        # Clean up the test environment
        test_helper.delete_test_paths()

        # Evaluate
        size_of_file_expected = 37
        self.assertEqual(result, size_of_file_expected)

    def test_size_of_non_existing_file(self):
        result = self.fc._get_size_of_item(NOT_EXISTING_FILE, get_size_folder=False)
        self.assertIs(result, NONE)

    def test_size_of_existing_directory(self):
        # Prepare the test environment
        test_helper = TestHelper(TEMP_DIR, os.path.join(TEMP_DIR, TEMP_FILE_1))
        test_helper.create_test_paths(TEST_TEXT)

        # Run test
        result = self.fc._get_size_of_item(TEMP_DIR, get_size_folder=True)

        # Clean up the test environment
        test_helper.delete_test_paths()

        # Evaluate
        size_of_folder_expected = 37
        self.assertEqual(result, size_of_folder_expected)


class FolderCrawlerTestsTabulateData(unittest.TestCase):
    def setUp(self):
        self.fc = FolderCrawler(path=CURRENT_DIRECTORY)

    def test_tabulate_data_with_empty_dataframe(self):
        result = self.fc._tabulate_data(EMPTY_DATAFRAME)
        expected = tabulate(EMPTY_DATAFRAME, headers=TABLE_HEADER, tablefmt=TABLE_FORMAT)
        self.assertEqual(result, expected)

    def test_tabulate_data_with_non_empty_dataframe(self):
        result = self.fc._tabulate_data(TEST_DATAFRAME)
        expected = tabulate(TEST_DATAFRAME, headers=TABLE_HEADER, tablefmt=TABLE_FORMAT)
        self.assertEqual(result, expected)


class FolderCrawlerTestsGetLastChangeOfItem(unittest.TestCase):
    def setUp(self):
        self.fc = FolderCrawler(path=CURRENT_DIRECTORY)

    def test_last_change_of_existing_file(self):
        # Prepare the test environment
        test_helper = TestHelper(TEMP_FILE_1)
        test_helper.create_test_paths(TEST_TEXT)

        # Run test
        result = self.fc._get_last_change_of_item(TEMP_FILE_1)

        # Clean up the test environment
        test_helper.delete_test_paths()

        # Evaluate
        self.assertIsInstance(result, datetime.datetime)

    def test_last_change_of_non_existing_file(self):
        result = self.fc._get_last_change_of_item(NOT_EXISTING_FILE)
        self.assertIs(result, NONE)


class FolderCrawlerTestsFilterSubdirectories(unittest.TestCase):
    def setUp(self):
        self.fc = FolderCrawler(path=CURRENT_DIRECTORY)

    def test_filter_subdirectories_with_no_subdirectories(self):
        # Prepare the test environment
        # We do not have to use complete dataset with all columns. Just one column with paths is enough for this test.
        PATHS_WITH_NO_SUBDIRS = {COLUMN_NAMES[0]: ['C:/Users', 'C:/Downloads', 'C:/Documents']}
        df = pd.DataFrame(PATHS_WITH_NO_SUBDIRS)

        # Run test
        result = self.fc._filter_subdirectories(df, COLUMN_NAMES[0])

        # Evaluate
        number_of_paths_expected = 3
        self.assertEqual(len(result), number_of_paths_expected)

    def test_filter_subdirectories_with_subdirectories(self):
        result = self.fc._filter_subdirectories(TEST_DATAFRAME, COLUMN_NAMES[0])
        number_of_paths_expected = 1
        self.assertEqual(len(result), number_of_paths_expected)

    def test_filter_subdirectories_with_mixed_paths(self):
        # Prepare the test environment
        # We do not have to use complete dataset with all columns. Just one column with paths is enough for this test.
        MIXED_PATHS = {COLUMN_NAMES[0]: ['C:/Users', 'C:/Users/Subfolder', 'C:/Downloads']}
        df = pd.DataFrame(MIXED_PATHS)

        # Run test
        result = self.fc._filter_subdirectories(df, COLUMN_NAMES[0])

        # Evaluate
        number_of_paths_expected = 2
        self.assertEqual(len(result), number_of_paths_expected)


class FolderCrawlerTestsGetTimePerformance(unittest.TestCase):
    def setUp(self):
        self.fc = FolderCrawler(path=CURRENT_DIRECTORY)

    def test_time_performance_returns_positive(self):
        TIME = 0.1
        start_time = time.perf_counter()
        time.sleep(TIME)
        time_result = self.fc._get_time_performance(start_time)
        time_expected = TIME

        self.assertGreater(time_result, time_expected)

    def test_time_performance_returns_zero_for_same_time(self):
        start_time = time.perf_counter()
        time_result = self.fc._get_time_performance(start_time).__floor__()
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
        self.assertEqual(result, ColoredBytes.ONE_KB_READABLE)

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
        self.fc._initialize_storage(SavedCrawls.ROOT,
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


class TestFolderCrawlerReadContentOfOneFile(unittest.TestCase):

    def setUp(self):
        self.fc = FolderCrawler(path=CURRENT_DIRECTORY)

    def test_read_content_of_one_file_one_line(self):
        # Prepare the test environment
        test_helper = TestHelper(TEMP_FILE_1)
        test_helper.create_test_paths(TEST_TEXT)

        # Run test
        result = self.fc._read_content_of_one_file(TEMP_FILE_1)

        # Clean up the test environment
        test_helper.delete_test_paths()

        # Evaluate
        self.assertEqual(result[0], TEST_TEXT)

    def test_read_content_of_one_file_one_line_matches_filter(self):
        # Prepare the test environment
        test_helper = TestHelper(TEMP_FILE_1)
        test_helper.create_test_paths(TEST_TEXT)

        # Run test
        result = self.fc._read_content_of_one_file(TEMP_FILE_1, filter_file_content=TEST_TEXT)

        # Clean up the test environment
        test_helper.delete_test_paths()

        # Evaluate
        self.assertEqual(result[0], TEST_TEXT)

    def test_read_content_of_one_file_one_line_does_not_match_filter(self):
        # Prepare the test environment
        test_helper = TestHelper(TEMP_FILE_1)
        test_helper.create_test_paths(TEST_TEXT)

        # Run test
        result = self.fc._read_content_of_one_file(TEMP_FILE_1, filter_file_content=NOT_EXISTING_FILE_CONTENT)

        # Clean up the test environment
        test_helper.delete_test_paths()

        # Evaluate
        empty_list = []
        self.assertListEqual(result, empty_list)

    def test_read_content_of_one_file_multiple_lines(self):
        # Prepare the test environment
        test_helper = TestHelper(TEMP_FILE_1)
        test_helper.create_test_paths(TEST_TEXT + "\n" + TEST_TEXT + "\n" + TEST_TEXT)

        # Run test
        result = self.fc._read_content_of_one_file(TEMP_FILE_1)

        # Clean up the test environment
        test_helper.delete_test_paths()

        # Evaluate
        self.assertEqual(result[0], TEST_TEXT)
        self.assertEqual(result[1], TEST_TEXT)
        self.assertEqual(result[2], TEST_TEXT)

    def test_read_content_of_one_file_multiple_lines_matches_filter(self):
        # Prepare the test environment
        test_helper = TestHelper(TEMP_FILE_1)
        test_helper.create_test_paths(TEST_TEXT + "\n" + TEST_TEXT + "\n" + TEST_TEXT)

        # Run test
        result = self.fc._read_content_of_one_file(TEMP_FILE_1, filter_file_content=TEST_TEXT)

        # Clean up the test environment
        test_helper.delete_test_paths()

        # Evaluate
        self.assertEqual(result[0], TEST_TEXT)
        self.assertEqual(result[1], TEST_TEXT)
        self.assertEqual(result[2], TEST_TEXT)

    def test_read_content_of_one_file_multiple_lines_does_not_match_filter(self):
        # Prepare the test environment
        test_helper = TestHelper(TEMP_FILE_1)
        test_helper.create_test_paths(TEST_TEXT + "\n" + TEST_TEXT + "\n" + TEST_TEXT)

        # Run test
        result = self.fc._read_content_of_one_file(TEMP_FILE_1, filter_file_content=NOT_EXISTING_FILE_CONTENT)

        # Clean up the test environment
        test_helper.delete_test_paths()

        # Evaluate
        self.assertListEqual(result, [])


class TestFolderCrawlerReadContentOfMultipleFiles(unittest.TestCase):

    def setUp(self):
        self.fc = FolderCrawler(path=CURRENT_DIRECTORY)

    def test_read_content_of_multiple_files_one_file_one_line(self):
        # Prepare the test environment
        test_helper = TestHelper(TEMP_FILE_1)
        test_helper.create_test_paths(TEST_TEXT)

        # Run test
        self.fc.files = pd.DataFrame({COLUMN_NAMES[0]: [TEMP_FILE_1]})
        result = self.fc._read_content_of_multiple_files()

        # Clean up the test environment
        test_helper.delete_test_paths()

        # Evaluate
        self.assertEqual(result[0][0], TEST_TEXT)

    def test_read_content_of_multiple_files_multiple_files_multiple_lines(self):
        # Prepare the test environment
        test_helper = TestHelper(TEMP_FILE_1, TEMP_FILE_2)
        test_helper.create_test_paths(TEST_TEXT + "\n" + TEST_TEXT + "\n" + TEST_TEXT)

        # Run test
        self.fc.files = pd.DataFrame({COLUMN_NAMES[0]: [TEMP_FILE_1, TEMP_FILE_2]})
        result = self.fc._read_content_of_multiple_files()

        # Clean up the test environment
        test_helper.delete_test_paths()

        # Evaluate
        file1 = result[0]
        file2 = result[1]
        self.assertEqual(file1[0], TEST_TEXT)  # line 0
        self.assertEqual(file1[1], TEST_TEXT)  # line 1
        self.assertEqual(file1[2], TEST_TEXT)  # line 2
        self.assertRaises(IndexError, lambda: file1[3])  # line 3

        self.assertEqual(file2[0], TEST_TEXT)
        self.assertEqual(file2[1], TEST_TEXT)
        self.assertEqual(file2[2], TEST_TEXT)
        self.assertRaises(IndexError, lambda: file2[3])

# endregion


if __name__ == '__main__':
    unittest.main()
