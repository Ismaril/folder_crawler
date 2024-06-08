import os
import time
import unittest
import datetime
import pandas as pd

from tabulate import tabulate
from colorama import Style, Fore
from structures import SavedCrawls, Messages, ColorFormatting, ByteSize, ItemType
from folder_crawler import FolderCrawler, NONE, COLUMN_NAMES
from unittest.mock import patch, mock_open


# TODO: The tests were wirtten by AI by large. There are a lot of hardcoded strings. To
#  make it more modular and change-proof, use existing constants or create some variables.


# region Integration tests
class FolderCrawlerTestsMain(unittest.TestCase):
    def setUp(self):
        self.folder_crawler = FolderCrawler(path=".")

    def test_main_1(self):
        os.mkdir('temp_dir')
        with open('temp_dir/temp_file.txt', 'w') as f:
            f.write('This is a temporary file for testing.')
        self.folder_crawler.path = "temp_dir"
        self.folder_crawler.main()
        os.remove('temp_dir/temp_file.txt')
        os.rmdir('temp_dir')
        os.remove(SavedCrawls.FILES)
        os.remove(SavedCrawls.FOLDERS)
        os.remove(SavedCrawls.SKIPPED)
        os.rmdir(SavedCrawls.SAVED_CRAWLS_FOLDER)

        self.assertTrue(self.folder_crawler.files[COLUMN_NAMES[0]][0] == r"temp_dir\temp_file.txt")
        self.assertTrue(self.folder_crawler.files[COLUMN_NAMES[3]][0] == "\x1b[31m 37 \x1b[0m")

    def test_main_2(self):
        os.mkdir('temp_dir')
        os.mkdir('temp_dir/sub_dir')
        with open('temp_dir/sub_dir/temp_file.txt', 'w') as f:
            f.write('This is a temporary file for testing.')
        self.folder_crawler.path = "temp_dir"
        self.folder_crawler.main()
        os.remove('temp_dir/sub_dir/temp_file.txt')
        os.rmdir('temp_dir/sub_dir')
        os.rmdir('temp_dir')
        os.remove(SavedCrawls.FILES)
        os.remove(SavedCrawls.FOLDERS)
        os.remove(SavedCrawls.SKIPPED)
        os.rmdir(SavedCrawls.SAVED_CRAWLS_FOLDER)

        self.assertTrue(self.folder_crawler.files[COLUMN_NAMES[0]][0] == r"temp_dir\sub_dir\temp_file.txt")
        self.assertTrue(self.folder_crawler.files[COLUMN_NAMES[3]][0] == "\x1b[31m 37 \x1b[0m")
        self.assertTrue(self.folder_crawler.folders[COLUMN_NAMES[0]][0] == r"temp_dir\sub_dir")
        self.assertTrue(self.folder_crawler.folders[COLUMN_NAMES[3]][0] == "\x1b[31m 37 \x1b[0m")
        self.assertTrue(self.folder_crawler.skipped.empty)


class FolderCrawlerTestsGetPathWithProperties(unittest.TestCase):
    """
    In these tests we are not really focusing on details which the method returns.
    We just focus if the method returns the correct number of items and if the second item is boolean.
    """

    def setUp(self):
        self.folder_crawler = FolderCrawler(path=".")

    def test_get_path_with_properties_with_no_subdirectories1(self):
        os.mkdir('temp_dir')
        with open('temp_dir/temp_file.txt', 'w') as f:
            f.write('This is a temporary file for testing.')
        result = self.folder_crawler._get_path_with_properties(('temp_dir', True))
        os.remove('temp_dir/temp_file.txt')
        os.rmdir('temp_dir')
        self.assertEqual((len(result[0]), result[1]), (len(COLUMN_NAMES), True))

    def test_get_path_with_properties_with_no_subdirectories2(self):
        with open('temp_file.txt', 'w') as f:
            f.write('This is a temporary file for testing.')
        result = self.folder_crawler._get_path_with_properties(('temp_file.txt', False))
        os.remove('temp_file.txt')
        self.assertEqual((len(result[0]), result[1]), (len(COLUMN_NAMES), False))


class FolderCrawlerTestsResolveSizes(unittest.TestCase):
    def setUp(self):
        self.folder_crawler = FolderCrawler(path=".")

    def test_resolve_sizes_with_valid_size(self):
        size = 1024
        result = self.folder_crawler._resolve_sizes(size)
        self.assertEqual(result, ("\x1b[33m1.00KB\x1b[0m", "\x1b[33m 1024 \x1b[0m"))

    def test_resolve_sizes_with_none(self):
        size = NONE
        result = self.folder_crawler._resolve_sizes(size)
        self.assertEqual(result, (NONE, NONE))


# endregion

# region Unit tests

class FolderCrawlerTestsGetIntsFromStrDataFrameColumn(unittest.TestCase):
    def setUp(self):
        self.folder_crawler = FolderCrawler(path=".")
        self.folder_crawler.files = pd.DataFrame({
            'Path': ['path1', 'path2', 'path3'],
            'Changed': ['change1', 'change2', 'change3'],
            'Size readable': ['1.00KB', '2.00KB', '3.00KB'],  # formmating of colors ommited here
            'Size bytes': ['color 1024 color', 'color 2048 color', 'color 3072 color'] # formmating of colors ommited here
        })

    def test_extract_integers_from_string(self):
        result = self.folder_crawler._get_ints_from_str_dataframe_column(self.folder_crawler.files, COLUMN_NAMES[3])
        expected_result = pd.Series([1024, 2048, 3072], dtype='int64', name=COLUMN_NAMES[3])
        print(result)
        print(expected_result)
        pd.testing.assert_series_equal(result, expected_result)


class FolderCrawlerTestsFilterPath(unittest.TestCase):
    def setUp(self):
        self.container = pd.DataFrame({
            'Path': ['path1', 'path2', 'path3'],
            'Changed': ['change1', 'change2', 'change3'],
            'Size readable': ['1.00KB', '2.00KB', '3.00KB'],  # formmating of colors ommited here
            'Size bytes': ['1024', '2048', '3072']  # formmating of colors ommited here
        })

    def test_filter_paths_with_matching_substring(self):
        filter_path = 'path'
        result = FolderCrawler._filter_paths(self.container, filter_path, COLUMN_NAMES[0])
        self.assertEqual(len(result), 3)

    def test_filter_paths_with_no_matching_substring(self):
        filter_path = 'nonexistent'
        result = FolderCrawler._filter_paths(self.container, filter_path, COLUMN_NAMES[0])
        self.assertEqual(len(result), 0)


class FolderCrawlerTestsFilterSizes(unittest.TestCase):
    def setUp(self):
        self.container = pd.DataFrame({
            'Path': ['path1', 'path2', 'path3'],
            'Changed': ['change1', 'change2', 'change3'],
            # size readable does not have color formatting here since it is not important for the test
            'Size readable': ['1KB', '2KB', '3KB'],
            'Size bytes': ["\x1b[33m 1024 \x1b[0m", "\x1b[33m 2048 \x1b[0m", "\x1b[33m 3072 \x1b[0m"]
        })

    def test_filter_sizes_greater_than_equal(self):
        result = FolderCrawler._filter_sizes(self.container, 1024, ">=", pd.Series([1024, 2048, 3072]))
        self.assertEqual(len(result), 3)

    def test_filter_sizes_less_than_equal(self):
        result = FolderCrawler._filter_sizes(self.container, 1024, "<=", pd.Series([1024, 2048, 3072]))
        self.assertEqual(len(result), 1)


class FolderCrawlerTestsFilterLastChange(unittest.TestCase):
    def setUp(self):
        self.folder_crawler = FolderCrawler(path=".")

    def test_filter_last_change_greater_than_equal(self):
        data = pd.DataFrame(
            {'Path': ['path1', 'path2'], 'Changed': [datetime.datetime(2022, 1, 1), datetime.datetime(2022, 2, 1)]})
        filter_date = datetime.datetime(2022, 1, 1)
        result = FolderCrawler._filter_last_change(data, filter_date, ">=", COLUMN_NAMES[1])
        self.assertEqual(len(result), 2)

    def test_filter_last_change_less_than_equal(self):
        data = pd.DataFrame(
            {'Path': ['path1', 'path2'], 'Changed': [datetime.datetime(2022, 1, 1), datetime.datetime(2022, 2, 1)]})
        filter_date = datetime.datetime(2022, 1, 1)
        result = FolderCrawler._filter_last_change(data, filter_date, "<=", COLUMN_NAMES[1])
        self.assertEqual(len(result), 1)


class FolderCrawlerTestsLoadCrawledData(unittest.TestCase):
    def setUp(self):
        self.folder_crawler = FolderCrawler(path=".")

    def test_load_crawled_data_with_empty_container(self):
        folder = SavedCrawls.SAVED_CRAWLS_FOLDER
        files = SavedCrawls.FILES
        extension = SavedCrawls.EXTENSION
        os.mkdir(folder)
        with open(files, 'w') as f:
            f.write("Path,Changed,Size readable,Size bytes\n")

        empty_df = pd.DataFrame()
        item_type = ItemType.FILES
        result = FolderCrawler.load_crawled_data(empty_df, item_type, folder, extension)
        expected_path = os.path.join(folder, f"{item_type}{extension}")
        expected_df = pd.read_csv(expected_path)

        os.remove(files)
        os.rmdir(folder)
        pd.testing.assert_frame_equal(result, expected_df)

    def test_load_crawled_data_with_non_empty_container(self):
        non_empty_df = pd.DataFrame(
            {'Path': ['path1'], 'Changed': ['change1'], 'Size readable': ['size1'], 'Size bytes': ['bytes1']})
        item_type = "files"
        result = FolderCrawler.load_crawled_data(non_empty_df, item_type,
                                                 SavedCrawls.SAVED_CRAWLS_FOLDER, SavedCrawls.EXTENSION)
        pd.testing.assert_frame_equal(result, non_empty_df)


class FolderCrawlerTestsGetCrawlSummary(unittest.TestCase):
    # The strings will come into parameters also with color formating but for the simplicity, they are written as plain
    # strings here.
    def test_summary_with_total_size(self):
        result = FolderCrawler._get_crawl_summary(True, Messages.NR_OF_CRAWLED_DATA, "1.00KB", "1024B")
        self.assertEqual(result, (Messages.NR_OF_CRAWLED_DATA, "1.00KB", "1024B", "\n\n"))

    def test_summary_without_total_size(self):
        result = FolderCrawler._get_crawl_summary(False, Messages.NR_OF_CRAWLED_DATA, "1.00KB", "1024B")
        self.assertEqual(result, ("\n",))


class FolderCrawlerTestsFilterData(unittest.TestCase):
    def setUp(self):
        self.folder_crawler = FolderCrawler(path=".")

    def test_filter_data_with_nan_values(self):
        files = pd.DataFrame({'Path': ['file1', 'file2'], 'Changed': ['change1', NONE]})
        folders = pd.DataFrame({'Path': ['folder1', 'folder2'], 'Changed': ['change1', NONE]})
        empty_dataframe = {'Path': [], 'Changed': []}
        column = 'Changed'
        files, folders, skipped = FolderCrawler._filter_data(files, folders, empty_dataframe, column)
        self.assertEqual(len(files), 1)
        self.assertEqual(len(folders), 1)
        self.assertEqual(len(skipped), 2)

    def test_filter_data_without_nan_values(self):
        files = pd.DataFrame({'Path': ['file1', 'file2'], 'Changed': ['change1', 'change2']})
        folders = pd.DataFrame({'Path': ['folder1', 'folder2'], 'Changed': ['change1', 'change2']})
        empty_dataframe = {'Path': [], 'Changed': []}
        column = 'Changed'
        files, folders, skipped = FolderCrawler._filter_data(files, folders, empty_dataframe, column)
        self.assertEqual(len(files), 2)
        self.assertEqual(len(folders), 2)
        self.assertEqual(len(skipped), 0)


class FolderCrawlerTestsGetCrawledData(unittest.TestCase):
    def setUp(self):
        self.folder_crawler = FolderCrawler(path=".")

    def test_get_crawled_data_with_folder(self):
        data = [(('path1', 'change1', 'size1', 'bytes1'), True),
                (('path2', 'change2', 'size2', 'bytes2'), False)]
        df = pd.DataFrame(data)
        result = FolderCrawler._get_crawled_data(df, True)
        expected = pd.DataFrame({'Path': ['path1'], 'Changed': ['change1'],
                                 'Size readable': ['size1'], 'Size bytes': ['bytes1']})

        pd.testing.assert_frame_equal(result.reset_index(), expected.reset_index())

    def test_get_crawled_data_with_file(self):
        data = [(('path2', 'change2', 'size2', 'bytes2'), False),
                (('path3', 'change3', 'size3', 'bytes3'), True)]
        df = pd.DataFrame(data)
        result = FolderCrawler._get_crawled_data(df, False)
        expected = pd.DataFrame({'Path': ['path2'], 'Changed': ['change2'],
                                 'Size readable': ['size2'], 'Size bytes': ['bytes2']})
        pd.testing.assert_frame_equal(result, expected)


class FolderCrawlerTestsCrawlShallow(unittest.TestCase):
    def setUp(self):
        self.folder_crawler = FolderCrawler(path=".")

    def test_shallow_crawl_with_no_subdirectories(self):
        os.mkdir('temp_dir')
        result = FolderCrawler._crawl_shallow('temp_dir')
        os.rmdir('temp_dir')
        self.assertEqual(len(result), 0)

    def test_shallow_crawl_with_one_subdirectory(self):
        os.mkdir('temp_dir')
        os.mkdir('temp_dir/sub_dir')
        result = FolderCrawler._crawl_shallow('temp_dir')
        os.rmdir('temp_dir/sub_dir')
        os.rmdir('temp_dir')
        self.assertEqual(len(result), 1)

    def test_shallow_crawl_with_multiple_subdirectories(self):
        os.mkdir('temp_dir')
        os.mkdir('temp_dir/sub_dir1')
        os.mkdir('temp_dir/sub_dir2')
        result = FolderCrawler._crawl_shallow('temp_dir')
        os.rmdir('temp_dir/sub_dir1')
        os.rmdir('temp_dir/sub_dir2')
        os.rmdir('temp_dir')
        self.assertEqual(len(result), 2)

    def test_shallow_crawl_with_no_subdirectories_and_files(self):
        os.mkdir('temp_dir')
        with open('temp_dir/temp_file.txt', 'w') as f:
            f.write('This is a temporary file for testing.')
        result = FolderCrawler._crawl_shallow('temp_dir')
        os.remove('temp_dir/temp_file.txt')
        os.rmdir('temp_dir')
        self.assertEqual(len(result), 1)

    def test_shallow_crawl_with_one_subdirectory_and_files(self):
        os.mkdir('temp_dir')
        os.mkdir('temp_dir/sub_dir')
        with open('temp_dir/sub_dir/temp_file.txt', 'w') as f:
            f.write('This is a temporary file for testing.')
        result = FolderCrawler._crawl_shallow('temp_dir')
        os.remove('temp_dir/sub_dir/temp_file.txt')
        os.rmdir('temp_dir/sub_dir')
        os.rmdir('temp_dir')
        self.assertEqual(len(result), 1)

    def test_shallow_crawl_with_multiple_subdirectories_and_files(self):
        os.mkdir('temp_dir')
        os.mkdir('temp_dir/sub_dir1')
        os.mkdir('temp_dir/sub_dir2')
        with open('temp_dir/sub_dir1/temp_file.txt', 'w') as f:
            f.write('This is a temporary file for testing.')
        with open('temp_dir/sub_dir2/temp_file.txt', 'w') as f:
            f.write('This is a temporary file for testing.')
        result = FolderCrawler._crawl_shallow('temp_dir')
        os.remove('temp_dir/sub_dir1/temp_file.txt')
        os.remove('temp_dir/sub_dir2/temp_file.txt')
        os.rmdir('temp_dir/sub_dir1')
        os.rmdir('temp_dir/sub_dir2')
        os.rmdir('temp_dir')
        self.assertEqual(len(result), 2)

    def test_shallow_crawl_with_multiple_subdirectories_and_files_2(self):
        os.mkdir('temp_dir')
        os.mkdir('temp_dir/sub_dir1')
        os.mkdir('temp_dir/sub_dir2')
        with open('temp_dir/temp_file1.txt', 'w') as f:
            f.write('This is a temporary file for testing.')
        with open('temp_dir/temp_file2.txt', 'w') as f:
            f.write('This is a temporary file for testing.')
        result = FolderCrawler._crawl_shallow('temp_dir')
        os.remove('temp_dir/temp_file1.txt')
        os.remove('temp_dir/temp_file2.txt')
        os.rmdir('temp_dir/sub_dir1')
        os.rmdir('temp_dir/sub_dir2')
        os.rmdir('temp_dir')
        self.assertEqual(len(result), 4)


class FolderCrawlerTestsCrawlDeep(unittest.TestCase):
    def setUp(self):
        self.folder_crawler = FolderCrawler(path=".")

    def test_crawl_deep_with_no_subdirectories(self):
        os.mkdir('temp_dir')
        result = FolderCrawler._crawl_deep('temp_dir')
        os.rmdir('temp_dir')
        self.assertEqual(len(result), 0)

    def test_crawl_deep_with_one_subdirectory(self):
        os.mkdir('temp_dir')
        os.mkdir('temp_dir/sub_dir')
        result = FolderCrawler._crawl_deep('temp_dir')
        os.rmdir('temp_dir/sub_dir')
        os.rmdir('temp_dir')
        self.assertEqual(len(result), 1)

    def test_crawl_deep_with_multiple_subdirectories(self):
        os.mkdir('temp_dir')
        os.mkdir('temp_dir/sub_dir1')
        os.mkdir('temp_dir/sub_dir2')
        result = FolderCrawler._crawl_deep('temp_dir')
        os.rmdir('temp_dir/sub_dir1')
        os.rmdir('temp_dir/sub_dir2')
        os.rmdir('temp_dir')
        self.assertEqual(len(result), 2)

    def test_crawl_deep_with_no_subdirectories_and_files(self):
        os.mkdir('temp_dir')
        with open('temp_dir/temp_file.txt', 'w') as f:
            f.write('This is a temporary file for testing.')
        result = FolderCrawler._crawl_deep('temp_dir')
        os.remove('temp_dir/temp_file.txt')
        os.rmdir('temp_dir')
        self.assertEqual(len(result), 1)

    def test_crawl_deep_with_one_subdirectory_and_files(self):
        os.mkdir('temp_dir')
        os.mkdir('temp_dir/sub_dir')
        with open('temp_dir/sub_dir/temp_file.txt', 'w') as f:
            f.write('This is a temporary file for testing.')
        result = FolderCrawler._crawl_deep('temp_dir')
        os.remove('temp_dir/sub_dir/temp_file.txt')
        os.rmdir('temp_dir/sub_dir')
        os.rmdir('temp_dir')
        self.assertEqual(len(result), 2)

    def test_crawl_deep_with_multiple_subdirectories_and_files(self):
        os.mkdir('temp_dir')
        os.mkdir('temp_dir/sub_dir1')
        os.mkdir('temp_dir/sub_dir2')
        with open('temp_dir/sub_dir1/temp_file.txt', 'w') as f:
            f.write('This is a temporary file for testing.')
        with open('temp_dir/sub_dir2/temp_file.txt', 'w') as f:
            f.write('This is a temporary file for testing.')
        result = FolderCrawler._crawl_deep('temp_dir')
        os.remove('temp_dir/sub_dir1/temp_file.txt')
        os.remove('temp_dir/sub_dir2/temp_file.txt')
        os.rmdir('temp_dir/sub_dir1')
        os.rmdir('temp_dir/sub_dir2')
        os.rmdir('temp_dir')
        self.assertEqual(len(result), 4)


class FolderCrawlerTestsSaveCrawlResults(unittest.TestCase):
    def setUp(self):
        self.folder_crawler = FolderCrawler(path=".")

    def test_save_crawl_result_with_existing_path(self):
        path = 'temp_file.csv'
        data = {'Path': ['C:/Users', 'C:/Downloads', 'C:/Documents']}
        df = pd.DataFrame(data)
        df.to_csv(path, index=False)
        FolderCrawler._save_result(path, df)
        self.assertTrue(os.path.exists(path))
        os.remove(path)

    def test_save_crawl_result_with_non_existing_path(self):
        path = 'temp_file.csv'
        data = {'Path': ['C:/Users', 'C:/Downloads', 'C:/Documents']}
        df = pd.DataFrame(data)
        FolderCrawler._save_result(path, df)
        self.assertTrue(os.path.exists(path))
        os.remove(path)


class FolderCrawlerTestsGetSizeOfItem(unittest.TestCase):
    def setUp(self):
        self.folder_crawler = FolderCrawler(path=".")

    def test_size_of_existing_file(self):
        with open('temp_file.txt', 'w') as f:
            f.write('This is a temporary file for testing.')
        result = FolderCrawler._get_size_of_item('temp_file.txt', False)
        os.remove('temp_file.txt')
        self.assertEqual(result, 37)

    def test_size_of_non_existing_file(self):
        result = FolderCrawler._get_size_of_item('non_existing_file.txt', False)
        self.assertIs(result, NONE)

    def test_size_of_existing_directory(self):
        os.mkdir('temp_dir')
        with open('temp_dir/temp_file.txt', 'w') as f:
            f.write('This is a temporary file for testing.')
        result = FolderCrawler._get_size_of_item('temp_dir', True)
        os.remove('temp_dir/temp_file.txt')
        os.rmdir('temp_dir')
        self.assertEqual(result, 37)


class FolderCrawlerTestsTabulateData(unittest.TestCase):
    def setUp(self):
        self.folder_crawler = FolderCrawler(path=".")

    def test_tabulate_data_with_empty_dataframe(self):
        df = pd.DataFrame()
        result = FolderCrawler._tabulate_data(df)
        expected = tabulate(df, headers="keys", tablefmt="psql")
        self.assertEqual(result, expected)

    def test_tabulate_data_with_non_empty_dataframe(self):
        data = {'Path': ['C:/Users', 'C:/Downloads', 'C:/Documents']}
        df = pd.DataFrame(data)
        result = FolderCrawler._tabulate_data(df)
        expected = tabulate(df, headers="keys", tablefmt="psql")
        self.assertEqual(result, expected)


class FolderCrawlerTestsGetLastChangeOfItem(unittest.TestCase):
    def setUp(self):
        self.folder_crawler = FolderCrawler(path=".")

    def test_last_change_of_existing_file(self):
        with open('temp_file.txt', 'w') as f:
            f.write('This is a temporary file for testing.')
        result = FolderCrawler._get_last_change_of_item('temp_file.txt')
        os.remove('temp_file.txt')
        self.assertIsInstance(result, datetime.datetime)

    def test_last_change_of_non_existing_file(self):
        result = FolderCrawler._get_last_change_of_item('non_existing_file.txt')
        self.assertIs(result, NONE)


class FolderCrawlerTestsFilterSubdirectories(unittest.TestCase):
    def setUp(self):
        self.folder_crawler = FolderCrawler(path=".")

    def test_filter_subdirectories_with_no_subdirectories(self):
        data = {'Path': ['C:/Users', 'C:/Downloads', 'C:/Documents']}
        df = pd.DataFrame(data)
        result = FolderCrawler._filter_subdirectories(df, 'Path')
        self.assertEqual(len(result), 3)

    def test_filter_subdirectories_with_subdirectories(self):
        data = {'Path': ['C:/Users', 'C:/Users/Subfolder', 'C:/Users/Subfolder/Subfolder2']}
        df = pd.DataFrame(data)
        result = FolderCrawler._filter_subdirectories(df, 'Path')
        self.assertEqual(len(result), 1)

    def test_filter_subdirectories_with_mixed_paths(self):
        data = {'Path': ['C:/Users', 'C:/Users/Subfolder', 'C:/Downloads']}
        df = pd.DataFrame(data)
        result = FolderCrawler._filter_subdirectories(df, 'Path')
        self.assertEqual(len(result), 2)


class FolderCrawlerTestsGetTimePerformance(unittest.TestCase):
    def setUp(self):
        self.folder_crawler = FolderCrawler(path=".")

    def test_time_performance_returns_positive(self):
        start_time = time.perf_counter()
        time.sleep(0.1)
        result = self.folder_crawler.get_time_performance(start_time)
        self.assertGreater(result, 0.1)

    def test_time_performance_returns_zero_for_same_time(self):
        start_time = time.perf_counter()
        result = self.folder_crawler.get_time_performance(start_time)
        self.assertEqual(result.__floor__(), 0)


class FolderCrawlerTestsGetCurrentTime(unittest.TestCase):
    def setUp(self):
        self.folder_crawler = FolderCrawler(path=".")

    def test_current_time_returns_now(self):
        result = self.folder_crawler._get_current_time()
        now = datetime.datetime.now()
        self.assertEqual(result.year, now.year)
        self.assertEqual(result.month, now.month)
        self.assertEqual(result.day, now.day)
        self.assertEqual(result.hour, now.hour)
        self.assertEqual(result.minute, now.minute)


class FolderCrawlerTestsColorFormatString(unittest.TestCase):
    def setUp(self):
        self.folder_crawler = FolderCrawler(path=".")

    def test_color_format_string_long(self):
        result = self.folder_crawler._color_format_string(Fore.YELLOW, 1, "KB", Style.RESET_ALL, False)
        self.assertEqual(result, "\x1b[33m1.00KB\x1b[0m")

    def test_color_format_string_short(self):
        result = self.folder_crawler._color_format_string(Fore.YELLOW, 1024, None, Style.RESET_ALL, True)
        self.assertEqual(result, "\x1b[33m 1024 \x1b[0m")


class FolderCrawlerTestsConvertBytesToReadableFormat(unittest.TestCase):
    """
    Due to hassle with color formatting in hardcoded strings here,
    I decided to use instead a method to compare the results with actual test result.
    By that you have more control over the expected results, since previously it was just bunch of
    hard to read string characters. Disadvantage is that you have to rely on another external component,
    but I think it is worth it when all above taken into account.
    """

    def setUp(self):
        self.folder_crawler = FolderCrawler(path=".")

    def test_bytes_to_readable_format_zero_bytes(self):
        size = 0
        result_short, result_long = self.folder_crawler._convert_bytes_to_readable_format(
            size=size, colors=ColorFormatting.COLORS, units=ColorFormatting.UNITS,
            reset_formatting=Style.RESET_ALL, function=self.folder_crawler._color_format_string)
        short = self.folder_crawler._color_format_string(*(Fore.RED, size, "B", Style.RESET_ALL, False))
        long = self.folder_crawler._color_format_string(*(Fore.RED, size, None, Style.RESET_ALL, True))
        self.assertEqual(result_short, short)
        self.assertEqual(result_long, long)

    def test_bytes_to_readable_format_one_byte(self):
        size = ByteSize.BYTE
        result_short, result_long = self.folder_crawler._convert_bytes_to_readable_format(
            size=ByteSize.BYTE, colors=ColorFormatting.COLORS, units=ColorFormatting.UNITS,
            reset_formatting=Style.RESET_ALL, function=self.folder_crawler._color_format_string)
        short = self.folder_crawler._color_format_string(*(Fore.RED, 1, "B", Style.RESET_ALL, False))
        long = self.folder_crawler._color_format_string(*(Fore.RED, size, None, Style.RESET_ALL, True))
        self.assertEqual(result_short, short)
        self.assertEqual(result_long, long)

    def test_bytes_to_readable_format_one_kilobyte(self):
        size = ByteSize.KILOBYTE
        result_short, result_long = self.folder_crawler._convert_bytes_to_readable_format(
            size=size, colors=ColorFormatting.COLORS, units=ColorFormatting.UNITS,
            reset_formatting=Style.RESET_ALL, function=self.folder_crawler._color_format_string)
        short = self.folder_crawler._color_format_string(*(Fore.YELLOW, 1, "KB", Style.RESET_ALL, False))
        long = self.folder_crawler._color_format_string(*(Fore.YELLOW, size, None, Style.RESET_ALL, True))
        self.assertEqual(result_short, short)
        self.assertEqual(result_long, long)

    def test_bytes_to_readable_format_one_megabyte(self):
        size = ByteSize.MEGABYTE
        result_short, result_long = self.folder_crawler._convert_bytes_to_readable_format(
            size=size, colors=ColorFormatting.COLORS, units=ColorFormatting.UNITS,
            reset_formatting=Style.RESET_ALL, function=self.folder_crawler._color_format_string)
        short = self.folder_crawler._color_format_string(*(Fore.GREEN, 1, "MB", Style.RESET_ALL, False))
        long = self.folder_crawler._color_format_string(*(Fore.GREEN, size, None, Style.RESET_ALL, True))
        self.assertEqual(result_short, short)
        self.assertEqual(result_long, long)

    def test_bytes_to_readable_format_one_gigabyte(self):
        size = ByteSize.GIGABYTE
        result_short, result_long = self.folder_crawler._convert_bytes_to_readable_format(
            size=size, colors=ColorFormatting.COLORS, units=ColorFormatting.UNITS,
            reset_formatting=Style.RESET_ALL, function=self.folder_crawler._color_format_string)
        short = self.folder_crawler._color_format_string(*(Fore.BLUE, 1, "GB", Style.RESET_ALL, False))
        long = self.folder_crawler._color_format_string(*(Fore.BLUE, size, None, Style.RESET_ALL, True))
        self.assertEqual(result_short, short)
        self.assertEqual(result_long, long)

    def test_bytes_to_readable_format_one_terabyte(self):
        size = ByteSize.TERABYTE
        result_short, result_long = self.folder_crawler._convert_bytes_to_readable_format(
            size=size, colors=ColorFormatting.COLORS, units=ColorFormatting.UNITS,
            reset_formatting=Style.RESET_ALL, function=self.folder_crawler._color_format_string)
        short = self.folder_crawler._color_format_string(*(Fore.CYAN, 1, "TB", Style.RESET_ALL, False))
        long = self.folder_crawler._color_format_string(*(Fore.CYAN, size, None, Style.RESET_ALL, True))
        self.assertEqual(result_short, short)
        self.assertEqual(result_long, long)


class TestFolderCrawlerFormatTimespan(unittest.TestCase):
    def setUp(self):
        self.folder_crawler = FolderCrawler(path=".")

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

        with patch('folder_crawler.SavedCrawls.SAVED_CRAWLS_FOLDER', new=mock_saved_crawls_folder), \
                patch('folder_crawler.SavedCrawls.FILES', new=mock_files), \
                patch('folder_crawler.SavedCrawls.FOLDERS', new=mock_folders), \
                patch('folder_crawler.SavedCrawls.SKIPPED', new=mock_skipped), \
                patch('folder_crawler.FileOps.APPEND_MODE', new=mock_append_mode), \
                patch('folder_crawler.FileOps.ENCODING', new=mock_encoding):
            # Call the method under test
            FolderCrawler.initialize_storage(SavedCrawls.SAVED_CRAWLS_FOLDER,
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
