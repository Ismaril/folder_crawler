import os
import time
import unittest
import datetime
import pandas as pd

from tabulate import tabulate
from structures import SavedCrawls
from folder_crawler import FolderCrawler, NONE
from unittest.mock import patch, mock_open


class FolderCrawlerTestsLoadCrawledData(unittest.TestCase):
    def setUp(self):
        self.folder_crawler = FolderCrawler(path=".")

    def test_load_crawled_data_with_empty_container(self):
        os.mkdir(SavedCrawls.SAVED_CRAWLS_FOLDER)
        with open(SavedCrawls.FILES, 'w') as f:
            f.write("Path,Changed,Size readable,Size bytes\n")

        empty_df = pd.DataFrame()
        item_type = "files"
        result = FolderCrawler.load_crawled_data(empty_df, item_type)
        expected_path = os.path.join(SavedCrawls.SAVED_CRAWLS_FOLDER, f"{item_type}{SavedCrawls.EXTENSION}")
        expected_df = pd.read_csv(expected_path)

        os.remove(SavedCrawls.FILES)
        os.rmdir(SavedCrawls.SAVED_CRAWLS_FOLDER)
        pd.testing.assert_frame_equal(result, expected_df)

    def test_load_crawled_data_with_non_empty_container(self):
        non_empty_df = pd.DataFrame(
            {'Path': ['path1'], 'Changed': ['change1'], 'Size readable': ['size1'], 'Size bytes': ['bytes1']})
        item_type = "files"
        result = FolderCrawler.load_crawled_data(non_empty_df, item_type)
        pd.testing.assert_frame_equal(result, non_empty_df)


class FolderCrawlerTestsGetCrawlSummary(unittest.TestCase):
    # The strings will come into parameters also with color formating but for the simplicity, they are written as plain
    # strings here.
    def test_summary_with_total_size(self):
        result = FolderCrawler._get_crawl_summary(True, "1.00KB", "1024B")
        self.assertEqual(result, ("NUMBER OF CRAWLED DATA:", "1.00KB", "1024B", "\n\n"))

    def test_summary_without_total_size(self):
        result = FolderCrawler._get_crawl_summary(False, "1.00KB", "1024B")
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
        FolderCrawler._save_crawl_result(path, df)
        self.assertTrue(os.path.exists(path))
        os.remove(path)

    def test_save_crawl_result_with_non_existing_path(self):
        path = 'temp_file.csv'
        data = {'Path': ['C:/Users', 'C:/Downloads', 'C:/Documents']}
        df = pd.DataFrame(data)
        FolderCrawler._save_crawl_result(path, df)
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
        result = self.folder_crawler._get_time_performance(start_time)
        self.assertGreater(result, 0.1)

    def test_time_performance_returns_zero_for_same_time(self):
        start_time = time.perf_counter()
        result = self.folder_crawler._get_time_performance(start_time)
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


class FolderCrawlerTestsConvertBytesToReadableFormat(unittest.TestCase):
    def setUp(self):
        self.folder_crawler = FolderCrawler(path=".")

    def test_bytes_to_readable_format_zero_bytes(self):
        result_short, result_long = self.folder_crawler._convert_bytes_to_readable_format(0)
        self.assertEqual(result_short, "\033[0;31;10m0.00B\033[0m")
        self.assertEqual(result_long, "\033[0;31;10m 0 \033[0m")

    def test_bytes_to_readable_format_one_byte(self):
        result_short, result_long = self.folder_crawler._convert_bytes_to_readable_format(1)
        self.assertEqual(result_short, "\033[0;31;10m1.00B\033[0m")
        self.assertEqual(result_long, "\033[0;31;10m 1 \033[0m")

    def test_bytes_to_readable_format_one_kilobyte(self):
        result_short, result_long = self.folder_crawler._convert_bytes_to_readable_format(1024)
        self.assertEqual(result_short, "\033[0;33;10m1.00KB\033[0m")
        self.assertEqual(result_long, "\033[0;33;10m 1024 \033[0m")

    def test_bytes_to_readable_format_one_megabyte(self):
        result_short, result_long = self.folder_crawler._convert_bytes_to_readable_format(1024 * 1024)
        self.assertEqual(result_short, "\033[0;32;10m1.00MB\033[0m")
        self.assertEqual(result_long, "\033[0;32;10m 1048576 \033[0m")

    def test_bytes_to_readable_format_one_gigabyte(self):
        result_short, result_long = self.folder_crawler._convert_bytes_to_readable_format(1024 * 1024 * 1024)
        self.assertEqual(result_short, "\033[0;34;10m1.00GB\033[0m")
        self.assertEqual(result_long, "\033[0;34;10m 1073741824 \033[0m")

    def test_bytes_to_readable_format_one_terabyte(self):
        result_short, result_long = self.folder_crawler._convert_bytes_to_readable_format(1024 * 1024 * 1024 * 1024)
        self.assertEqual(result_short, "\033[0;36;10m1.00TB\033[0m")
        self.assertEqual(result_long, "\033[0;36;10m 1099511627776 \033[0m")


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
            FolderCrawler._initialize_files()

        # Assert that the directory was created
        mock_makedirs.assert_called_once_with(mock_saved_crawls_folder, exist_ok=True)

        # Assert that the files were created
        mock_open.assert_any_call(mock_files, mock_append_mode, encoding=mock_encoding)
        mock_open.assert_any_call(mock_folders, mock_append_mode, encoding=mock_encoding)
        mock_open.assert_any_call(mock_skipped, mock_append_mode, encoding=mock_encoding)
        self.assertEqual(mock_open.call_count, 3)


if __name__ == '__main__':
    unittest.main()
