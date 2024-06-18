import os

from structures import SavedCrawls, FileOps


class TestHelper:
    """
    This class is used to create and delete test paths for the tests.
    """

    def __init__(self, *paths: str):
        self.paths = paths

    def assert_text_files(self, *text_to_write: str, use_the_same_text: bool):
        """
        This method asserts if the number of text files matches the number of text messages.

        :param text_to_write: Text messages which will be written into the text files.
        :param use_the_same_text: If True, the same text will be written into all text files.
        """
        if use_the_same_text:
            return True

        number_of_text_files = 0
        for path in self.paths:
            if path.endswith(SavedCrawls.EXTENSION):
                number_of_text_files += 1

        number_of_text_messages = len(text_to_write)

        return number_of_text_messages == number_of_text_files

    def create_test_paths(self, *texts_to_write: str, use_the_same_text=True):
        """
        This method creates test paths with folders and with text files.

        :param texts_to_write: Text messages which will be written into the text files.
        :param use_the_same_text: If True, the same text will be written into all text files.
        """

        assert self.assert_text_files(*texts_to_write, use_the_same_text=use_the_same_text), \
            (f"Number of text files does not match the number of text messages in TestHelper.\n"
             f"To fix this, the number of text messages must be equal to the number of text files,\n"
             f"if 'use_the_same_text' parameter is False.\n"
             f"Number of text files: {len(self.paths)}, number of text messages: {len(texts_to_write)}.")

        text_to_write_index = 0
        for path in self.paths:
            if path.endswith(SavedCrawls.EXTENSION):
                with open(path, FileOps.WRITE_MODE) as f:
                    f.write(texts_to_write[text_to_write_index])
                    if not use_the_same_text:
                        text_to_write_index += 1
            else:
                os.mkdir(path)

    def delete_test_paths(self):
        """
        This method automatically deletes all test paths which were crated by the TestHelper in method "create_test_paths".
        """
        paths_positions_reversed = self.paths[::-1]
        for path in paths_positions_reversed:
            if path.endswith(SavedCrawls.EXTENSION):
                os.remove(path)
            else:
                os.rmdir(path)

    @staticmethod
    def delete_saved_crawls():
        """
        This method deletes all files containing saved crawls and the root folder of the saved crawls.
        """
        os.remove(SavedCrawls.FILES)
        os.remove(SavedCrawls.FOLDERS)
        os.remove(SavedCrawls.SKIPPED)
        os.rmdir(SavedCrawls.ROOT)
