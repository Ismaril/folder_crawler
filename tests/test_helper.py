import os

from structures import SavedCrawls, FileOps


class TestHelper:

    def __init__(self, *args: str):
        self.args = args

    def assert_text_files(self, *text_to_write: str, use_the_same_text: bool):
        if use_the_same_text:
            return True

        number_of_text_files = 0
        for path in self.args:
            if path.endswith(SavedCrawls.EXTENSION):
                number_of_text_files += 1

        number_of_text_messages = len(text_to_write)

        return number_of_text_messages == number_of_text_files

    def create_test_paths(self, *text_to_write: str, use_the_same_text=True):

        assert self.assert_text_files(*text_to_write, use_the_same_text=use_the_same_text), \
            (f"Number of text files does not match the number of text messages in TestHelper. "
             f"To fix this, the number of text messages must be equal to the number of text files, "
             f"if 'use_the_same_text' parameter is False.")

        text_to_write_index = 0
        for path in self.args:
            if path.endswith(SavedCrawls.EXTENSION):
                with open(path, FileOps.WRITE_MODE) as f:
                    f.write(text_to_write[text_to_write_index])
                    if not use_the_same_text:
                        text_to_write_index += 1
            else:
                os.mkdir(path)

    def delete_test_paths(self):
        args_reversed = self.args[::-1]
        for path in args_reversed:
            if path.endswith(SavedCrawls.EXTENSION):
                os.remove(path)
            else:
                os.rmdir(path)

    @staticmethod
    def delete_saved_crawls():
        os.remove(SavedCrawls.FILES)
        os.remove(SavedCrawls.FOLDERS)
        os.remove(SavedCrawls.SKIPPED)
        os.rmdir(SavedCrawls.ROOT)
