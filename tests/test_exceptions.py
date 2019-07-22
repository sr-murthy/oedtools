from unittest import TestCase

from oedtools.exceptions import (
    get_file_error,
    DataOutOfRangeError,
    EmptyFileError,
    InvalidDataTypeError,
    MissingRequiredColumnError,
    NonOedColumnError,
    NonOedDataError,
    NonOedSchemaAndColumnError,
    NonOedSchemaColumnError,
    NonOedSchemaError,
    NullDataInNonNullColumnError,
    OedError,
    CommandError,
    ProcessError,
    ReportingError,
)


class TestExceptions(TestCase):

    def test_get_file_error__data_out_of_range(self):
        self.assertIsInstance(get_file_error('data out of range', 'test'), DataOutOfRangeError)

    def test_get_file_error__empty_file(self):
        self.assertIsInstance(get_file_error('empty file', 'test'), EmptyFileError)

    def test_get_file_error__invalid_data_type(self):
        self.assertIsInstance(get_file_error('invalid data type', 'test'), InvalidDataTypeError)

    def test_get_file_error__missing_required_column(self):
        self.assertIsInstance(get_file_error('missing required column', 'test'), MissingRequiredColumnError)

    def test_get_file_error__non_oed_column(self):
        self.assertIsInstance(get_file_error('non oed column', 'test'), NonOedColumnError)

    def test_get_file_error__non_oed_data(self):
        self.assertIsInstance(get_file_error('non oed data', 'test'), NonOedDataError)

    def test_get_file_error__non_oed_schema_and_column(self):
        self.assertIsInstance(get_file_error('non oed schema and column', 'test'), NonOedSchemaAndColumnError)

    def test_get_file_error__non_oed_schema(self):
        self.assertIsInstance(get_file_error('non oed schema', 'test'), NonOedSchemaError)

    def test_get_file_error__null_data_in_non_null_column(self):
        self.assertIsInstance(get_file_error('null data in non null column', 'test'), NullDataInNonNullColumnError)

    def test_get_file_error__oed(self):
        self.assertIsInstance(get_file_error('oed', 'test'), OedError)

    def test_get_file_error__command(self):
        self.assertIsInstance(get_file_error('command', 'test'), CommandError)

    def test_get_file_error__process(self):
        self.assertIsInstance(get_file_error('process', 'test'), ProcessError)

    def test_get_file_error__reporting(self):
        self.assertIsInstance(get_file_error('reporting', 'test'), ReportingError)
