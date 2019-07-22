__all__ = [
    'DataOutOfRangeError',
    'EmptyFileError',
    'InvalidDataTypeError',
    'MissingRequiredColumnError',
    'NonOedColumnError',
    'NonOedDataError',
    'NonOedSchemaError',
    'NonOedSchemaAndColumnError',
    'NonOedSchemaColumnError',
    'NullDataInNonNullColumnError',
    'get_file_error',
    'OedError',
    'OedException',
    'CommandError',
    'ProcessError',
    'ReportingError',
    'OedWarning'
]

import sys


class OedException(Exception):
    etype = 'exception'
    code = 'OED_EXP100'
    code_desc = 'Base OED exception'

    def __init__(self, msg=None):
        self.msg = msg or '<no msg.>'

    def __repr__(self):
        return 'OED {}: {} {}'.format(
            self.etype,
            self.code,
            self.code_desc
        )

    def __str__(self):
        return self.__repr__()


class OedError(OedException):
    etype = 'error'
    code = 'E200'
    code_desc = 'Base OED error'


class NonOedSchemaAndColumnError(OedError):
    code = 'E301'
    code_desc = 'Not a valid OED schema and column'


class NonOedSchemaError(OedError):
    code = 'E302'
    code_desc = 'Not a valid OED schema type'


class NonOedColumnError(OedError):
    code = 'E303'
    code_desc = 'Not a valid column in any OED schema'


class NonOedSchemaColumnError(OedError):
    code = 'E304'
    code_desc = 'Not a valid column in the given OED schema'


class ProcessError(OedError):
    code = 'E211'
    code_desc = 'OED process error'

    def __repr__(self):
        return super(self.__class__, self).__repr__() + ': {}'.format(self.msg)


class CommandError(OedError):
    code = 'E221'
    code_desc = 'OED command error'

    def __repr__(self):
        return super(self.__class__, self).__repr__() + ': {}'.format(self.msg)


class ReportingError(OedError):
    code = 'E231'
    code_desc = 'OED reporting error'

    def __repr__(self):
        return super(self.__class__, self).__repr__() + ': {}'.format(self.msg)


class EmptyFileError(OedError):
    code = 'E321'
    code_desc = 'File has no data'


class InvalidDataTypeError(OedError):
    code = 'E351'
    code_desc = 'Invalid data type(s) in column'


class MissingRequiredColumnError(OedError):
    code = 'E331'
    code_desc = 'Missing required column in file'


class NonOedDataError(OedError):
    code = 'E341'
    code_desc = 'Invalid or non-OED data in column'


class NullDataInNonNullColumnError(OedError):
    code = 'E361'
    code_desc = 'Null data found in non-null column'


class DataOutOfRangeError(OedError):
    code = 'E371'
    code_desc = 'Out of range data found in column'


class OedWarning(OedException):
    etype = 'warning'
    code = 'W261'
    code_desc = 'Base OED warning'


def get_file_error(err_shortdesc, err_msg=None):
    err_classname = '{}Error'.format(
        ''.join([s.capitalize() for s in err_shortdesc.split()])
    )
    return getattr(sys.modules[__name__], err_classname)(err_msg)
