import io
import os

from .schema import SCHEMA_DIR
with io.open(os.path.join(SCHEMA_DIR, 'schema_version.txt'), 'r', encoding='utf-8') as f:
    __version__ = f.readline().strip()
