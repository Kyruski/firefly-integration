from pyarrow import parquet

from .extensions import *
from .service import *

# KLUDGE: Currently, awswrangler does not expose the use_deprecated_int96_timestamps parameter, so you can't disable
# them. This patch fixes that. If/when awswrangler changes their api, we should remove this.
orig = parquet.ParquetWriter.__init__


def init(self, use_deprecated_int96_timestamps=False, allow_truncated_timestamps=True, **kwargs):
    orig(self, use_deprecated_int96_timestamps=use_deprecated_int96_timestamps,
         allow_truncated_timestamps=allow_truncated_timestamps, **kwargs)


parquet.ParquetWriter.__init__ = init
