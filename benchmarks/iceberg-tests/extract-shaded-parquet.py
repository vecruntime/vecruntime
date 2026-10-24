#!/usr/bin/env python3
# Build a minimal jar holding only Iceberg's SHADED parquet classes
# (org/apache/iceberg/shaded/org/apache/parquet/**), which iceberg-bridge's IcebergParquetInterop
# needs but which are absent from Iceberg's unshaded test modules. Avoids pulling the runtime jar's
# shaded Jackson/Spark onto the test classpath (that caused a Jackson NoSuchMethodError).
import sys, zipfile

src, dst = sys.argv[1], sys.argv[2]
PREFIX = "org/apache/iceberg/shaded/org/apache/parquet/"
n = 0
with zipfile.ZipFile(src) as zin, zipfile.ZipFile(dst, "w", zipfile.ZIP_DEFLATED) as zout:
    for info in zin.infolist():
        if info.filename.startswith(PREFIX):
            zout.writestr(info, zin.read(info.filename))
            n += 1
print(f"wrote {n} entries to {dst}")
