"""
Integration guard for reading a Delta table from Azure storage on a non-default port.

The component relies on two DuckDB extensions honouring `BlobEndpoint` (port included) when the table is
addressed as `az://<container>/<path>`: duckdb-azure reads the parquet files and duckdb-delta forwards the
endpoint to delta-kernel, which reads `_delta_log`. Unit tests only check the secret string, so a DuckDB or
extension bump could silently break this. This test runs a real `delta_scan` against Azurite listening on a
non-default port.

Run it with scripts/azurite_test.sh <image> (CI does). Skipped unless AZURITE_BLOB_ENDPOINT, AZURITE_ACCOUNT and
AZURITE_ACCOUNT_KEY are set.
"""
import base64
import hashlib
import hmac
import io
import json
import os
import tempfile
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import requests

AZURITE_BLOB_ENDPOINT = os.environ.get("AZURITE_BLOB_ENDPOINT")
# Not devstoreaccount1: for that name duckdb-delta switches delta-kernel into emulator mode, which ignores
# BlobEndpoint and so would not exercise the path production uses.
ACCOUNT = os.environ.get("AZURITE_ACCOUNT")
ACCOUNT_KEY = os.environ.get("AZURITE_ACCOUNT_KEY")
SAS_VERSION = "2020-08-04"


def account_sas() -> str:
    expiry = (datetime.now(timezone.utc) + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    fields = {"sp": "rwdlac", "ss": "b", "srt": "sco", "se": expiry, "spr": "https,http", "sv": SAS_VERSION}
    string_to_sign = "\n".join(
        [ACCOUNT, fields["sp"], fields["ss"], fields["srt"], "", fields["se"], "", fields["spr"], fields["sv"], ""]
    )
    signature = base64.b64encode(
        hmac.new(base64.b64decode(ACCOUNT_KEY), string_to_sign.encode(), hashlib.sha256).digest()
    ).decode()
    fields["sig"] = signature
    return "&".join(f"{k}={quote(v, safe='')}" for k, v in fields.items())


@unittest.skipUnless(AZURITE_BLOB_ENDPOINT and ACCOUNT and ACCOUNT_KEY, "Azurite not configured")
class TestAzuriteNonDefaultPort(unittest.TestCase):
    def setUp(self):
        self.sas = account_sas()
        self.container = f"delta-{uuid.uuid4().hex[:12]}"
        self._put(f"{self.container}?restype=container")

    def _put(self, path: str, body: bytes = b"", headers: dict = None):
        response = requests.put(f"{AZURITE_BLOB_ENDPOINT}/{path}{'&' if '?' in path else '?'}{self.sas}",
                                data=body, headers=headers or {}, timeout=30)
        response.raise_for_status()

    def _upload_delta_table(self, table_path: str, rows: pa.Table):
        buffer = io.BytesIO()
        pq.write_table(rows, buffer)
        data = buffer.getvalue()
        self._put(f"{self.container}/{table_path}/part-0.parquet", data, {"x-ms-blob-type": "BlockBlob"})

        schema = {
            "type": "struct",
            "fields": [
                {"name": "id", "type": "long", "nullable": True, "metadata": {}},
                {"name": "name", "type": "string", "nullable": True, "metadata": {}},
            ],
        }
        actions = [
            {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
            {"metaData": {"id": str(uuid.uuid4()), "format": {"provider": "parquet", "options": {}},
                          "schemaString": json.dumps(schema), "partitionColumns": [], "configuration": {},
                          "createdTime": 1700000000000}},
            {"add": {"path": "part-0.parquet", "partitionValues": {}, "size": len(data),
                     "modificationTime": 1700000000000, "dataChange": True}},
        ]
        log = "\n".join(json.dumps(a) for a in actions).encode()
        self._put(f"{self.container}/{table_path}/_delta_log/00000000000000000000.json", log,
                  {"x-ms-blob-type": "BlockBlob"})

    def test_delta_scan_over_az_uri_honours_blob_endpoint_port(self):
        rows = pa.table({"id": pa.array([1, 2, 3], pa.int64()), "name": ["a", "b", "c"]})
        self._upload_delta_table("schema/table", rows)

        with tempfile.TemporaryDirectory() as tmp:
            conn = duckdb.connect(config={"extension_directory": os.path.join(tmp, "extensions")})
            # Same secret shape and transport as Component.build_connection_query() emits when a port is set.
            conn.execute(
                f"""
                CREATE SECRET (
                    TYPE AZURE,
                    CONNECTION_STRING 'AccountName={ACCOUNT};SharedAccessSignature={self.sas};BlobEndpoint={AZURITE_BLOB_ENDPOINT}');
                SET azure_transport_option_type = 'curl';
                """
            )
            result = conn.execute(
                f"SELECT id, name FROM delta_scan('az://{self.container}/schema/table') ORDER BY id"
            ).fetchall()

        self.assertEqual(result, [(1, "a"), (2, "b"), (3, "c")])


if __name__ == "__main__":
    unittest.main()
