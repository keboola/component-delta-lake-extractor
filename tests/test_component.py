import unittest
import mock
import os
from freezegun import freeze_time

from keboola.component.dao import SupportedDataTypes

from component import Component


class TestComponent(unittest.TestCase):
    # set global time to 2010-10-10 - affects functions like datetime.now()
    @freeze_time("2010-10-10")
    # set KBC_DATADIR env to non-existing dir
    @mock.patch.dict(os.environ, {"KBC_DATADIR": "./non-existing-dir"})
    def test_run_no_cfg_fails(self):
        with self.assertRaises(ValueError):
            comp = Component()
            comp.run()

    def test_convert_base_types(self):
        cases = {
            "DECIMAL(10,0)": SupportedDataTypes.NUMERIC,
            "DECIMAL(18,2)": SupportedDataTypes.NUMERIC,
            "NUMERIC": SupportedDataTypes.NUMERIC,
            "FLOAT": SupportedDataTypes.FLOAT,
            "REAL": SupportedDataTypes.FLOAT,
            "DOUBLE": SupportedDataTypes.FLOAT,
            "BIGINT": SupportedDataTypes.INTEGER,
            "BOOLEAN": SupportedDataTypes.BOOLEAN,
            "TIMESTAMP WITH TIME ZONE": SupportedDataTypes.TIMESTAMP,
            "DATE": SupportedDataTypes.DATE,
            "VARCHAR": SupportedDataTypes.STRING,
        }
        for dtype, expected in cases.items():
            self.assertEqual(Component.convert_base_types(dtype), expected, msg=dtype)

    def test_to_base_type_preserves_length(self):
        # precision/scale and varchar length are preserved
        cases = {
            "DECIMAL(4,2)": (SupportedDataTypes.NUMERIC, "4,2"),
            "DECIMAL(38, 18)": (SupportedDataTypes.NUMERIC, "38,18"),
            "VARCHAR(255)": (SupportedDataTypes.STRING, "255"),
            # types without meaningful length carry none
            "BIGINT": (SupportedDataTypes.INTEGER, None),
            "DOUBLE": (SupportedDataTypes.FLOAT, None),
            "DATE": (SupportedDataTypes.DATE, None),
        }
        for dtype, (expected_base, expected_len) in cases.items():
            bt = Component.to_base_type(dtype)["base"]
            self.assertEqual(bt.dtype, expected_base.value, msg=dtype)
            self.assertEqual(bt.length, expected_len, msg=dtype)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
