import sys
import unittest
from pathlib import Path


UTILITIES_DIR = Path(__file__).resolve().parents[1] / "src" / "copath_utilities"
sys.path.insert(0, str(UTILITIES_DIR))

from copath_texttypes import compile_report_fields


class CompileReportFieldsTests(unittest.TestCase):
    def test_prefixes_each_report_field_with_readable_name(self):
        report = compile_report_fields(
            {
                "final_diagnosis": "Primary finding",
                "addendum_diagnosis": "Additional finding",
            },
            ["final_diagnosis", "addendum_diagnosis"],
        )

        self.assertEqual(
            "Final Diagnosis:\nPrimary finding\n\n"
            "Addendum Diagnosis:\nAdditional finding",
            report,
        )

    def test_retains_empty_report_fields_as_labeled_sections(self):
        report = compile_report_fields(
            {"final_diagnosis": "Primary finding", "addendum_diagnosis": ""},
            ["final_diagnosis", "addendum_diagnosis"],
        )

        self.assertEqual(
            "Final Diagnosis:\nPrimary finding\n\nAddendum Diagnosis:\n",
            report,
        )


if __name__ == "__main__":
    unittest.main()
