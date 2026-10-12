"""Offline tests for the golden workbook and Excel CSV helpers. Synthetic fixtures only."""

import csv
import tempfile
import unittest
import zipfile
from pathlib import Path

from pipeline import xlsx_lite
from pipeline.common import FIELDS
from pipeline.golden import (COLUMNS, LABEL_FIELDS, create_excel_csv, create_template, create_workbook,
                             import_workbook, validate)

TEXTS = ["Can't log in since the update.", "Great app 🎧 ünïcode", "Line one\nline two  with  spaces"]


class GoldenWorkbookTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.source = self.dir / "golden_source.csv"
        with self.source.open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow(list(FIELDS) + LABEL_FIELDS)
            for i, text in enumerate(TEXTS, 1):
                w.writerow([f"g{i}", text, "3", "0", "", "2023-01-01 00:00:00"] + [""] * 7)
        self.working = self.dir / "labels.csv"
        create_template(self.source, self.working)

    def tearDown(self):
        self.tmp.cleanup()

    def test_workbook_round_trip_is_exact_and_blank(self):
        book = self.dir / "labels.xlsx"
        self.assertEqual(create_workbook(self.working, book), 3)
        rows = xlsx_lite.read_sheet(book, "Labels")
        self.assertEqual(rows[0], COLUMNS)
        with self.working.open(encoding="utf-8", newline="") as f:
            original = list(csv.reader(f))
        self.assertEqual(rows, original)
        out = self.dir / "completed.csv"
        import_workbook(book, out)
        report = validate(self.source, out)
        self.assertEqual((report["unlabeled"], report["problems"]), (3, {}))

    def test_workbook_has_dropdowns_and_definitions(self):
        book = self.dir / "labels.xlsx"
        create_workbook(self.working, book)
        with zipfile.ZipFile(book) as z:
            sheet = z.read("xl/worksheets/sheet1.xml").decode()
            self.assertEqual(sheet.count("<dataValidation "), 6)
            self.assertIn('"access,usability,playback,downloads,catalog,billing,support,other"', sheet)
            self.assertIn('state="frozen"', sheet)
        self.assertEqual(xlsx_lite.read_sheet(book, "Definitions")[0], ["field", "value", "meaning"])

    def test_never_overwrites(self):
        book = self.dir / "labels.xlsx"
        create_workbook(self.working, book)
        with self.assertRaises(SystemExit):
            create_workbook(self.working, book)
        out = self.dir / "excel.csv"
        create_excel_csv(self.working, out)
        with self.assertRaises(SystemExit):
            create_excel_csv(self.working, out)
        done = self.dir / "completed.csv"
        import_workbook(book, done)
        with self.assertRaises(SystemExit):
            import_workbook(book, done)
        import_workbook(book, done, overwrite=True)

    def test_excel_csv_has_bom_and_same_content(self):
        out = self.dir / "excel.csv"
        create_excel_csv(self.working, out)
        data = out.read_bytes()
        self.assertTrue(data.startswith(b"\xef\xbb\xbf"))
        self.assertEqual(data[3:], self.working.read_bytes())
        self.assertEqual(validate(self.source, out)["rows"], 3)

    def test_reads_excel_style_cells_after_labeling(self):
        """Simulate how Excel saves: shared strings, numeric cells and boolean cells."""
        book = self.dir / "excel_saved.xlsx"
        create_workbook(self.working, book)
        with zipfile.ZipFile(book) as z:
            parts = {n: z.read(n) for n in z.namelist()}
        ns = xlsx_lite.NS
        shared = [COLUMNS[0], "access", "complaint", "Can't log in since the update.", "[]"]
        parts["xl/sharedStrings.xml"] = (f'<sst xmlns="{ns}">' + "".join(f"<si><t>{s}</t></si>" for s in shared)
                                         + "</sst>").encode()
        sheet = parts["xl/worksheets/sheet1.xml"].decode()
        filled = ('<c r="D2" t="s"><v>1</v></c><c r="E2" t="s"><v>2</v></c><c r="F2"><v>-0.5</v></c>'
                  '<c r="G2"><v>4</v></c><c r="H2" t="s"><v>4</v></c><c r="I2" t="s"><v>3</v></c>'
                  '<c r="J2" t="b"><v>0</v></c>')
        start = sheet.index('<c r="D2"')
        end = sheet.index('<c r="K2"')
        sheet = sheet[:start] + filled + sheet[end:]
        parts["xl/worksheets/sheet1.xml"] = sheet.encode()
        rebuilt = self.dir / "rebuilt.xlsx"
        with zipfile.ZipFile(rebuilt, "w") as z:
            for name, data in parts.items():
                z.writestr(name, data)
        out = self.dir / "completed.csv"
        import_workbook(rebuilt, out)
        report = validate(self.source, out)
        self.assertEqual((report["labeled"], report["valid"], report["unlabeled"]), (1, 1, 2), report["problems"])
        with out.open(encoding="utf-8", newline="") as f:
            row = next(csv.DictReader(f))
        self.assertEqual((row["sentiment"], row["severity"], row["needs_review"]), ("-0.5", "4", "false"))


if __name__ == "__main__":
    unittest.main()
