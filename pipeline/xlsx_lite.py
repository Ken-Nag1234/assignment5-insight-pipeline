"""Minimal .xlsx writer and reader using only the standard library (zipfile + XML).

Writer: text cells as inline strings (exact text, whitespace preserved), a frozen header row,
column widths, wrapped text, read-only-looking shading, and list data validations (dropdowns).
Reader: returns every sheet row as strings, handling shared strings, inline strings, numbers and
booleans as Excel, Numbers or LibreOffice save them.
"""

import re
import zipfile
from xml.etree import ElementTree as ET
from xml.sax.saxutils import escape

NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PKG_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"

STYLE_DEFAULT, STYLE_HEADER, STYLE_READONLY_WRAP, STYLE_INPUT, STYLE_WRAP = 0, 1, 2, 3, 4

STYLES = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="{NS}">
<fonts count="2"><font><sz val="11"/><name val="Calibri"/></font><font><b/><sz val="11"/><name val="Calibri"/></font></fonts>
<fills count="5"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill>
<fill><patternFill patternType="solid"><fgColor rgb="FFD9E1F2"/><bgColor indexed="64"/></patternFill></fill>
<fill><patternFill patternType="solid"><fgColor rgb="FFF2F2F2"/><bgColor indexed="64"/></patternFill></fill>
<fill><patternFill patternType="solid"><fgColor rgb="FFFFF9E6"/><bgColor indexed="64"/></patternFill></fill></fills>
<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>
<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>
<cellXfs count="5">
<xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>
<xf numFmtId="0" fontId="1" fillId="2" borderId="0" xfId="0" applyFont="1" applyFill="1" applyAlignment="1"><alignment wrapText="1" vertical="top"/></xf>
<xf numFmtId="49" fontId="0" fillId="3" borderId="0" xfId="0" applyNumberFormat="1" applyFill="1" applyAlignment="1"><alignment wrapText="1" vertical="top"/></xf>
<xf numFmtId="49" fontId="0" fillId="4" borderId="0" xfId="0" applyNumberFormat="1" applyFill="1" applyAlignment="1"><alignment wrapText="1" vertical="top"/></xf>
<xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0" applyAlignment="1"><alignment wrapText="1" vertical="top"/></xf>
</cellXfs>
<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>
</styleSheet>"""


def col_letter(index):
    """0 -> A, 25 -> Z, 26 -> AA."""
    letters = ""
    index += 1
    while index:
        index, rem = divmod(index - 1, 26)
        letters = chr(65 + rem) + letters
    return letters


def col_index(letters):
    n = 0
    for ch in letters:
        n = n * 26 + ord(ch) - 64
    return n - 1


def _cell(ref, value, style):
    if value is None or value == "":
        return f'<c r="{ref}" s="{style}"/>'
    return (f'<c r="{ref}" s="{style}" t="inlineStr"><is><t xml:space="preserve">{escape(str(value))}</t></is></c>')


def sheet_xml(rows, widths, styles=None, header_style=STYLE_HEADER, validations=(), freeze_header=True):
    """rows: list of lists of strings (first row = header). styles: per-column body style ids."""
    styles = styles or [STYLE_WRAP] * len(rows[0])
    out = [f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?><worksheet xmlns="{NS}" xmlns:r="{REL_NS}">']
    if freeze_header:
        out.append('<sheetViews><sheetView workbookViewId="0"><pane ySplit="1" topLeftCell="A2" activePane="bottomLeft"'
                   ' state="frozen"/></sheetView></sheetViews>')
    out.append("<cols>" + "".join(f'<col min="{i + 1}" max="{i + 1}" width="{w}" customWidth="1"/>'
                                   for i, w in enumerate(widths)) + "</cols><sheetData>")
    for r, row in enumerate(rows, 1):
        cells = "".join(_cell(f"{col_letter(c)}{r}", v, header_style if r == 1 else styles[c]) for c, v in enumerate(row))
        out.append(f'<row r="{r}">{cells}</row>')
    out.append("</sheetData>")
    if validations:
        out.append(f'<dataValidations count="{len(validations)}">')
        for sqref, values, prompt in validations:
            formula = escape('"' + ",".join(values) + '"')
            out.append(f'<dataValidation type="list" allowBlank="1" showInputMessage="1" showErrorMessage="1"'
                       f' errorTitle="Invalid value" error="Choose a value from the list."'
                       f' promptTitle="Allowed values" prompt="{escape(prompt)}" sqref="{sqref}">'
                       f"<formula1>{formula}</formula1></dataValidation>")
        out.append("</dataValidations>")
    out.append("</worksheet>")
    return "".join(out)


def write_workbook(path, sheets):
    """sheets: list of (name, sheet_xml). Fails if path exists."""
    names = [name for name, _ in sheets]
    content_types = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        '<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>'
        + "".join(f'<Override PartName="/xl/worksheets/sheet{i}.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
                  for i in range(1, len(sheets) + 1)) + "</Types>")
    root_rels = (f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="{PKG_REL_NS}">'
                 '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument"'
                 ' Target="xl/workbook.xml"/></Relationships>')
    workbook = (f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?><workbook xmlns="{NS}" xmlns:r="{REL_NS}"><sheets>'
                + "".join(f'<sheet name="{escape(n)}" sheetId="{i}" r:id="rId{i}"/>' for i, n in enumerate(names, 1))
                + "</sheets></workbook>")
    wb_rels = (f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="{PKG_REL_NS}">'
               + "".join(f'<Relationship Id="rId{i}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet"'
                         f' Target="worksheets/sheet{i}.xml"/>' for i in range(1, len(sheets) + 1))
               + f'<Relationship Id="rId{len(sheets) + 1}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles"'
                 ' Target="styles.xml"/></Relationships>')
    with zipfile.ZipFile(path, "x", zipfile.ZIP_DEFLATED) as z:
        z.writestr("[Content_Types].xml", content_types)
        z.writestr("_rels/.rels", root_rels)
        z.writestr("xl/workbook.xml", workbook)
        z.writestr("xl/_rels/workbook.xml.rels", wb_rels)
        z.writestr("xl/styles.xml", STYLES)
        for i, (_, xml) in enumerate(sheets, 1):
            z.writestr(f"xl/worksheets/sheet{i}.xml", xml)


def _text(element):
    """Concatenate all <t> text inside a string item (handles rich-text runs)."""
    return "".join(t.text or "" for t in element.iter(f"{{{NS}}}t"))


def _number_string(value):
    number = float(value)
    return str(int(number)) if number.is_integer() else repr(number)


def read_sheet(path, sheet_name):
    """Return rows (lists of strings) of the named sheet, padded to the header width."""
    with zipfile.ZipFile(path) as z:
        workbook = ET.fromstring(z.read("xl/workbook.xml"))
        rels = ET.fromstring(z.read("xl/_rels/workbook.xml.rels"))
        targets = {r.get("Id"): r.get("Target") for r in rels}
        target = None
        for sheet in workbook.iter(f"{{{NS}}}sheet"):
            if sheet.get("name") == sheet_name:
                target = targets[sheet.get(f"{{{REL_NS}}}id")]
        if target is None:
            raise ValueError(f"Sheet {sheet_name!r} not found")
        target = target.lstrip("/")
        target = target if target.startswith("xl/") else "xl/" + target
        shared = []
        if "xl/sharedStrings.xml" in z.namelist():
            shared = [_text(si) for si in ET.fromstring(z.read("xl/sharedStrings.xml")).iter(f"{{{NS}}}si")]
        root = ET.fromstring(z.read(target))
    rows = []
    for row in root.iter(f"{{{NS}}}row"):
        values = {}
        for c in row.iter(f"{{{NS}}}c"):
            col = col_index(re.match(r"[A-Z]+", c.get("r")).group(0))
            kind = c.get("t")
            v = c.find(f"{{{NS}}}v")
            if kind == "inlineStr":
                value = _text(c.find(f"{{{NS}}}is"))
            elif kind == "s":
                value = shared[int(v.text)]
            elif kind == "b":
                value = "true" if v is not None and v.text == "1" else "false"
            elif kind in ("str", "e"):
                value = v.text if v is not None else ""
            else:
                value = _number_string(v.text) if v is not None and v.text else ""
            values[col] = value
        rows.append([values.get(i, "") for i in range(max(values, default=-1) + 1)])
    width = len(rows[0]) if rows else 0
    return [r + [""] * (width - len(r)) for r in rows]
