import json
import re
from pathlib import Path


IPYNB_PATH = Path("src/ndr/deployment/canonical_end_to_end_deployment_plan.ipynb")
MD_MIRROR_PATH = Path("src/ndr/deployment/canonical_end_to_end_deployment_plan.md")

REQUIRED_FIELDS = [
    "Behavior:",
    "Purpose:",
    "Required input:",
    "Expected output:",
    "Expected result:",
    "Usage instructions:",
    "Runnable example:",
]


def _load_ipynb_cells() -> list[dict]:
    payload = json.loads(IPYNB_PATH.read_text(encoding="utf-8"))
    return payload["cells"]


def _parse_md_cells(md_text: str) -> list[dict[str, str]]:
    pattern = re.compile(
        r"## Cell (?P<idx>\d+) \((?P<cell_type>markdown|code)\)\n\n"
        r"```(?P<lang>markdown|python)\n(?P<body>.*?)\n```",
        re.DOTALL,
    )
    cells = []
    for match in pattern.finditer(md_text):
        cell_type = match.group("cell_type")
        body = match.group("body")
        cells.append({"cell_type": cell_type, "source": body})
    return cells


def test_every_code_cell_has_immediately_preceding_markdown_with_required_sections():
    cells = _load_ipynb_cells()
    for idx, cell in enumerate(cells):
        if cell["cell_type"] != "code":
            continue
        assert idx > 0, "First notebook cell cannot be code for this contract"

        prev = cells[idx - 1]
        assert prev["cell_type"] == "markdown", f"Code cell {idx} is missing preceding markdown"

        prev_source = "".join(prev.get("source", []))
        for required in REQUIRED_FIELDS:
            assert required in prev_source, f"Code cell {idx} guide missing field: {required}"


def test_md_mirror_parity_matches_ipynb_cell_order_and_content():
    ipynb_cells = _load_ipynb_cells()
    mirror_text = MD_MIRROR_PATH.read_text(encoding="utf-8")
    md_cells = _parse_md_cells(mirror_text)

    assert len(md_cells) == len(ipynb_cells)

    for i, (ipynb_cell, md_cell) in enumerate(zip(ipynb_cells, md_cells), 1):
        expected_type = ipynb_cell["cell_type"]
        expected_source = "".join(ipynb_cell.get("source", [])).rstrip("\n")

        assert md_cell["cell_type"] == expected_type, f"Cell {i} type mismatch"
        assert md_cell["source"] == expected_source, f"Cell {i} source mismatch"


def test_md_mirror_documents_deterministic_sync_process():
    mirror_text = MD_MIRROR_PATH.read_text(encoding="utf-8")
    assert "Deterministic sync process:" in mirror_text
