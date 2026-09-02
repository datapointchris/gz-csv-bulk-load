"""The header check is the only gate between a vendor change and the warehouse.

Everything here runs against the pure half — inflating a header, splitting it, and
describing a difference. The S3 half is three boto3 calls with no logic in them.
"""

import gzip

import pytest

import check_headers
import schemas

DIALECT = {
    'encoding': schemas.CSV_OPTIONS['encoding'],
    'delimiter': schemas.CSV_OPTIONS['sep'],
    'quote': schemas.CSV_OPTIONS['quote'],
}


def gzipped(text: str) -> bytes:
    return gzip.compress(text.encode())


def test_first_line_reads_a_header_without_the_rest_of_the_file():
    head = gzipped('a,b,c\n1,2,3\n4,5,6\n')
    assert check_headers.first_line(head) == b'a,b,c'


def test_first_line_strips_a_carriage_return():
    """A vendor writing CRLF would otherwise leave \\r on the last column name."""
    assert check_headers.first_line(gzipped('a,b,c\r\n1,2,3\r\n')) == b'a,b,c'


def test_first_line_raises_when_no_newline_arrives():
    with pytest.raises(ValueError, match='HEADER_BYTES'):
        check_headers.first_line(gzipped('a,b,c'))


def test_parse_header_line_splits_on_the_delimiter():
    assert check_headers.parse_header_line(b'a,b,c', **DIALECT) == ('a', 'b', 'c')


def test_parse_header_line_strips_quotes_and_spaces():
    assert check_headers.parse_header_line(b'"a", "b" ,c', **DIALECT) == ('a', 'b', 'c')


def test_parse_header_line_strips_a_byte_order_mark():
    """A BOM decodes into the first name and would read as a rename, not as an encoding fault."""
    assert check_headers.parse_header_line('﻿a,b,c'.encode(), **DIALECT) == ('a', 'b', 'c')


def test_matching_header_has_no_mismatch():
    assert check_headers.describe_mismatch(('a', 'b'), ('a', 'b')) is None


def test_missing_column_is_named():
    message = check_headers.describe_mismatch(('a', 'b', 'c'), ('a', 'c'))
    assert 'missing: b' in message


def test_extra_column_is_named():
    message = check_headers.describe_mismatch(('a', 'b'), ('a', 'b', 'z'))
    assert 'extra:   z' in message


def test_missing_and_extra_are_reported_together():
    """One pass should show everything wrong with a file, not the first thing."""
    message = check_headers.describe_mismatch(('a', 'b'), ('a', 'z'))
    assert 'missing: b' in message
    assert 'extra:   z' in message


def test_reordering_names_the_first_bad_position():
    message = check_headers.describe_mismatch(('a', 'b', 'c'), ('a', 'c', 'b'))
    assert "column 2 is 'c'" in message
    assert "contract says 'b'" in message


def test_sizing_note_reports_idle_cores():
    note = check_headers.sizing_note('orders', file_count=4, worker_type='G.2X', workers=3)
    assert '4 files' in note
    assert '16 cores' in note
    assert '12 will idle' in note


def test_sizing_note_reports_queueing():
    note = check_headers.sizing_note('orders', file_count=40, worker_type='G.1X', workers=3)
    assert 'files will queue' in note


def test_sizing_note_survives_an_unknown_worker_type():
    assert 'unknown' in check_headers.sizing_note('orders', 4, 'G.99X', 3)
