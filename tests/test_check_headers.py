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


def test_parse_header_line_keeps_a_quoted_delimiter_in_one_column():
    assert check_headers.parse_header_line(b'"a,x",b', **DIALECT) == ('a,x', 'b')


def test_parse_header_line_strips_a_byte_order_mark():
    """A BOM decodes into the first name and would read as a rename, not as an encoding fault."""
    assert check_headers.parse_header_line('﻿a,b,c'.encode(), **DIALECT) == ('a', 'b', 'c')


def test_matching_header_has_no_mismatch():
    assert check_headers.header_mismatch(('a', 'b'), ('a', 'b')) is None


def test_missing_column_is_named():
    assert check_headers.header_mismatch(('a', 'b', 'c'), ('a', 'c')) == check_headers.HeaderMismatch(missing=('b',))


def test_extra_column_is_named():
    assert check_headers.header_mismatch(('a', 'b'), ('a', 'b', 'z')) == check_headers.HeaderMismatch(extra=('z',))


def test_missing_and_extra_are_reported_together():
    """One pass should show everything wrong with a file, not the first thing."""
    mismatch = check_headers.header_mismatch(('a', 'b'), ('a', 'z'))
    assert mismatch == check_headers.HeaderMismatch(missing=('b',), extra=('z',))


def test_duplicated_column_is_named():
    assert check_headers.header_mismatch(('a', 'b'), ('a', 'b', 'b')) == check_headers.HeaderMismatch(duplicated=('b',))


def test_reordering_names_the_first_bad_position():
    mismatch = check_headers.header_mismatch(('a', 'b', 'c'), ('a', 'c', 'b'))
    assert mismatch == check_headers.HeaderMismatch(reordered=check_headers.Reorder(position=2, found='c', expected='b'))


def test_sizing_counts_idle_cores():
    sizing = check_headers.cluster_sizing(file_count=4, worker_type='G.2X', workers=3)
    assert (sizing.cores, sizing.idle, sizing.queued) == (16, 12, 0)


def test_sizing_counts_queued_files():
    sizing = check_headers.cluster_sizing(file_count=40, worker_type='G.1X', workers=3)
    assert (sizing.cores, sizing.idle, sizing.queued) == (8, 0, 32)


def test_sizing_survives_an_unknown_worker_type():
    sizing = check_headers.cluster_sizing(file_count=4, worker_type='G.99X', workers=3)
    assert (sizing.cores, sizing.idle, sizing.queued) == (0, 0, 0)
