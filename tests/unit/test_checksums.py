# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
from binascii import crc32

import pytest

from s3transfer.checksums import combine_crc32


class TestCombineCrc32:
    def _calculate_crc32_serial(self, data):
        return crc32(data) & 0xFFFFFFFF

    def _calculate_crc32_combined(self, data1, data2):
        crc1 = crc32(data1)
        crc2 = crc32(data2)
        return combine_crc32(crc1, crc2, len(data2))

    @pytest.mark.parametrize(
        "data1,data2",
        [
            (b"foo", b"bar"),
            (b"", b""),
            (b"", b"foobar"),
            (b"foobar", b""),
            (b"foobar", b"foobar"),
            (b"foo" * 10000, b"bar" * 10000),
            (b"\x00", b"\x00"),
        ],
    )
    def test_combine_crc32(self, data1, data2):
        serial_crc = self._calculate_crc32_serial(data1 + data2)
        combined_crc = self._calculate_crc32_combined(data1, data2)
        assert combined_crc == serial_crc

    def test_combine_crc32_many_parts(self):
        parts = [f"Part{i}".encode() for i in range(1000)]

        serial_crc = self._calculate_crc32_serial(b"".join(parts))
        combined_crc = self._calculate_crc32_serial(parts[0])
        for i in range(1, len(parts)):
            part_crc = self._calculate_crc32_serial(parts[i])
            combined_crc = combine_crc32(combined_crc, part_crc, len(parts[i]))

        assert combined_crc == serial_crc

    def test_combine_crc32_associative_property(self):
        data_a = b"foo"
        data_b = b"bar"
        data_c = b"baz"

        serial_crc = self._calculate_crc32_serial(data_a + data_b + data_c)

        crc_a = self._calculate_crc32_serial(data_a)
        crc_b = self._calculate_crc32_serial(data_b)
        crc_c = self._calculate_crc32_serial(data_c)

        # (a+b) + c
        crc_ab = combine_crc32(crc_a, crc_b, len(data_b))
        crc_ab_c = combine_crc32(crc_ab, crc_c, len(data_c))

        # a + (b+c)
        crc_bc = combine_crc32(crc_b, crc_c, len(data_c))
        crc_a_bc = combine_crc32(crc_a, crc_bc, len(data_b + data_c))

        assert serial_crc == crc_ab_c == crc_a_bc
