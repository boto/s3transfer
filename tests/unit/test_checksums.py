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
import random
from binascii import crc32

from s3transfer.checksums import combine_crc32


class TestCombineCrc32:
    def test_combine(self):
        for _ in range(100):
            data1 = random.randbytes(32)
            crc1 = crc32(data1)
            data2 = random.randbytes(32)
            crc2 = crc32(data2)
            serial = crc32(data1 + data2)
            combined = combine_crc32(crc1, crc2, len(data2))
            assert serial == combined

    def test_combine_no_update(self):
        data = random.randbytes(32)
        init = random.randint(1, 0x80000000)
        serial = crc32(data, init)
        combined = combine_crc32(init, crc32(data), len(data))
        assert serial == combined

    def test_combine_many_parts(self):
        parts = [f"Part{i}".encode() for i in range(1000)]

        serial_crc = crc32(b"".join(parts))
        combined_crc = crc32(parts[0])
        for i in range(1, len(parts)):
            part_crc = crc32(parts[i])
            combined_crc = combine_crc32(combined_crc, part_crc, len(parts[i]))

        assert combined_crc == serial_crc

    def test_combine_associative_property(self):
        data_a = b"foo"
        data_b = b"bar"
        data_c = b"baz"

        serial_crc = crc32(data_a + data_b + data_c)

        crc_a = crc32(data_a)
        crc_b = crc32(data_b)
        crc_c = crc32(data_c)

        # (a+b) + c
        crc_ab = combine_crc32(crc_a, crc_b, len(data_b))
        crc_ab_c = combine_crc32(crc_ab, crc_c, len(data_c))

        # a + (b+c)
        crc_bc = combine_crc32(crc_b, crc_c, len(data_c))
        crc_a_bc = combine_crc32(crc_a, crc_bc, len(data_b + data_c))

        assert serial_crc == crc_ab_c == crc_a_bc
