# Copyright 2026 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import pytest
from botocore.compat import HAS_CRT

from s3transfer.checksums import (
    FullObjectChecksum,
    FullObjectChecksumCombiner,
    combine_crc32,
    create_checksum_for_algorithm,
    is_full_object_checksum_supported,
    resolve_full_object_checksum,
)
from s3transfer.exceptions import S3DownloadChecksumError

CRT_ONLY_ALGORITHMS = ['crc32c', 'crc64nvme']

requires_crt = pytest.mark.skipif(
    not HAS_CRT, reason='awscrt is not installed'
)


def _compute_expected_b64(data, algorithm):
    checksum = create_checksum_for_algorithm(algorithm)
    checksum.update(data)
    return checksum.b64digest()


def _register_parts(combiner, parts, algorithm):
    for i, part_data in enumerate(parts):
        checksum = create_checksum_for_algorithm(algorithm)
        checksum.update(part_data)
        combiner.register_part(i, checksum, len(part_data))


class TestCombineCrc32:
    def test_combine(self):
        rand = random.Random(0)
        for _ in range(100):
            data1 = rand.randbytes(32)
            crc1 = crc32(data1)
            data2 = rand.randbytes(32)
            crc2 = crc32(data2)
            serial = crc32(data1 + data2)
            combined = combine_crc32(crc1, crc2, len(data2))
            assert serial == combined

    def test_combine_with_initial_crc(self):
        rand = random.Random(0)
        data = rand.randbytes(32)
        init = rand.randint(1, 0x80000000)
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

    @requires_crt
    def test_matches_crt_implementation(self):
        from awscrt import checksums as crt_checksums

        rand = random.Random(0)
        for _ in range(100):
            crc1 = rand.getrandbits(32)
            crc2 = rand.getrandbits(32)
            len2 = rand.randint(1, 16 * 1024 * 1024)
            assert combine_crc32(crc1, crc2, len2) == (
                crt_checksums.combine_crc32(crc1, crc2, len2)
            )

    @pytest.mark.parametrize(
        'len2',
        [1, 2, 3, 4, 7, 8, 15, 16, 31, 32, 63, 64, 127, 128, 255, 256, 257],
    )
    def test_combine_data_lengths(self, len2):
        rand = random.Random(len2)
        data1 = rand.randbytes(50)
        data2 = rand.randbytes(len2)
        combined = combine_crc32(crc32(data1), crc32(data2), len2)
        assert combined == crc32(data1 + data2)

    def test_combine_part_sized_length(self):
        # Validate combining an 8 MiB second part, calculated incrementally
        # from 128 repeated 64 KiB chunks to avoid allocating the full part.
        chunk = bytes(range(256)) * 256
        num_chunks = 128
        len2 = len(chunk) * num_chunks

        data1 = b'first part'
        crc2 = 0
        serial = crc32(data1)
        for _ in range(num_chunks):
            crc2 = crc32(chunk, crc2)
            serial = crc32(chunk, serial)

        assert combine_crc32(crc32(data1), crc2, len2) == serial

    @pytest.mark.parametrize('len2', [0, -1])
    def test_combine_no_second_part(self, len2):
        # A non-positive length is a no-op that returns the first CRC.
        assert combine_crc32(0xDEADBEEF, 0x12345678, len2) == 0xDEADBEEF


class TestIsFullObjectChecksumSupported:
    def test_crc32_is_always_supported(self):
        assert is_full_object_checksum_supported('crc32')

    @pytest.mark.parametrize('algorithm', CRT_ONLY_ALGORITHMS)
    def test_crt_only_algorithms(self, algorithm):
        assert is_full_object_checksum_supported(algorithm) is HAS_CRT

    def test_unsupported_algorithm(self):
        assert not is_full_object_checksum_supported('sha256')


class TestResolveFullObjectChecksum:
    def test_full_object_crc32(self):
        response = {
            'ChecksumType': 'FULL_OBJECT',
            'ChecksumCRC32': 'abc123==',
        }
        result = resolve_full_object_checksum(response)
        assert result == FullObjectChecksum(
            algorithm='crc32', expected_b64='abc123=='
        )

    def test_full_object_crc32c(self):
        response = {
            'ChecksumType': 'FULL_OBJECT',
            'ChecksumCRC32C': 'xyz789==',
        }
        result = resolve_full_object_checksum(response)
        assert result == FullObjectChecksum(
            algorithm='crc32c', expected_b64='xyz789=='
        )

    def test_full_object_crc64nvme(self):
        response = {
            'ChecksumType': 'FULL_OBJECT',
            'ChecksumCRC64NVME': 'nvme64==',
        }
        result = resolve_full_object_checksum(response)
        assert result == FullObjectChecksum(
            algorithm='crc64nvme', expected_b64='nvme64=='
        )

    def test_missing_checksum_type(self):
        assert resolve_full_object_checksum({}) is None

    def test_composite_checksum(self):
        response = {
            'ChecksumType': 'COMPOSITE',
            'ChecksumCRC32': 'abc123==',
        }
        assert resolve_full_object_checksum(response) is None

    def test_full_object_sha_only(self):
        response = {
            'ChecksumType': 'FULL_OBJECT',
            'ChecksumSHA256': 'sha256value==',
        }
        assert resolve_full_object_checksum(response) is None

    def test_case_insensitive_checksum_type(self):
        response = {
            'ChecksumType': 'full_object',
            'ChecksumCRC32': 'abc123==',
        }
        result = resolve_full_object_checksum(response)
        assert result is not None
        assert result.algorithm == 'crc32'


class TestCreateChecksumForAlgorithm:
    def test_crc32(self):
        checksum = create_checksum_for_algorithm('crc32')
        assert checksum is not None
        assert hasattr(checksum, 'update')
        assert hasattr(checksum, 'digest')

    @requires_crt
    @pytest.mark.parametrize('algorithm', CRT_ONLY_ALGORITHMS)
    def test_crt_only_algorithm(self, algorithm):
        checksum = create_checksum_for_algorithm(algorithm)
        assert checksum is not None
        assert hasattr(checksum, 'update')
        assert hasattr(checksum, 'digest')

    def test_unknown_algorithm(self):
        assert create_checksum_for_algorithm('unknown') is None


class TestFullObjectChecksumCombiner:
    def test_combine_and_validate_crc32(self):
        data = b'hello world, this is a test of CRC combining'
        parts = [data[:15], data[15:30], data[30:]]
        expected = _compute_expected_b64(data, 'crc32')

        combiner = FullObjectChecksumCombiner(
            'crc32', len(parts), expected_b64=expected
        )
        _register_parts(combiner, parts, 'crc32')
        combiner.combine_and_validate()

    @requires_crt
    def test_combine_and_validate_crc32c(self):
        data = b'testing crc32c combining across parts'
        parts = [data[:10], data[10:]]
        expected = _compute_expected_b64(data, 'crc32c')

        combiner = FullObjectChecksumCombiner(
            'crc32c', len(parts), expected_b64=expected
        )
        _register_parts(combiner, parts, 'crc32c')
        combiner.combine_and_validate()

    @requires_crt
    def test_combine_and_validate_crc64nvme(self):
        data = b'testing crc64nvme combining across parts'
        parts = [data[:10], data[10:20], data[20:]]
        expected = _compute_expected_b64(data, 'crc64nvme')

        combiner = FullObjectChecksumCombiner(
            'crc64nvme', len(parts), expected_b64=expected
        )
        _register_parts(combiner, parts, 'crc64nvme')
        combiner.combine_and_validate()

    def test_checksum_mismatch_raises(self):
        combiner = FullObjectChecksumCombiner(
            'crc32', 1, expected_b64='AAAABB=='
        )
        checksum = create_checksum_for_algorithm('crc32')
        checksum.update(b'some data')
        combiner.register_part(0, checksum, 9)

        with pytest.raises(S3DownloadChecksumError, match='did not match'):
            combiner.combine_and_validate()

    def test_combined_b64_without_expected(self):
        data = b'upload use case'
        parts = [data[:5], data[5:]]
        expected = _compute_expected_b64(data, 'crc32')

        combiner = FullObjectChecksumCombiner('crc32', len(parts))
        _register_parts(combiner, parts, 'crc32')
        assert combiner.combined_b64 == expected

    def test_combined_bytes_are_cached(self):
        combiner = FullObjectChecksumCombiner('crc32', 1)
        checksum = create_checksum_for_algorithm('crc32')
        checksum.update(b'cache test')
        combiner.register_part(0, checksum, 10)

        assert combiner.combined_b64 == combiner.combined_b64

    def test_chunked_update_matches_single_update(self):
        data = b'streaming chunk test data for verification'
        expected = _compute_expected_b64(data, 'crc32')

        combiner = FullObjectChecksumCombiner(
            'crc32', 1, expected_b64=expected
        )
        checksum = create_checksum_for_algorithm('crc32')
        length = 0
        for i in range(0, len(data), 5):
            chunk = data[i : i + 5]
            checksum.update(chunk)
            length += len(chunk)
        combiner.register_part(0, checksum, length)
        combiner.combine_and_validate()
