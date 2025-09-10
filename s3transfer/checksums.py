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
"""
NOTE: All classes and functions in this module are considered private and are
subject to abrupt breaking changes. Please do not use them directly.
"""


def combine_crc32(crc1, crc2, len2):
    """Combine two CRC32 values.

    :type crc1: int
    :param crc1: Current CRC32 integer value.

    :type crc2: int
    :param crc2: Second CRC32 integer value to combine.

    :type len2: int
    :param len2: Length of data that produced `crc2`.

    :rtype: int
    :returns: Combined CRC32 integer value.
    """

    POLY = 0xEDB88320

    def gf2_matrix_multiply(mat, vec):
        """Multiply a matrix by a vector in GF(2)"""
        result = 0
        for i in range(32):
            if vec & 1:
                result ^= mat[i]
            vec >>= 1
        return result

    def gf2_matrix_square(mat):
        """Square a matrix in GF(2)"""
        result = [0] * 32
        for i in range(32):
            result[i] = gf2_matrix_multiply(mat, mat[i])
        return result

    def crc32_matrix_power(length):
        """Generate transformation matrix for CRC32 over given length"""
        # Base transformation matrix (multiply by x)
        mat = [0] * 32
        mat[0] = POLY
        for i in range(1, 32):
            mat[i] = 1 << (i - 1)

        # Compute mat^length using binary exponentiation
        result = [0] * 32
        for i in range(32):
            result[i] = 1 << i

        n = length
        while n > 0:
            if n & 1:
                result = [
                    gf2_matrix_multiply(mat, result[i]) for i in range(32)
                ]
            mat = gf2_matrix_square(mat)
            n >>= 1

        return result

    if len2 == 0:
        return crc1

    transform_matrix = crc32_matrix_power(len2 * 8)
    transformed_crc1 = gf2_matrix_multiply(transform_matrix, crc1)
    combined = transformed_crc1 ^ crc2

    return combined & 0xFFFFFFFF


_CRC_CHECKSUM_TO_COMBINE_FUNCTION = {
    "ChecksumCRC32": combine_crc32,
}
