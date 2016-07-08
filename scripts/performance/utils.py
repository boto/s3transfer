KB = 1024
SIZE_SUFFIX = {
    'kb': 1024,
    'mb': 1024 ** 2,
    'gb': 1024 ** 3,
    'tb': 1024 ** 4,
    'kib': 1024,
    'mib': 1024 ** 2,
    'gib': 1024 ** 3,
    'tib': 1024 ** 4,
}


def human_readable_to_bytes(value):
    """Converts a human readable size to bytes.
    :param value: A string such as "10MB".  If a suffix is not included,
        then the value is assumed to be an integer representing the size
        in bytes.
    :returns: The converted value in bytes as an integer
    """
    value = value.lower()
    if value[-2:] == 'ib':
        # Assume IEC suffix.
        suffix = value[-3:].lower()
    else:
        suffix = value[-2:].lower()
    has_size_identifier = (
        len(value) >= 2 and suffix in SIZE_SUFFIX)
    if not has_size_identifier:
        try:
            return int(value)
        except ValueError:
            raise ValueError("Invalid size value: %s" % value)
    else:
        multiplier = SIZE_SUFFIX[suffix]
        return int(value[:-len(suffix)]) * multiplier


def create_file(filename, file_size):
    with open(filename, 'wb') as f:
        for i in range(0, file_size, KB):
            f.write(b'a' * i)
