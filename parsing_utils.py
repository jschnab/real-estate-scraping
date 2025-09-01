def string_to_float(string):
    """
    Convert a string representing a number to a float.

    :param str string: string to convert
    :return float:
    """
    remap = {
        ord(","): None,
        ord("$"): None,
        ord("\xa0"): None,
    }
    clean = string.translate(remap)
    try:
        return float(clean)
    except ValueError:
        return float("nan")
