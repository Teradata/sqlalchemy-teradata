def parse_show_table_col_attrs(table_str, col_names):
    """
    Parses the string returned by the SHOW TABLE query to extract the attributes
    of a selection of columns from the associated table.

    Args:
        table_str: A table string returned by a SHOW TABLE query.
        col_names: A list of column names (string) to extract the attributes of.

    Returns:
        A dictionary keyed by the column names of the table with the
        corresponding values being the string of attributes specified by that
        column.
    """
    parsed = [line.strip(', ') for line in table_str.split('\r')
        if line.strip(' ').startswith(col_names)]
    return {col[:col.index(' ')]: col[col.index(' ')+1:] for col in parsed}
