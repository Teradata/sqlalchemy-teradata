import shlex

def parse_show_table_col_attrs(table_str, col_names):
    """Parses the string returned by the SHOW TABLE query to extract the
    attributes of a selection of columns from the associated table.

    Args:
        table_str (str): A table string returned by a SHOW TABLE query.
        col_names (tuple(str)): A tuple of strings (column names) specifying
            which column(s) to parse attributes for. Note these column
            names must be exactly as they appear in SHOW TABLE (same quoting
            and case-sensitive).

    Returns:
        A dictionary keyed by the column names of the table with the
        corresponding values being the string of attributes specified
        by that column.
    """

    parsed = [shlex.split(line.strip(', '))
              for line in table_str.split('\r')
              if line.strip(' ').startswith(col_names)]
    return {col[0]: ' '.join(col[1:]) for col in parsed}
