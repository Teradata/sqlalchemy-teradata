def parse_show_table_col_attrs(table_str, col_names):
    parsed = [line.strip(', ') for line in table_str.split('\r')
        if line.strip(' ').startswith(col_names)]
    return {col[:col.index(' ')]: col[col.index(' ')+1:] for col in parsed}
