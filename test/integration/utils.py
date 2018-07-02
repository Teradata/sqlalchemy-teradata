def parse_types_show_table(table_str, col_names):
    parsed = [line.strip(', ').split(' ') for line in table_str.split('\r')
        if line.strip(' ').startswith(col_names)]
    return {col[0]: col[1] for col in parsed}
