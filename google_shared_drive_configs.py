configs = {
    'all_folders': [
        'folder_1',
        'folder_2',
        'folder_3'
    ],

    **dict.fromkeys(['folder_1', 'folder_2'], {
        'date_cols': ['col_date'],
        'dtypes': {
            'column_1': int,
            'column_2': str,
            'column_3': str,
            'column_4': float,
            'column_5': float,
            'column_6': str,
            'column_7': float,
        },
        'skip_rows': 2,
        'encoding': 'CP1250'
    }),

    'folder_3': {
        'date_cols': ['date_col'],
        'dtypes': {
            'column_1': str,
            'column_2': int,
            'column_3': str,
            'column_4': str,
            'column_5': float,
            'column_6': str,
            'column_7': float,
        }
    },

}
