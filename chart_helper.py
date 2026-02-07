"""
Chart Helper - Bridge between DataAccess and Animation Scripts
===============================================================
Convenience functions for creating charts using the data API.
"""

import sys
sys.path.insert(0, '/Users/paul/Documents/DATA/tools/animation_new')

import pandas as pd
from bokeh.models import ColumnDataSource, DataRange1d, Range1d
from datetime import timedelta
import sqlite_data_access as sda


def get_series(columns, freq='m', start_date=None, end_date=None, country=None):
    """
    Get time series data as a DataFrame (compatible with old DuckDB interface).
    Returns DataFrame with Date index and requested columns.
    """
    if isinstance(columns, str):
        columns = [columns]

    dfs = []
    for col in columns:
        data = sda.get_series_data(col, freq=freq, start=start_date, end=end_date)
        if data is not None and data['data']:
            series_df = pd.DataFrame(data['data'])
            series_df = series_df.rename(columns={'value': col})
            series_df['Date'] = pd.to_datetime(series_df['Date'])
            series_df = series_df.set_index('Date')
            dfs.append(series_df)

    if not dfs:
        return pd.DataFrame()

    return pd.concat(dfs, axis=1)


def search_columns(pattern, freq='m', country=None, limit=50):
    """
    Search for columns matching a pattern.
    Returns list of column name strings (compatible with old DuckDB interface).
    """
    results = sda.search_series(pattern, freq=freq, country=country, limit=limit)
    return [r['name'] for r in results]


def get_chart_source(columns: list, freq: str = 'm', start_date: str = None) -> ColumnDataSource:
    """
    Get a ColumnDataSource ready for Bokeh charts.

    Parameters:
    -----------
    columns : list
        List of column names to include
    freq : str
        Frequency: 'm', 'q', 'a'
    start_date : str
        Start date filter (YYYY-MM-DD)

    Returns:
    --------
    ColumnDataSource with 'Date' and requested columns
    """
    df = get_series(columns, freq=freq, start_date=start_date)

    if df.empty:
        raise ValueError(f"No data found for columns: {columns}")

    # Reset index to make Date a column
    df = df.reset_index()

    return ColumnDataSource(df)


def create_line_animation(
    title: str,
    columns: list,
    labels: list,
    freq: str = 'm',
    start_date: str = '2000-01-01',
    country: str = 'jp',
    colors: list = None,
    foot_label: str = 'EAE',
    legend_location: str = 'top_left',
    zero_span: float = None
):
    """
    Create an animated line chart.

    Parameters:
    -----------
    title : str
        Chart title
    columns : list
        Column names for each line
    labels : list
        Display labels for each line
    freq : str
        Data frequency
    start_date : str
        Start date
    country : str
        Country code for footer
    colors : list
        Color codes (c1, c2, etc.) - defaults to c1, c2, c3...
    foot_label : str
        Footer source label
    legend_location : str
        Legend position
    zero_span : float
        Y value for horizontal line (or None)
    """
    from animation_line import animated_line_chart

    # Default colors
    if colors is None:
        colors = [f'c{i+1}' for i in range(len(columns))]

    # Get data
    source = get_chart_source(columns, freq=freq, start_date=start_date)

    # Build lines list
    lines = [
        (col, label, source, color, 3, 'solid')
        for col, label, color in zip(columns, labels, colors)
    ]

    # Frequency code for chart name
    name = freq

    y_range = DataRange1d()
    right_y_range = DataRange1d()

    animated_line_chart(
        title=title,
        name=name,
        country=country,
        foot_label=foot_label,
        legend_location=legend_location,
        legend_vis=True,
        lines=lines,
        y_range=y_range,
        right_y_range=right_y_range,
        right_y_lines=[],
        exclude_labels=[],
        zero_span=zero_span
    )


def create_vbar_animation(
    title: str,
    columns: list,
    labels: list,
    freq: str = 'a',
    start_date: str = '1990-01-01',
    country: str = 'jp',
    colors: list = None,
    foot_label: str = 'EAE',
    legend_location: str = 'top_left',
    y_range: tuple = None
):
    """
    Create an animated stacked bar chart.

    Parameters:
    -----------
    title : str
        Chart title
    columns : list
        Column names to stack
    labels : list
        Display labels for each category
    freq : str
        Data frequency (usually 'a' for annual)
    start_date : str
        Start date
    country : str
        Country code for footer
    colors : list
        Color codes (c1, c2, etc.)
    foot_label : str
        Footer source label
    legend_location : str
        Legend position
    y_range : tuple
        (min, max) for y-axis, or None for auto
    """
    from animation_vbar import animated_vbar_stacker_chart

    # Default colors
    if colors is None:
        colors = [f'c{i+1}' for i in range(len(columns))]

    # Get data
    source = get_chart_source(columns, freq=freq, start_date=start_date)

    name = freq
    width = timedelta(weeks=42)

    if y_range is None:
        y_range_obj = DataRange1d()
    else:
        y_range_obj = y_range

    animated_vbar_stacker_chart(
        title=title,
        name=name,
        country=country,
        foot_label=foot_label,
        legend_location=legend_location,
        legend_vis=True,
        labels=labels,
        colors=colors,
        cats=columns,
        width=width,
        y_range=y_range_obj,
        right_y_range=y_range_obj,
        source1=source,
        source2=None,
        exclude_labels=None
    )


# Example usage
if __name__ == '__main__':
    # Search for columns
    print("Searching for JGB columns...")
    jgb_cols = search_columns('JGB, 10Y', freq='m')
    print(f"Found: {jgb_cols[:5]}")
