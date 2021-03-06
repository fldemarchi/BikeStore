"""Helper functions for ipyb visualizations and explorations."""
import re
import subprocess
import pandas as pd
import numpy as np
from hashlib import md5
import seaborn as sns
import matplotlib.pyplot as plt
# for nice df prints that can be copy pasted to chat services
from tabulate import tabulate 
from typing import List
from IPython.display import display


def convert_to_snake_case(name: str): 
    """Convert string to snake_case."""
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def tabulated_df(df: pd.DataFrame):
    """Pretty print, ready-to-clipboard dataframes in ipynbs."""
    print(tabulate(df, headers='keys', tablefmt='psql'))


def list_to_sql_tuple(l: List)->str:
    """Create an sql-string synthax-valid tuple from python list."""
    assert len(l) > 0
    placeholders = ', '.join(str(element) for element in l)
    return f'({placeholders:s})'


def describe_table(table_name: str, db_connector)->pd.DataFrame:
    """Describe table sql template."""
    desc = db_connector.execute(f'describe {table_name}')
    return desc


def write_to_clipboard(output)->None:
    """Write str to clipboard using UTF-8 encoding."""
    process = subprocess.Popen(
        'pbcopy', env={'LANG': 'en_US.UTF-8'}, stdin=subprocess.PIPE)
    process.communicate(output.encode('utf-8'))


def list_vals_contains_str(in_list, val: str):
    """Filter string list containing certain string match in lowercase."""
    return [s for s in in_list if val.lower() in s.lower()]


def ab_split(id, salt, control_group_size: float):
    """Return True (for test) or False (for control).

    Logic is based on the ID string and salt.. The control_group_size number
    is between 0 and 1. This sets how big the control group is in perc.
    """
    test_id = str(id) + '-' + str(salt)
    test_id_digest = md5(test_id.encode('ascii')).hexdigest()
    test_id_first_digits = test_id_digest[:6]
    test_id_last_int = int(test_id_first_digits, 16)
    ab_split = (test_id_last_int/0xFFFFFF)
    return ab_split > control_group_size


def unique_value_counts(df, series, grp_cols=['CELLULAR_NUMBER']):
    """Count unique category levels."""
    rv = (df.groupby(grp_cols, as_index=False)[series]
          .first()[series].value_counts()
          )
    return rv


def col_sample_display(df: pd.DataFrame, col: str,
                       quantile: float=None, top_val: float=None):
    """Fast printing/visualization of sample data for given column.

    Also shows 10 unique specific values from the column and has
    modifiers for either showing a histogram for numeric data, or
    showing top_value counts for non-numeric columns.
    """
    unique_vals = df[col].unique()
    null_count = df[col].isnull().sum()
    null_pct = null_count/df.shape[0]
    print(f'\nCol is {col}\n')
    print(f'Null count is {null_count}, Null percentage is: {null_pct:.2%}')
    print(len(unique_vals), unique_vals[0:10],)
    display(df[col].describe())
    display(df[col].sample(10))

    # check either numerical or not
    if not np.issubdtype(df[col].dtype, np.number):
        display(df[col].value_counts().sort_values(ascending=False).head(10))
    else:
        query_str = f'{col}== {col}'
        if quantile is not None:
            top_perc = df[col].quantile(q=quantile)
            # this +100 is a safety net for when top_perc results
            # are equal to the lower limit of the filter.
            query_str = f'{col}>=0 and {col}<= {top_perc+100}'

        elif top_val is not None:
            query_str = f'{col}<= {top_val}'

        df.query(query_str)[col].hist(bins=60)
        plt.title(query_str)


def top_categorical_vs_kdeplot(
        df: pd.DataFrame,
        categorical_col: str,
        numerical_col: str,
        quantile: float=None,
        upper_bound_val: float = None,
        num_category_levels: int=2):
    """
    Plot multiple kdeplots for each category level.

    We would like to plot different kdeplots for a numerical column,
    yet generating a different plot for all rows corresponding to
    a specific category level (the possible values of the category set).
    """
    # Get enough colors for our test
    palette = sns.color_palette("husl", num_category_levels)
    top_values = (df[categorical_col]
                  .value_counts()
                  .sort_values(ascending=False)
                  .head(num_category_levels)
                  .index
                  )

    # Default query to filter data
    query_str = f'{numerical_col}=={numerical_col}'
    if quantile is not None:
        top_perc = df[numerical_col].quantile(q=quantile)
        # This +10 is a safety net for when top_perc results
        # Are equal to the lower limit of the filter.
        query_str = f'{numerical_col}>=0 and {numerical_col}<= {top_perc}'

    elif upper_bound_val is not None:
        query_str = f'{numerical_col}<= {upper_bound_val}'

    # Filter data
    view = df.query(query_str)
    i = 0
    # Create grouping condition on top category values
    gr_condition = (view[categorical_col]
                    .where(view[categorical_col].isin(top_values))
                    )

    # Group and plot for each category level
    iterator = view.groupby(gr_condition)[numerical_col]
    for name, grp in iterator:
        sns.kdeplot(grp, shade=True, alpha=.4,
                    label=f'{name}', color=palette[i])
        i = i+1
    title_str = f"Feature {numerical_col} distributions across "
    title_str += f"different {categorical_col}"
    plt.title(title_str, fontsize=15)
    plt.xlabel(f'{numerical_col} value')
    plt.ylabel('Probability')
    plt.show()


def top_categorical_vs_heatmap(
    df: pd.DataFrame,
    dependent_col: str,
    ind_col: str, quantile: float=None,
    top_val: float = None,
    with_log: bool=False
):
    """
    Plot heatmap from two variables which are categorical.

    Where one variable is categorical and in the index, and the other
    is categorical but is the dependent variable, as columns.
    We generate a plot for all values/levels of the index.
    """
    # Default query to filter data
    query_str = f'{dependent_col}== {dependent_col}'
    if quantile is not None:
        top_perc = df[ind_col].quantile(q=quantile)
        query_str = f'{ind_col}<= {top_perc}'

    elif top_val is not None:
        query_str = f'{ind_col}<= {top_val}'

    # Filter data
    view = df.query(query_str)

    # Create pivot with top category values
    agg_fun = (lambda x: np.log(x+1)) if with_log else (lambda x: x)
    gr_cols = [dependent_col, ind_col]
    pivot_grid = (view[gr_cols].groupby(gr_cols).size().apply(agg_fun)
                  .reset_index()
                  .pivot(index=ind_col,
                         columns=dependent_col,
                         values=0))
    # Normalize vals per column
    max_min_range = pivot_grid.max()-pivot_grid.min()
    pivot_grid = (pivot_grid-pivot_grid.min()) / max_min_range
    sns.heatmap(pivot_grid)

    title_str = f'Heatmap of  {ind_col} level percentages across '
    title_str += f'different {dependent_col}'
    plt.title(title_str, fontsize=15)
    plt.xlabel(f'{dependent_col} value')
    plt.ylabel(f'{ind_col} level')
    plt.show()


def get_one_to_one_relationship(
    df: pd.DataFrame,
    factor_id: str,
    factor_name: str
):
    """Do for a given factor, which we understand as a categorical column.

    Of different category levels. We would like to know if there is a
    1-1 relation between the ids of the values of that factor with the
    column corresponding to the names of those ids.
    """
    rv = None
    g = df[[factor_id, factor_name]].groupby(factor_id)
    id_col_name_counts = g.transform(lambda x: len(x.unique()))
    rv = id_col_name_counts
    return rv


def sum_count_aggregation(
    df: pd.DataFrame,
    group_cols: List,
    aggregation_operations: List=['sum', 'count'],
    numerical_cols: List=['PONDERACION_RIESGO_PAYMENT', 'Q_RECARGA']
):
    """Aggregate data by a gruop of columns into sum and count."""
    # Create aggregating dictionary
    agg_dict = {col: aggregation_operations for col in numerical_cols}

    # Group and aggregate
    counts = df.groupby(group_cols).agg(agg_dict)

    # Flatten multi-hierarchy index
    counts.columns = ['_'.join(col).strip() for col in counts.columns.values]

    for col in numerical_cols:
        for aggr in aggregation_operations:
            perc_col = '_'.join([col, aggr, 'perc'])
            aggr_col = '_'.join([col, aggr])
            counts[perc_col] = counts[aggr_col]/counts[aggr_col].sum()

    # Chose one column to sort for [first column]
    sort_col = [col for col in counts.columns if 'count' in col][0]
    counts.sort_values(by=sort_col, ascending=False)
    return counts


def sum_count_time_series(
    df: pd.DataFrame,
    date_col: str,
    resample_frequency: str='D',
    aggregation_operations: List=['sum', 'count'],
    numerical_series: List=['PONDERACION_RIESGO_PAYMENT', 'Q_RECARGA'],
    filter_query: str=None  # to select a subset of the whole database only
):
    """Get time series grouping by a certain time-window.

    Only for a view of the original df.
    """
    if not filter_query:
        filter_query = 'advertiser_name == advertiser_name'
    # generate aggregating dictionary
    agg_dict = {col: aggregation_operations for col in numerical_series}

    # Count the amount of events in this time frequency
    time_series = (df.query(filter_query).resample(
        resample_frequency,
        on=date_col)[numerical_series].agg(agg_dict)
        )

    # Flatten multi-hierarchy index
    time_series.columns = [
        '_'.join(col).strip()
        for col in time_series.columns.values
    ]
    # Reset index and sort by oldest event date first
    time_series = time_series.reset_index().sort_values(date_col)

    return time_series


def plot_agg_bar_charts(
    agg_df,
    agg_ops,
    group_cols,
    series_col: str='impressions',
    perc_filter: float=0.03
):
    """Plot bar charts on an aggregated dataframe."""
    perc_cols = ['_'.join([series_col, aggr, 'perc']) for aggr in agg_ops]

    fig = plt.figure()
    # Remove small levels in perc.
    (agg_df.query(f'{perc_cols[0]}>@perc_filter')[perc_cols].T.plot
        .bar(
            stacked=True, figsize=(8, 6),
            colormap="GnBu", label='right',
            ax=fig.gca(), rot=0))

    title_str = f'Grouped by:{group_cols!s}, aggregated by: {agg_ops!s}, '
    title_str += f'filter at least {perc_filter} perc'
    plt.title(title_str, color='black')
    plt.legend(loc='center left', bbox_to_anchor=(1.0, 0.5))
    plt.show()

# def plot_agg_time_series(
#     data: pd.DataFrame,
#     date_col: str='hour',
#     plot_series: List=['impressions','clicks']):
#     """Plot different line series on an already-aggregated df's category."""
#     fig = plt.figure()
#     data.set_index(date_col)[plot_series].plot(logy=True,
#                                             figsize =(8, 6),
#     #                                       colormap="GnBu",
#                                             label ='right',
#                                             ax=fig.gca()
#                                            )

#     title_str = '{!s} - {!s} time series for every {}'.format(val,
#                 category_col.capitalize(), time_frequency)
#     plt.title(title_str, color='black')


def _optimize_types(df):
    """Cast columns to more memory friendly types."""
    # Check which types best ponderacion_riesgo_payment must be float.
    new_dtypes = {
        c: 'float32' for c in
        df.dtypes[df.dtypes == 'float64'].index
    }
    df = df.astype(new_dtypes, copy=False)
    return df
