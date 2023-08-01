import pandas as pd
import seaborn as sns
from tabulate import tabulate


def summary_stats(df: pd.DataFrame) -> None:
    """Print the summary statistics for dataframe"""
    print(tabulate(df.describe(), headers='keys', tablefmt='fancy_grid', floatfmt='.3f'))


def merge_lookup(df, lookup_df):
    """Merge dataframe to lookup dataframe by country code"""
    return pd.merge(df, lookup_df[["Country Code", "Region", "Income"]], on="Country Code")


def pivot_table(df, group):
    """Pivot dataframe by income or region and return transposed table"""
    flow_year_cols = df.columns[1:-2] # exclude Country Code, Region and Income
    pivot_df = pd.pivot_table(data=df, index=group, values=flow_year_cols, aggfunc="mean")
    pivot_df.index.name = "Year"
    pivot_df = pivot_df.transpose()
    return pivot_df


def plot_lineplot(pivot_df, group, ax):
    """Plot lineplot for pivot dataframe by income or region on given axis"""
    sns.lineplot(data=pivot_df, ax=ax)
    ax.set_title(f"Average Remittance {group}")
    ax.set_xlabel("Year")
    if "Income" in group:
        y_label = "Billion $"
    elif "Region" in group:
        y_label = "Ten Billion $"
    ax.set_ylabel(y_label)
    ax.legend(loc="upper left", title=group.split()[-1][:-1], bbox_to_anchor=(1, 1))
    # Set x-axis tick positions and rotate labels
    ax.set_xticks(ax.get_xticks())
    ax.set_xticklabels(ax.get_xticklabels(), rotation=25)