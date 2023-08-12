import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from tabulate import tabulate

sns.set(style="whitegrid") 


def get_fees_column(df: pd.DataFrame) -> str:
    """Get transaction fees column name from dataframe"""
    cols = df.columns
    if 'median_transaction_fees' in cols:
        return 'median_transaction_fees'
    elif 'average_transaction_fees' in cols:
        return 'average_transaction_fees'
    else:
        return ''
    

def get_value_column(df: pd.DataFrame) -> str:
    """Get transaction value column name from dataframe"""
    cols = df.columns
    if 'median_transaction_value' in cols:
        return 'median_transaction_value'
    elif 'average_transaction_value' in cols:
        return 'average_transaction_value'
    else:
        return ''


def fees_df(dfs: dict) -> pd.DataFrame:
    """Create a dataframe of transaction fees"""
    fees_data = pd.DataFrame()
    for token, df in dfs.items():
        col_name = get_fees_column(df)
        fees_data[token] = df[col_name]
    return fees_data


def transactions_df(dfs: dict) -> pd.DataFrame:
    """Create a dataframe of transaction values"""
    transactions_data = pd.DataFrame()
    for token, df in dfs.items():
        transactions_data[token] = df['transactions_count']
    return transactions_data


def fees_boxplots(dfs: dict) -> None:
    """Plot the box plots of transaction fees for each column"""
    df = fees_df(dfs)
    fig, axes = plt.subplots(nrows=len(df.columns), ncols=1, figsize=(10, 15))
    colors = sns.color_palette()
    for col, ax, color in zip(df.columns, axes, colors):
        # Plot box plot without outliers
        sns.boxplot(x=df[col], ax=ax, showfliers=False, color=color)
        ax.set_title(col.upper())
        ax.set_xlabel('')
    fig.suptitle('Crypto Transaction Fees')
    fig.subplots_adjust(top=0.85)
    plt.tight_layout()
    plt.show()


def corr_heatmaps(dfs: dict) -> None:
    """Plot the correlation heatmap of variables in each dataframe"""
    fig, axes = plt.subplots(nrows=10, ncols=1, figsize=(12, 60))
    axes = axes.flatten()  
    for name, df in dfs.items():
        # Get index position of token name
        i = list(dfs.keys()).index(name)
        ax = axes[i]
        corr_matrix = df.corr()
        sns.heatmap(corr_matrix, annot=True, ax=ax, cmap='mako_r')
        ax.set_title(name.upper())
        ax.set_xticklabels(ax.get_xticklabels(), rotation=15)
    plt.tight_layout()
    plt.show()


def summary_stats(dfs: dict) -> None:
    """Print the summary statistics for each dataframe"""
    for name, df in dfs.items():
        print(f'>>> {name.upper()}')
        print(tabulate(df.describe(), headers='keys', tablefmt='fancy_grid', floatfmt='.3f'))


def fees_stats(dfs: dict) -> None:
    """Calculate the standard deviation, skewness and kurtosis of the transaction fees for each token"""
    stats_list = []
    for name, df in dfs.items():
        # calculate and append each statistic to the list
        std = df['average_transaction_fees'].std()
        sk = df['average_transaction_fees'].skew()
        kt = df['average_transaction_fees'].kurt()
        stats_list.append((name, std, sk, kt))
    # create a new dataframe from the list
    stats_df = pd.DataFrame(stats_list, columns=['token', 'std', 'skew', 'kurt'])
    # Print the dataframe as table
    print(tabulate(stats_df, headers='keys', tablefmt='fancy_grid', floatfmt='.3f'))


def fees_scatterplots(dfs: dict) -> None:
    """Plot the scatterplots of transaction fees against 24h volume and set hue to transaction value"""
    # Create two subplots per row
    fig, axes = plt.subplots(nrows=len(dfs) // 2 + len(dfs) % 2, ncols=2, figsize=(15, 20))
    # Flatten the axes array
    axes = axes.flatten()
    colors = sns.color_palette()
    for name, df in dfs.items():
        # Get index position of token name
        i = list(dfs.keys()).index(name)
        ax = axes[i]
        fees_col = get_fees_column(df)
        volume = df['24h_volume']
        value_col = get_value_column(df)
        sns.regplot(x=volume, y=fees_col, data=df, ax=ax)
        if value_col:
            sns.scatterplot(x=volume, y=fees_col, data=df, ax=ax, color=colors[i], hue=value_col)
        else:
            sns.scatterplot(x=volume, y=fees_col, data=df, ax=ax, color=colors[i])
        ax.set_title(name.upper())
        ax.set_xlabel('24h_volume')
        ax.set_ylabel(fees_col)
    fig.suptitle('Transaction Fees vs 24h Volume')
    plt.tight_layout()
    plt.show()