import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt
from tabulate import tabulate


def fees_stats(df: pd.DataFrame) -> None:
    """Calculate the standard deviation, skewness and kurtosis of the transaction fees"""
    # calculate and each statistic
    std = df['transaction_fees'].std()
    sk = df['transaction_fees'].skew()
    kt = df['transaction_fees'].kurt()
    # create a new dataframe
    stats_df = pd.DataFrame([[std, sk, kt]], columns=["std", "skew", "kurt"])
    stats_df = stats_df.rename(index={0: "transaction_fees"})
    # Print the dataframe as table
    print(tabulate(stats_df, headers='keys', tablefmt='fancy_grid', floatfmt='.3f'))


def volumes_lineplot(dfs: dict) -> None:
    """Plot the total volumes of all stablecoins in a line plot"""
    # Create an empty dataframe to store the total volumes
    df_volumes = pd.DataFrame()
    # Loop and append each token's total volume
    for token in dfs.keys():
        if token != 'fees':
            df = dfs[token]
            df_volumes[token] = df['total_volumes']
    df_volumes.index = pd.date_range(start='2019-01-01', end='2022-12-31', freq='D', name='date')
    df_volumes = df_volumes.resample('M').mean()
    plt.figure(figsize=(14, 8))
    sns.lineplot(data=df_volumes)
    plt.xlabel('Date')
    plt.ylabel('Total Volume')
    plt.title('Total Volumes of Stablecoins')
    plt.yscale('log')
    plt.show()


def mcaps_volumes_scatterplots(dfs: dict) -> None:
    """Plot a scatterplot of the market caps and total volumes of all stablecoins"""
    # Create a 2x4 grid of subplots
    fig, axes = plt.subplots(2, 4, figsize=(16, 8))
    # Loop and plot each token
    colors = sns.color_palette()
    for token, ax, color in zip(dfs.keys(), axes.flatten(), colors):
        if token != 'fees':
            df = dfs[token]
            sns.scatterplot(x='market_caps', y='total_volumes', data=df, ax=ax, color=color)
            ax.set_title(token)
    axes[-1, -1].remove()  # Remove the empty subplot
    fig.tight_layout()
    plt.show()