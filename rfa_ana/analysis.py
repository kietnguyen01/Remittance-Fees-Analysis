import pandas as pd
import seaborn as sns
import statsmodels.api as sm
import matplotlib.pyplot as plt

sns.set(style="whitegrid") 


def crypto_fee_percentage(dfs: dict) -> dict:
    """Calculate and create a new column for fee percentage based on average transaction fees and values"""
    for _, df in dfs.items():
        df['fee_percentage'] = (df['average_transaction_fees'] / df['average_transaction_value']) * 100
    return dfs


def crypto_fee_percentage_lineplot(dfs: dict) -> None:
    """Plot the fee percentage column for all dataframes in the dictionary on a weekly basis"""
    plt.figure(figsize=(10, 6))
    for name, df in dfs.items():
        # Resample to weekly frequency 
        df_weekly = df.resample('W').median()  
        # Calculate rolling mean with 4 week window
        df_roll = df_weekly.rolling(4, min_periods=1).mean()
        # Plot smoothed line
        sns.lineplot(data=df_roll, x=df_roll.index, y='fee_percentage', label=name)
    plt.xlabel('date')
    plt.ylabel('Fee Percentage')
    plt.title('Fee Percentage Plot (Weekly)')
    plt.yscale('log')
    plt.legend(loc='upper left', bbox_to_anchor=(1, 1))
    plt.show()


def crypto_fees_scenarios(dfs: dict) -> pd.DataFrame:
    """Create a dataframe that has different scenarios for each year for crypto fee percentage"""
    scenario_data = []
    for name, df in dfs.items():
        for year in range(2019, 2023):
            df_year = df[df.index.year == year]
            # Get the mean value of fee_percentage directly
            fee_percent = df_year['fee_percentage'].describe().loc['mean']
            # Get the mean value of 24h_volume directly
            volume_weight = df_year['24h_volume'].mean()
            scenario_data.append({
                'crypto': name,
                'year': year,
                'mean_fee_percent': fee_percent,
                'volume_weight': volume_weight
            })
    # Create dataframe and reorder columns
    scenario_df = pd.DataFrame(scenario_data)
    scenario_df = scenario_df.reindex(columns=['crypto', 'year', 'mean_fee_percent', 'volume_weight'])
    return scenario_df


def combine_average_case(scenario_df: pd.DataFrame) -> pd.DataFrame:
    """Create a dataframe that has the weighted_average_case for each year using the volume_weight"""
    combined_data = []
    for year in range(2019, 2023):
        scenario_df_year = scenario_df[scenario_df['year'] == year]
        # Calculate the weighted_average_case using the formula
        weighted_mean_fee = (scenario_df_year['mean_fee_percent'] * scenario_df_year['volume_weight']).sum() / scenario_df_year['volume_weight'].sum()
        combined_data.append({
            'year': year, 
            'Crypto': weighted_mean_fee * 100
        })
    # Create dataframe
    combined_df = pd.DataFrame(combined_data)
    combined_df.index = combined_df['year']
    combined_df = combined_df.drop(columns=['year'])
    return combined_df


def plot_gas_fees(df: pd.DataFrame) -> None:
    """Plot the Ethereum transaction fees and gas prices on a weekly basis"""
    plt.figure(figsize=(12, 6))
    # Resample to weekly frequency
    df_weekly = df.resample('W').mean()
    # Use the index as the x-axis
    sns.lineplot(data=df_weekly, x=df_weekly.index, y='transaction_fees')
    plt.xlabel('Date')
    plt.ylabel('Gas Fees (Dollar)')
    plt.title('Ethereum Gas Fees Plot (Weekly)')
    plt.show()


def stablecoin_fees_scenarios(df: pd.DataFrame) -> pd.DataFrame:
    """Create a dataframe that has different scenarios for Ethereum gas fees each year"""
    scenario_data = []
    for year in range(2019, 2023):
        df_year = df[df.index.year == year]
        # Get the mean value of transaction_fees directly
        mean_fee = df_year['transaction_fees'].describe().loc['mean']
        # Calculate fee percentage for $200
        scenario_data.append({
            'year': year,
            'Stablecoin': (mean_fee / 200) * 100
        })
    scenario_df = pd.DataFrame(scenario_data)
    scenario_df.index = scenario_df['year']
    scenario_df = scenario_df.drop(columns=['year'])
    return scenario_df


def predict_remittance_cost(df: pd.DataFrame) -> pd.DataFrame:
    """Predict remittance costs for the year 2021 and 2022 using linear regression"""
    # Empty predictions dataframe
    pred_df = pd.DataFrame(columns=df.columns)
    for col in df.columns:
        # Year and remittance cost variables
        X = df.index.values
        y = df[col].values
        # Year constant term
        X = sm.add_constant(X)
        # Fit model with Ordinary Least Squares
        model = sm.OLS(y, X).fit()
        # Predict with fitted model
        pred_X = sm.add_constant([2021, 2022])
        pred_y = model.predict(pred_X)
        # Add the predictions to the dataframe
        pred_df[col] = pred_y
    # Set the index of dataframe
    pred_df.index = [2021, 2022]
    pred_df.index.name = df.index.name
    return pred_df