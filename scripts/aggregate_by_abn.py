from .constants import *
from .transform import *
from .read import *
from .load import *
import matplotlib.pyplot as plt

def aggregate_by_abn(data: DataFrame, target_feature: str):
    """
    Aggregate the target_feature by merchant_abn
    - Parameters
        - data: DataFrame used to aggregate
        - target_feature: Feature to aggregate by
    - Returns
        - result: DataFrame with aggregated target_feature
    """

    # Aggregate target_feature by merchant_abn
    result = data.groupby(MERCHANT_ABN).agg({target_feature: 'sum'})
    return result

def plot_target_feature_distribution(data: DataFrame, target_feature: str, save_path: str):
    """
    Plot the distribution of the target_feature
    - Parameters
        - data: DataFrame to plot data with
        - target_feature: Feature to plot by
        - save_path: Path to save the figure
    - Returns
        - None
    """

    # Convert result to Pandas DataFrame
    new_df = data.toPandas()
    
    # Generate a histogram of the distribution of the target_feature
    plt.figure(figsize=(10, 6))  
    plt.hist(new_df[f'sum({target_feature})'], bins=30, color='blue', alpha=0.7)
    plt.title(f'Distribution of {target_feature}') 
    plt.xlabel(f'{target_feature}') 
    plt.ylabel('Frequency')
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.show() 

    # save the figure
    plt.savefig(save_path, dpi=300, bbox_inches='tight')

def give_statistical_info(data: DataFrame, target_feature: str):
    """
    Give statistical information of the target_feature
    - Parameters
        - data: DataFrame to plot data with
        - target_feature: Feature to plot by
    - Returns
        - None
    """
    
    # Convert result to Pandas DataFrame
    new_df = data.toPandas()
    
    # Generate a histogram of the distribution of the target_feature
    print(new_df[f'sum({target_feature})'].describe())