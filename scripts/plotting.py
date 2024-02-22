import plotly.express as px
import plotly.figure_factory as ff
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import seaborn as sns
from .constants import *
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date
import pandas as pd
from matplotlib.patches import Patch
import numpy as np
import random

X_TICK_SKIP = 4
DPI = 300

def make_matplotlib_barh(fname:str, title:str, x, y, 
                         x_label:str, y_label:str, x_ticks:list=None, 
                         dataset=None) -> None:
    """
    Plot a horizontal bar graph of two given list or list-like dtypes
    - Parameters:
        - fname: File path to save in
        - title: Title of the Graph
        - x: x values to plot (str/list)
        - y: y values to plot (str/list)
        - x_label: label of the x-axis
        - y_label: label of the y-axis
        - x_ticks: Optional, change representation
                    of values in x-axis
        - dataset: Optional, instead get each x, y
                    from dataset's columns
    """
    fig, ax = plt.subplots(figsize=(10,5))

    plt.suptitle(title, fontweight='bold', fontsize=15)
    plt.rcParams['font.family'] = 'serif'

    if dataset:
        y_axis = list(dataset.select(y).toPandas()[y])
        x_axis = list(dataset.select(x).toPandas()[x])
    else:
        y_axis = y
        x_axis = x

    # Set the font parameters and values of the plot
    _ = plt.barh(x_axis, y_axis)
    ax.set_xlabel(x_label, fontname='serif',
                  fontdict={'weight': 'bold',
                            'size': 15})
    ax.set_ylabel(y_label, fontname='serif',
                  fontdict={'weight': 'bold',
                            'size': 15})
    if x_ticks:
        tick_positions = range(0, len(x_ticks))
        tick_labels = [x_ticks[i] for i in tick_positions]
        ax.set_xticks(tick_positions, tick_labels)
    else:
        ax.set_yticks(range(len(x_axis)))
        ax.set_yticklabels(x_axis)

    # Label the lines correctly
    fig.savefig(fname, dpi=DPI, bbox_inches='tight')
    return


def make_plotly_barh(title:str, x, y, 
                         x_label:str, y_label:str, x_ticks:list=None, 
                         dataset=None) -> None:
    """
    Plot a horizontal bar graph of two given list or list-like dtypes,
    Used for notebooks for more interactive UI
    - Parameters:
        - title: Title of the Graph
        - x: x values to plot (str/list)
        - y: y values to plot (str/list)
        - x_label: label of the x-axis
        - y_label: label of the y-axis
        - x_ticks: Optional, change representation
                    of values in x-axis
        - dataset: Optional, instead get each x, y
                    from dataset's columns
    """
    if dataset:
        fig = px.bar(dataset, x=x, y=y,
                            title=title,
                            orientation='h')
    else:
        fig = px.bar(y=x, x=y, 
                     title=title,
                     orientation='h')

    # Update x-tick values if not None
    if x_ticks:
        fig.update_xaxes(tickvals=x_ticks, 
        ticktext=[str(t) for t in x_ticks])

    # Update layout of plotly graph
    fig.update_layout(xaxis_title=x_label,
                      yaxis_title=y_label,
                      title = dict(x=0.5),
                      font = dict(size=20, family='Droid Serif'),
                      margin=dict(l=60, r=60, t=60, b=60),
                      height=len(x)*30)
    fig.show()
    return


def make_matplotlib_line(fname:str, title:str, x, y, x_label:str,
                          y_label:str, x_ticks:list=None, 
                          dataset=None) -> None:
    """
    Plot a line graph of two given list or list-like dtypes
    - Parameters:
        - fname: File path to save in
        - title: Title of the Graph
        - x: x values to plot (str/list)
        - y: y values to plot (str/list)
        - x_label: label of the x-axis
        - y_label: label of the y-axis
        - x_ticks: Optional, change representation
                    of values in x-axis
        - dataset: Optional, instead get each x, y
                    from dataset's columns
    """
    fig, ax = plt.subplots(figsize=(10,5))
    
    plt.rcParams['font.family'] = 'serif'
    plt.suptitle(title, fontweight='bold', fontsize=15)

    if dataset:
        y_axis = list(dataset.select(y).toPandas()[y])
        x_axis = list(dataset.select(x).toPandas()[x])
    else:
        y_axis = y
        x_axis = x

    plt.plot(x_axis, y_axis)
    ax.set_xlabel(x_label, fontname='serif', 
                  fontdict={'weight': 'bold', 'size': 15})
    ax.set_ylabel(y_label, fontname='serif', 
                  fontdict={'weight': 'bold', 'size': 15})
    if x_ticks:
        if len(x_ticks) <= 10:
            tick_positions = range(0, len(x_ticks))
        else:
            tick_positions = range(0, len(x_ticks), X_TICK_SKIP)
        tick_labels = [x_ticks[i] for i in tick_positions]
        ax.set_xticks(tick_positions, tick_labels, rotation=45, ha="right")
    else:
        pass

    fig.savefig(fname, dpi=DPI, bbox_inches='tight')
    return


def make_plotly_line(title:str, x, y, 
                        x_label:str, y_label:str, 
                        x_ticks:list=None,
                        dataset=None) -> None:
    """
    Plot a line graph of two given list or list-like dtypes
    Used for notebooks for more interactive UI
    - Parameters:

        - title: Title of the Graph
        - x: x values to plot
        - y: y values to plot
        - x_label: label of the x-axis
        - y_label: label of the y-axis
        - x_ticks: Optional, change representation
                    of values in x-axis
        - dataset: Optional, instead get each x, y
                    from dataset's columns
    """
    if dataset:
        fig = px.line(dataset, x=x, y=y,
                            title=title)
    else:
        fig = px.line(x=x, y=y, title=title)

    # Update x-tick values if not None
    if x_ticks:
        fig.update_xaxes(tickvals=x_ticks, 
        ticktext=[str(t) for t in x_ticks])

    # Update layout of plotly graph
    fig.update_layout(xaxis_title=x_label,
                      yaxis_title=y_label,
                      title = dict(x=0.5),
                      font = dict(size=20, family='Serif'),
                      margin=dict(l=60, r=60, t=60, b=60))
    fig.show()
    return


# We have to make our own special plotting function because of the relationship between the x-ticks and the x-labels
def make_matplotlib_line_irregular(fname:str, title:str, x, y, 
                                   x_label:str, y_label:str, y_pred:list, 
                                   x_ticks:list=None,
                                   legend:list=None ,dataset=None) -> None:
    """
    Makes a line plot, works when x values are not a clean sequence of 1,2,3...
    - Parameters
        - fname: Path for file saving
        - title: Title of plot
        - x: x values, positions in the graph (str/list)
        - y: y values, positions in the graph (str/list)
        - x_label: Name of X axis
        - y_label: Name of Y axis
        - y_pred: List in case you want to plot predicted
                  Put None if you don't want one
        - x_ticks: Used if you want to change the name of the x values
        - legend: List indicating names you want to set for both plots
                Assumes 2 labels
        - dataset: Used if you are indexing from a dataset with cols x, y
    - Returns
    -   None, but shows and saves a figure
    """
    fig, ax = plt.subplots(figsize=(10,5))
    
    plt.rcParams['font.family'] = 'serif'
    plt.suptitle(title, fontweight='bold', fontsize=15)

    if dataset:
        y_axis = list(dataset.select(y).toPandas()[y])
        x_axis = list(dataset.select(x).toPandas()[x])
    else:
        y_axis = y
        x_axis = x
    
    # need this to set the legends
    if legend:
        ax.plot(x_axis, y_axis, label=legend[0])
    # Otherwise just plot normally
    else:
        ax.plot(x_axis, y_axis)

    ax.set_xlabel(x_label, fontname='serif',
                  fontdict={'size': 15})
    ax.set_ylabel(y_label, fontname='serif',
                  fontdict={'size': 15})
    if x_ticks != None:
        # This is the change, instead we set tick_positions
        # to x, labels don't have to be indexed since
        # both x and x_ticks are proper order already
        if len(x_ticks) <= 10:
            tick_positions = range(0, len(x_ticks))
        else:
            tick_positions = range(0, len(x_ticks), X_TICK_SKIP)
        tick_labels = [x_ticks[i] for i in tick_positions]
        tick_positions = [x[i] for i in tick_positions]
        ax.set_xticks(tick_positions, tick_labels, rotation=45, ha="right")
        ax.tick_params(axis='x', labelsize=10)
        ax.tick_params(axis='y', labelsize=10)
    else:
        # ax.set_xticks(x_label, rotation=45, ha="right")
        pass

    if y_pred:
        # Get latter half of indices
        if legend:
            for i in range(len(y_pred)):
                back_len = len(y_pred[i])
                x_pred = list(x[-back_len:])
                ax.plot(x_pred, y_pred[i], label=legend[i+1])
                ax.legend(loc='upper right') 
        else:
            ax.plot(x_pred, y_pred)

    plt.subplots_adjust(bottom=0.1)
    # Label the lines correctly
    x_label = x_label.replace(" ", "_")
    y_label = y_label.replace (" ", "_")
    fig.savefig(fname, dpi=DPI, bbox_inches='tight')
    return


def make_plotly_line_irregular(title:str, x, y, 
                                x_label:str, y_label:str, y_pred:list, 
                                x_ticks:list=None,
                                legend:list=None ,dataset=None) -> None:
    """
    Makes a line plot, works when x values are not a clean sequence of 1,2,3...
    - Parameters
        - title: Title of plot
        - x: x values, positions in the graph (str/list)
        - y: y values, positions in the graph (str/list)
        - x_label: Name of X axis
        - y_label: Name of Y axis
        - y_pred: List in case you want to plot predicted
                  Put None if you don't want one
        - x_ticks: Used if you want to change the name of the x values
        - legend: List indicating names you want to set for both plots;
        - dataset: Used if you are indexing from a dataset using cols x, y
    - Returns
    -   None, but shows and saves a figure
    """

    actual_labels = [f'wide_variable_{i}' for i in range(len(legend))]
    map_labels = {}
    for i in range(len(legend)):
        map_labels[actual_labels[i]] = f'Line {i+1}'

    if not dataset:
        x_vals = x
        y_vals = y
    else:
        x_vals = dataset[x]
        y_vals = dataset[y]


    if y_pred and legend:
        fig = px.line(dataset, x=x_vals, 
                      y=[y_vals]+y_pred,
                      title=title,
                      labels=map_labels)
        for i in range(len(map_labels)):
            fig.update_traces(selector=dict(name=list(map_labels.keys())[i]),
                              name = legend[i])
    else:
        fig = px.line(dataset, x=x_vals, y=y_vals,
                            title=title)
        
    # Update x-tick values if not None
    if x_ticks:
        tick_vals = []
        ticktext = []
        for i in range(0, len(x_vals), 6):
            tick_vals.append(x_vals[i])
            ticktext.append(x_ticks[i])
        fig.update_xaxes(tickvals=tick_vals, 
        ticktext=ticktext)

    # # Position x-axis below the plot
    fig.update_xaxes(tickangle=45)

    # Update layout of plotly graph
    fig.update_layout(xaxis_title=x_label,
                      yaxis_title=y_label,
                      title = dict(x=0.5),
                      font = dict(size=20, family='Serif'),
                      margin=dict(l=60, r=60, t=60, b=60))
    fig.show()
    return

def plot_distribution(df, column:str, prefix:str="", append_at_end:str=""):
    """
    Function to plot the distribution of a column in a dataframe
    - Parameters
        - df: Dataframe of the distribution
        - column: Column to plot
        - prefix: Prefix to add to the title
        - append_at_end: String to append at the end of the title
    - Returns
        - None, but shows a plot
    """
    if type(df) == DataFrame:
        data = df.select(F.col(column)).toPandas()
    elif type(df) == pd.DataFrame:
        data = df[column]
    else:
        raise ValueError("INVALID DATAFRAME TYPE")

    plt.figure(figsize=(8, 6))
    sns.histplot(data = df, x = data, bins=20, kde=True)
    plt.title(f'Distribution of {column}{append_at_end}')
    plt.xlabel(column)
    plt.ylabel('Frequency')
    plt.show()

def plot_consumer_count_by_state(title:str, grouped_consumer:pd.DataFrame, x_label:str, y_label:str, save_path:str):
    """
    Function to plot the consumer count by state
    - Parameters
        - title: Title of the plot
        - grouped_consumer: Dataframe of the grouped consumer
        - x_label: Label of the x axis
        - y_label: Label of the y axis
        - save_path: Path to save the plot
    - Returns
        - None, but shows a plot
    """
    plt.figure(figsize=(8, 4))
    state_names = grouped_consumer[STE_NAME]
    consumer_counts = grouped_consumer["consumer_count"]

    # create a color palette for each state
    colors = sns.color_palette("pastel", n_colors=len(state_names))
    
    # plot bar chart
    plt.bar(state_names, consumer_counts, color=colors)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(title)
    plt.xticks(rotation=45)

    plt.tight_layout()

    # save plot
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    plt.show()

def plot_pie_chart(title:str, data, labels, save_path:str):
    """
    Function to plot the pie chart
    - Parameters
        - title: Title of the plot
        - data: Data of the pie chart
        - labels: Labels of the pie chart
        - save_path: Path to save the plot
    - Returns
        - None, but shows a plot
    """
    plt.figure(figsize=(6, 6))
    plt.pie(data, labels=labels, autopct='%1.1f%%', startangle=140)
    plt.title(title)
    plt.axis('equal')

    # save plot
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    plt.show()

def plot_ratio_comparison(joined_male_and_female_ratio: DataFrame, save_path:str):
    """
    plot the compared bar chart for the consumer_male_female_ratio and official_male_female_ratio for each state_name
    - Parameters
        - joined_male_and_female_ratio: datafrmae that store the joined male and female ratio
        - save_path: Path to save the plot
    - Returns
        - None, but shows a plot
    """
    
    bar_width = 0.25
    x = range(len(joined_male_and_female_ratio))
    plt.figure(figsize=(12, 6))
    
    # plot bar char for the consumer ratio
    plt.bar(x, joined_male_and_female_ratio['consumer_male_female_ratio'], width=bar_width, label='Consumer Ratio', color='skyblue', alpha=0.7)
    
    # plot bar char for the official ratio
    plt.bar([i + bar_width for i in x], joined_male_and_female_ratio['official_male_female_ratio'], width=bar_width, label='Official Ratio', color='lightcoral', alpha=0.7)

    # plot bar chart for the consumer undisclosed ratio
    plt.bar([i + 2 * bar_width for i in x], joined_male_and_female_ratio['consumer_undisclosed_ratio'], width=bar_width, label='Undisclosed Ratio', color='purple', alpha=0.7)
    
    # set x axis label, y axis label and title
    plt.xlabel(STE_NAME)
    plt.xticks([i + 1.5 * bar_width for i in x], joined_male_and_female_ratio[STE_NAME], rotation=45)
    plt.ylabel('Ratio')
    plt.title('Consumer vs Official Male, Female, and Undisclosed Ratio Comparison by State')
    # Place legend outside the plot area (to the upper right)
    plt.legend(loc='upper right', bbox_to_anchor=(1.1, 1.1))
     # Set y axis tick positions and labels
    y_ticks = [i * 0.1 for i in range(int(min(joined_male_and_female_ratio[['consumer_male_female_ratio', 'consumer_undisclosed_ratio']].min()) / 0.1),
                                      int(max(joined_male_and_female_ratio[['consumer_male_female_ratio', 'consumer_undisclosed_ratio']].max()) / 0.1) + 1)]
    plt.yticks(y_ticks)
    plt.tight_layout()

    # save plot
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    plt.show()


def plot_bar_chart_of_sex_ratio_for_top_100_sa2(data: DataFrame, save_path:str):
    """
    Function is to plot bar char for the different sex ratio levels for the top 100 SA2
    - Parameters
        - data: Dataframe that store the data
        - save_path: Path to save the plot
    - Returns
        - None, but shows a plot
    """
    # create bins and labels
    bins = [0, 0.85, 0.90, 0.95, 1.00, 1.05, 1.10, 1.15, float('inf')]
    labels = ['<0.85', '0.85-0.90', '0.90-0.95', '0.95-1.00', '1.00-1.05', '1.05-1.1.00', '1.10-1.15', '>=1.15']

    data['ratio_group'] = pd.cut(data['male_female_ratio'], bins=bins, labels=labels)

    grouped_counts = data['ratio_group'].value_counts().sort_index()
    
    cmap = plt.get_cmap('viridis')
    colors = cmap(np.linspace(0, 1, len(grouped_counts)))

    # plot bar chart
    plt.figure(figsize=(10, 6))
    plt.bar(grouped_counts.index, grouped_counts.values, color=colors)
    plt.xlabel("Male/Female Ratio Group")
    plt.ylabel("Count")
    plt.title("Count of SA2s in Each Male/Female Ratio Group")
    plt.xticks(rotation=45)
    plt.tight_layout()

    # save plot
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    plt.show()

def plot_and_save_merchants_per_state(data: DataFrame, title: str, save_path: str) -> None:
    """
    Plot and save the number of merchants that made the transactions per state
    - Parameters
        - data: Dataframe that store the data
        - title: Title of the plot
        - save_path: Path to save the plot
    - Returns
        - None, but shows a plot
    """

    # Set the range of colorbar
    cmap = plt.get_cmap("hot_r")
    new_cmap = mcolors.ListedColormap(cmap(np.linspace(0, 0.78, 256)))

    # Plot the map
    fig, ax = plt.subplots(figsize=(12, 12))
    data.plot(column="merchant_abn_count", cmap=new_cmap, linewidth=0.8, ax=ax, edgecolor="0.8", legend=True, legend_kwds={'shrink': 0.7})

    # Create text annotation for each state
    for x, y, state, sa2_code, label in zip(data.geometry.centroid.x, data.geometry.centroid.y, data["state"], data["sa2_code"], data["merchant_abn_count"]):
        conditions = [
            (state == 'NT' and sa2_code == '702011054'),
            (state == 'SA' and sa2_code == '406011132'),
            (state == 'NSW' and sa2_code == '105011093'),
            (state == 'VIC' and sa2_code == '206071517'),
            (state == 'TAS' and sa2_code == '603011065'),
            (state == 'ACT' and sa2_code == '801071082'),
            (state == 'WA' and sa2_code == '510021267'),
            (state == 'QLD' and sa2_code == '315021407')
        ]
        if any(conditions):
            text = f'{state}: {label}'  # Add state name to the text
            ax.annotate(
                text=text,
                xy=(x, y),
                xytext=(3, 3),
                textcoords="offset points",
                bbox=dict(boxstyle='round,pad=0.5', edgecolor='black', facecolor='white', alpha=0.7)
            )

    ax.set_title(title)

    # Save the plot
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    plt.show()

def plot_and_save_merchants_per_sa2(data: pd.DataFrame, title: str, save_path: str) -> None:
    """
    Plot and save the number of merchants per SA2 region and display the top 5 SA2 regions.
    - Parameters
        - data: Dataframe that store the data
        - title: Title of the plot
        - save_path: Path to save the plot
    - Returns
        - None, but shows a plot
    """

    # Set the color map
    cmap = plt.get_cmap("hot_r")
    new_cmap = mcolors.ListedColormap(cmap(np.linspace(0, 1, 256)))

    # Calculate the map data bounds
    minx, miny, maxx, maxy = data.total_bounds

    # Create the plot
    fig, ax = plt.subplots(figsize=(15, 36))

    # Set the map coordinates range
    ax.set_xlim(minx + 10, maxx - 10)  # You can fine-tune the range
    ax.set_ylim(miny - 0.1, maxy + 0.1)

    # Plot SA2 regions with merchant counts
    data.plot(column="sa2_merchant_abn_count", cmap=new_cmap, linewidth=0.8, ax=ax, edgecolor="0.8", legend=True, legend_kwds={'shrink': 0.2})

    # Turn off the external axis
    ax.axis('off')

    # Get the top 5 SA2 regions with the highest merchant counts
    top_5_sa2 = data.nlargest(5, "sa2_merchant_abn_count")

    # Annotate the top 5 SA2 regions with details and offset them
    for x, y, sa2_name, label, state_name in zip(top_5_sa2.geometry.centroid.x, top_5_sa2.geometry.centroid.y, top_5_sa2["sa2_name"], top_5_sa2["sa2_merchant_abn_count"], top_5_sa2["state"]):
        text = f'SA2 Name: {sa2_name}\nCount: {label}\nState: {state_name}'
        ax.annotate(
            text=text,
            xy=(x, y),
            xytext=(3, 3),
            textcoords="offset points",
            bbox=dict(boxstyle='round,pad=0.5', edgecolor='black', facecolor='white', alpha=0.7)
        )

    # Add a text box to display the top 5 SA2 areas in the top left corner
    ax.text(minx + 10, maxy - 1, "Top 5 SA2 Areas:", fontsize=12, weight='bold')
    for i, (sa2_name, label, state_name) in enumerate(zip(top_5_sa2["sa2_name"], top_5_sa2["sa2_merchant_abn_count"], top_5_sa2["state"])):
        text = f'{sa2_name}: {label}\nState: {state_name}'
        ax.text(minx + 10, maxy - i*2 - 3, text, fontsize=10)

    ax.set_title(title)

    # Save the plot
    plt.savefig(save_path, bbox_inches='tight')
    plt.show()

def plot_and_save_dollar_per_state(data: DataFrame, title: str, save_path: str) -> None:
    """
    Plot and save the total dollar value of transactions per state
    - Parameters
        - data: Dataframe that store the data
        - title: Title of the plot
        - save_path: Path to save the plot
    - Returns
        - None, but shows a plot
    """

    # Set the range of colorbar
    cmap = plt.get_cmap("hot_r")
    new_cmap = mcolors.ListedColormap(cmap(np.linspace(0, 0.78, 256)))
    
    # Plot the map
    fig, ax = plt.subplots(figsize=(12, 12))
    data.plot(column="total_dollar_value", cmap=new_cmap, linewidth=0.8, ax=ax, edgecolor="0.8", legend=True, legend_kwds={'shrink': 0.7})

    # Turn off the external axis
    ax.axis('off')
    
    # Create text annotation for each state with values in millions
    for x, y, state, sa2_code, label in zip(data.geometry.centroid.x, data.geometry.centroid.y, data["state"], data["sa2_code"], data["total_dollar_value"]):
        conditions = [
            (state == 'NT' and sa2_code == '702011054'),
            (state == 'SA' and sa2_code == '406011132'),
            (state == 'NSW' and sa2_code == '105011093'),
            (state == 'VIC' and sa2_code == '206071517'),
            (state == 'TAS' and sa2_code == '603011065'),
            (state == 'ACT' and sa2_code == '801071082'),
            (state == 'WA' and sa2_code == '511041290'),
            (state == 'QLD' and sa2_code == '315021407')
        ]
        if any(conditions):
            text = f'{state}: {label:.2f} Million'  # Add state name and format the label to two decimal places with "Million"
            ax.annotate(
                text=text,
                xy=(x, y),
                xytext=(3, 3),
                textcoords="offset points",
                bbox=dict(boxstyle='round,pad=0.5', edgecolor='black', facecolor='white', alpha=0.7)
            )

    ax.set_title(title)

    # Save the plot
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    plt.show()

def plot_and_save_dollar_per_sa2(data: pd.DataFrame, title: str, save_path: str) -> None:
    """
    Plot the and save sum of business transaction per SA2 region and display the top 5 SA2 regions.
    - Parameters
        - data: Dataframe that store the visualisation data
        - title: Title of the plot
        - save_path: Path to save the plot
    - Returns
        - None, but shows a plot
    """

    # Set the color map
    cmap = plt.get_cmap("hot_r")
    new_cmap = mcolors.ListedColormap(cmap(np.linspace(0, 1, 256)))

    # Calculate the map data bounds
    minx, miny, maxx, maxy = data.total_bounds

    # Create the plot
    fig, ax = plt.subplots(figsize=(15, 36))

    # Set the map coordinates range
    ax.set_xlim(minx + 10, maxx - 10)  # You can fine-tune the range
    ax.set_ylim(miny - 0.1, maxy + 0.1)

    # Plot SA2 regions with merchant counts
    data.plot(column="total_dollar_value", cmap=new_cmap, linewidth=0.8, ax=ax, edgecolor="0.8", legend=True, legend_kwds={'shrink': 0.2})

    # Turn off the external axis
    ax.axis('off')

    # Get the top 5 SA2 regions with the highest merchant counts
    top_5_sa2 = data.nlargest(5, "total_dollar_value")

    # Annotate the top 5 SA2 regions with details and offset them
    for x, y, sa2_name, label, state_name in zip(top_5_sa2.geometry.centroid.x, top_5_sa2.geometry.centroid.y, top_5_sa2["sa2_name"], top_5_sa2["total_dollar_value"], top_5_sa2["state"]):
        text = f'SA2 Name: {sa2_name}\nCount: {label}\nState: {state_name}'
        ax.annotate(
            text=text,
            xy=(x, y),
            xytext=(3, 3),
            textcoords="offset points",
            bbox=dict(boxstyle='round,pad=0.5', edgecolor='black', facecolor='white', alpha=0.5)
        )

    # Add a text box to display the top 5 SA2 areas in the top left corner
    ax.text(minx + 10, maxy - 1, "Top 5 SA2 Areas:", fontsize=12, weight='bold')
    for i, (sa2_name, label, state_name) in enumerate(zip(top_5_sa2["sa2_name"], top_5_sa2["total_dollar_value"], top_5_sa2["state"])):
        text = f'{sa2_name}: {label}\nState: {state_name}'
        ax.text(minx + 10, maxy - i*2 - 3, text, fontsize=10)

    ax.set_title(title)

    # Save the plot
    plt.savefig(save_path, bbox_inches='tight')
    plt.show()

def plot_and_save_customer_loyalty_per_state(data: DataFrame, title: str, save_path: str) -> None:
    """
    Plot and save the average customer loyalty per state.
    - Parameters
        - data: Dataframe that store the customer loyalty state visualisation data
        - title: Title of the plot
        - save_path: Path to save the plot
    - Returns
        - None, but shows a plot
    """

    # Set the range of colorbar
    cmap = plt.get_cmap("hot_r")
    new_cmap = mcolors.ListedColormap(cmap(np.linspace(0, 0.78, 256)))
    
    # Plot the map
    fig, ax = plt.subplots(figsize=(12, 12))
    data.plot(column="avg_loyalty", cmap=new_cmap, linewidth=0.8, ax=ax, edgecolor="0.8", legend=True, legend_kwds={'shrink': 0.7})

    # Turn off the external axis
    ax.axis('off')
    
    # Create text annotation for each state with values in millions
    for x, y, state, sa2_code, label in zip(data.geometry.centroid.x, data.geometry.centroid.y, data["state"], data["sa2_code"], data["avg_loyalty"]):
        conditions = [
            (state == 'NT' and sa2_code == '702011054'),
            (state == 'SA' and sa2_code == '406011132'),
            (state == 'NSW' and sa2_code == '105011093'),
            (state == 'VIC' and sa2_code == '206071517'),
            (state == 'TAS' and sa2_code == '603011065'),
            (state == 'ACT' and sa2_code == '801071082'),
            (state == 'WA' and sa2_code == '511041290'),
            (state == 'QLD' and sa2_code == '315021407')
        ]
        if any(conditions):
            text = f'{state}: {label}'  # Add state name and format the label to two decimal places with "Million"
            ax.annotate(
                text=text,
                xy=(x, y),
                xytext=(3, 3),
                textcoords="offset points",
                bbox=dict(boxstyle='round,pad=0.5', edgecolor='black', facecolor='white', alpha=0.7)
            )

    ax.set_title(title)

    # Save the plot
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    plt.show()

def plot_and_save_customer_royalty_per_sa2(data: pd.DataFrame, title: str, save_path: str) -> None:
    """
    Plot and save the average customer royalty per SA2 region and display the top 5 SA2 regions.
    - Parameters
        - data: Dataframe that store the customer royalty sa2 visualisation data
        - title: Title of the plot
        - save_path: Path to save the plot
    - Returns
        - None, but shows a plot
    """

    # Set the color map
    cmap = plt.get_cmap("hot_r")
    new_cmap = mcolors.ListedColormap(cmap(np.linspace(0, 1, 256)))

    # Calculate the map data bounds
    minx, miny, maxx, maxy = data.total_bounds

    # Create the plot
    fig, ax = plt.subplots(figsize=(18, 36))

    # Set the map coordinates range
    ax.set_xlim(minx + 10, maxx - 10)  # You can fine-tune the range
    ax.set_ylim(miny - 0.1, maxy + 0.1)

    # Plot SA2 regions with merchant counts
    data.plot(column="avg_loyalty", cmap=new_cmap, linewidth=0.8, ax=ax, edgecolor="0.8", legend=True, legend_kwds={'shrink': 0.2})

    # Turn off the external axis
    ax.axis('off')

    # Get the top 5 SA2 regions with the highest merchant counts
    top_5_sa2 = data.nlargest(5, "avg_loyalty")

    # Annotate the top 5 SA2 regions with details and offset them
    for x, y, sa2_name, label, state_name in zip(top_5_sa2.geometry.centroid.x, top_5_sa2.geometry.centroid.y, top_5_sa2["sa2_name"], top_5_sa2["avg_loyalty"], top_5_sa2["state"]):
        text = f'SA2 Name: {sa2_name}\nCount: {label}\nState: {state_name}'
        ax.annotate(
            text=text,
            xy=(x, y),
            xytext=(3, 3),
            textcoords="offset points",
            bbox=dict(boxstyle='round,pad=0.5', edgecolor='black', facecolor='white', alpha=0.7)
        )

    # Add a text box to display the top 5 SA2 areas in the top left corner
    ax.text(minx + 10, maxy - 1, "Top 5 SA2 Areas:", fontsize=12, weight='bold')
    for i, (sa2_name, label, state_name) in enumerate(zip(top_5_sa2["sa2_name"], top_5_sa2["avg_loyalty"], top_5_sa2["state"])):
        text = f'{sa2_name}: {label}\nState: {state_name}'
        ax.text(minx + 10, maxy - i*2 - 3, text, fontsize=10)

    ax.set_title(title)
    
    # save the plot
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    plt.show()
    
def plot_time_series(df, date_column: str, title: str):
    """
    Generates a time series line plot 
    - Parameters
        - df: DataFrame that contains the data
        - date_column: The name of the date column
        - title: The title of the plot
    - Returns
        - None, but shows a plot
    """
    
    # Check the data type of the 'date_column'
    date_column_type = df.schema[date_column].dataType
    
    if not isinstance(date_column_type, DateType):
        # If the date_column is not already of DateType, create a new DataFrame with the date column
        time_series_df = df.withColumn("date", to_date(col(date_column), "yyyy-MM-dd"))
    else:
        # If it's already of DateType, no conversion is needed
        time_series_df = df
    
    # Perform time series aggregation (e.g. daily transaction count)
    time_series = time_series_df.groupBy("date").count().orderBy("date")

    # Create a time series line plot 
    plt.figure(figsize=(12, 6))
    sns.set(style="whitegrid") 

    # Plot the time series line
    sns.lineplot(data=time_series.toPandas(), x = "date", y="count", marker="o", color="b", label="date")

    # Customize the plot
    plt.xlabel('Date')
    plt.ylabel('Count')
    plt.title(title)
    plt.xticks(rotation=45)
    plt.tight_layout()

    # Display the plot
    plt.show()

def plot_categorical_countplot(data, column: str, title: str) -> None:
    """
    This is a function to plot the distribution o the categorical data for consumers based on their genders.
    - Parameters
        - data: DataFrame that contains the data
        - column: The name of the column
        - title: The title of the plot
    - Returns
        - None, but shows a plot
    """
    plt.figure(figsize=(8, 6))
    ax = sns.countplot(data = data, x = column, palette = 'viridis')
    ax.bar_label(ax.containers[0])
    plt.xlabel(column)
    plt.ylabel('Count')
    plt.title(title)
    
    plt.tight_layout()
    plt.show()

def plot_and_save_consumers_per_transaction_per_state(data: DataFrame, title: str, save_path: str) -> None:
    """
    Plot and save the number of consumers per transaction per state.
    - Parameters
        - data: DataFrame that contains the data
        - title: The title of the plot
        - save_path: The path to save the plot
    - Returns
        - None, but shows a plot
    """
    
    # Plot the map
    fig, ax = plt.subplots(figsize=(12, 12))
    data.plot(column = "num_consumers", cmap = "OrRd", linewidth = 0.8, ax = ax, edgecolor = "0.8", legend = True, legend_kwds = {'shrink': 0.7})


    # Create text annotation for each state
    for x, y, state, sa2_code, label in zip(data.geometry.centroid.x, data.geometry.centroid.y, data["state"], data["sa2_code"], data["num_consumers"]):
        conditions = [
            (state == 'NT' and sa2_code == '702011054'),
            (state == 'SA' and sa2_code == '406011132'),
            (state == 'NSW' and sa2_code == '105011093'),
            (state == 'VIC' and sa2_code == '206071517'),
            (state == 'TAS' and sa2_code == '603011065'),
            (state == 'ACT' and sa2_code == '801071082'),
            (state == 'WA' and sa2_code == '510021267'),
            (state == 'QLD' and sa2_code == '315021407')
        ]
        if any(conditions):
            text = f'{state}: {label}'  # Add state name to the text
            ax.annotate(
                text=text,
                xy=(x, y),
                xytext=(0, 0),  # Adjust the position of the text
                textcoords="offset points",
                fontsize=10,
                ha='center', va='center',  # Center the text
                bbox=dict(boxstyle='round,pad=0.5', edgecolor='black', facecolor='white', alpha=0.7)
            )

    ax.set_title(title, fontdict={'fontsize': '15', 'fontweight': 'bold'})

    # save the plot
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    plt.show()

def plot_average_dollar_value_by_state(data: DataFrame, title: str, save_path: str) -> None:
    """
    Plot and save the avegare dollar value by state.
    - Parameters
        - data: DataFrame that contains the data
        - title: The title of the plot
        - save_path: The path to save the plot
    - Returns
        - None, but shows a plot
    """
    
    fig, ax = plt.subplots(figsize=(12, 12))
    data.plot(column = "avg_dollar_value", cmap = "OrRd", linewidth = 0.8, ax = ax, edgecolor = "0.8", legend = True, legend_kwds = {'shrink': 0.7})
    
    # Create text annotation for each state
    for x, y, state, sa2_code, label in zip(data.geometry.centroid.x, data.geometry.centroid.y, data["state"], data["sa2_code"], data["avg_dollar_value"]):
        conditions = [
            (state == 'NT' and sa2_code == '702011054'),
            (state == 'SA' and sa2_code == '406011132'),
            (state == 'NSW' and sa2_code == '105011093'),
            (state == 'VIC' and sa2_code == '206071517'),
            (state == 'TAS' and sa2_code == '603011065'),
            (state == 'ACT' and sa2_code == '801071082'),
            (state == 'WA' and sa2_code == '510021267'),
            (state == 'QLD' and sa2_code == '315021407')
        ]
        if any(conditions):
            text = f'{state}: {label}'  # Add state name to the text
            ax.annotate(
                text=text,
                xy=(x, y),
                xytext=(3, 3),  # Adjust the position of the text
                textcoords="offset points",
                fontsize=10,
                ha='center', va='center',  # Center the text
                bbox=dict(boxstyle='round,pad=0.5', edgecolor='black', facecolor='white', alpha=0.7)
            )

    # Customize plot settings
    ax.set_title(title, fontdict={'fontsize': '15', 'fontweight': 'bold'})

    # Save the plot (replace 'path_to_save_plot.png' with your desired path)
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    plt.show()

def plot_gender_distribution_by_state(data: DataFrame, title: str, column_name: str) -> None:
    """
    Plot the distribution of the gender recorded by state.
    - Parameters
        - data: DataFrame that contains the data
        - title: The title of the plot
        - column_name: The name of the column to plot
    - Returns
        - None, but shows a plot
    """
    
    custom_colors = ['BuGn', 'PuOr', 'YlGn', 'autumn', 'coolwarm', 'flare']
    
    # Randomly choose a color from the set of custom colors
    color = random.choice(custom_colors)
    
    # Re-project the geometries to Australian Albers Equal Area (EPSG:3577)
    data = data.to_crs(epsg=3577)
    
    fig, ax = plt.subplots(figsize=(12, 12))
    data.plot(column=column_name, cmap=color, linewidth=0.8, ax=ax, edgecolor='0.8', legend=True, legend_kwds={'label': column_name})
    
    # Create text annotation for each state
    for x, y, state, sa2_code, label in zip(data.geometry.centroid.x, data.geometry.centroid.y, data["state"], data["sa2_code"], data[column_name]):
        conditions = [
            (state == 'NT' and sa2_code == '702011054'),
            (state == 'SA' and sa2_code == '406011132'),
            (state == 'NSW' and sa2_code == '105011093'),
            (state == 'VIC' and sa2_code == '206071517'),
            (state == 'TAS' and sa2_code == '603011065'),
            (state == 'ACT' and sa2_code == '801071082'),
            (state == 'WA' and sa2_code == '510021267'),
            (state == 'QLD' and sa2_code == '315021407')
        ]
        if any(conditions):
            text = f'{state}: {label}'  # Add state name to the text
            ax.annotate(
                text=text,
                xy=(x, y),
                xytext=(3, 3),  # Adjust the position of the text
                textcoords="offset points",
                fontsize=10,
                ha='center', va='center',  # Center the text
                bbox=dict(boxstyle='round,pad=0.5', edgecolor='black', facecolor='white', alpha=0.7)
            )

    # Customize plot settings
    ax.set_title(title, fontdict={'fontsize': '15', 'fontweight': 'bold'})

    # Save the plot (replace 'path_to_save_plot.png' with your desired path)
    # plt.savefig('path_to_save_plot.png', dpi=300, bbox_inches='tight')
    plt.show()