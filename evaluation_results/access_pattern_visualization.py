import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Paths to your CSV files
frequency_file = 'frequency_metrics.csv'
sequential_file = 'sequential_metrics.csv'
range_file = 'range_metrics.csv'

# Read CSV files
frequency_df = pd.read_csv(frequency_file)
sequential_df = pd.read_csv(sequential_file)
range_df = pd.read_csv(range_file)

# Function to calculate summary statistics
def calculate_summary(df, pattern_type):
    summary = df.groupby('Implementation_DataType')['Average_Time_ms'].agg(['mean', 'min', 'max', 'std'])
    summary['Pattern_Type'] = pattern_type
    return summary

# Calculate summaries for each access pattern
frequency_summary = calculate_summary(frequency_df, 'Frequency')
sequential_summary = calculate_summary(sequential_df, 'Sequential')
range_summary = calculate_summary(range_df, 'Range')

# Combine all summaries
all_summaries = pd.concat([frequency_summary, sequential_summary, range_summary])
print("Summary statistics for each access pattern:\n", all_summaries)

# Function to save latency plot for each pattern to its own PNG file
def save_pattern_plot(df, pattern_name):
    plt.figure(figsize=(12, 6))
    sns.barplot(x='Implementation_DataType', y='Average_Time_ms', data=df, errorbar='sd')
    plt.title(f'{pattern_name} Access Pattern Latency Comparison')
    plt.xticks(rotation=45, ha='right')
    plt.ylabel('Average Time (ms)')
    plt.xlabel('Implementation and DataType')
    plt.tight_layout()
    file_name = f'{pattern_name.lower()}_access_pattern.png'
    plt.savefig(file_name)
    plt.close()
    print(f"{pattern_name} access pattern plot saved as '{file_name}'")

# Generate plots for each access pattern
save_pattern_plot(frequency_df, 'Frequency')
save_pattern_plot(sequential_df, 'Sequential')
save_pattern_plot(range_df, 'Range')

# Export combined summaries to a CSV file in the same folder as the Python script
all_summaries.to_csv('access_pattern_summary.csv', index=True)
print("Access pattern summary saved as 'access_pattern_summary.csv'")
