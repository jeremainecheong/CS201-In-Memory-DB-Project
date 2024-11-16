import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# Set style for all plots
plt.style.use('seaborn-v0_8')
plt.rcParams['figure.figsize'] = [12, 6]
plt.rcParams['figure.dpi'] = 100

def analyze_csv(file_path='summary_statistics.csv'):
    # Read data
    print("Reading data...")
    df = pd.read_csv(file_path)

    # Extract implementation and operation performance data
    operations_data = df[df['Category'] == 'Operation'].copy()
    memory_data = df[df['Category'] == 'Memory'].copy()

    # Split Best_Implementation into Implementation and DataType
    operations_data[['Implementation', 'DataType']] = operations_data['Best_Implementation'].str.split('__', expand=True)
    memory_data[['Implementation', 'DataType']] = memory_data['Best_Implementation'].str.split('__', expand=True)

    # Drop any rows with NaN values
    operations_data = operations_data.dropna(subset=['Implementation', 'DataType', 'Metric', 'Average_Time_ms'])
    memory_data = memory_data.dropna(subset=['Implementation', 'DataType', 'Average_Time_ms'])

    # Create visualizations and summaries
    create_operation_performance_plot(operations_data)          # Generates operation_performance.png
    create_memory_usage_plot(memory_data)                       # Generates memory_usage.png
    create_data_type_comparison(operations_data)                # Generates data_type_performance.png
    print_summary_statistics(operations_data, memory_data)       # Generates performance_summary.txt
    analyze_operation_patterns(operations_data)                  # Generates implementation_patterns.png
    analyze_implementation_characteristics(operations_data)     # Generates implementation_characteristics.png
    print_implementation_analysis(operations_data)               # Generates implementation_analysis.txt
    plot_latency_distribution(operations_data)                   # Generates latency_distribution.png
    analyze_conditional_queries(operations_data)                 # Generates conditional_query_performance.png

    # Check if 'Data_Size' column exists before plotting trend
    if 'Data_Size' in operations_data.columns:
        plot_trend_over_data_sizes(operations_data)              # Generates performance_trend_over_data_sizes.png
    else:
        print("Data_Size column missing. Skipping performance_trend_over_data_sizes.png generation.")

    generate_statistical_summary(operations_data)                # Generates statistical_summary.csv

    print("\nAnalysis completed successfully!")
    print("\nGenerated files:")
    print("1. operation_performance.png - Operation performance heatmap")
    print("2. memory_usage.png - Memory usage patterns")
    print("3. data_type_performance.png - Performance across different data types")
    print("4. performance_summary.txt - Summary statistics of performance and memory usage")
    print("5. implementation_patterns.png - Basic vs Complex operation patterns")
    print("6. implementation_characteristics.png - Implementation characteristics comparison")
    print("7. implementation_analysis.txt - Detailed implementation analysis")
    print("8. latency_distribution.png - Latency distribution by operation and implementation")
    print("9. conditional_query_performance.png - Performance in conditional query operations")
    print("10. performance_trend_over_data_sizes.png - Operation performance trends over data sizes")
    print("11. statistical_summary.csv - Comprehensive statistical summary")

def create_operation_performance_plot(df):
    print("Creating operation performance plot...")
    # Average performance by implementation and operation type
    performance_data = df.pivot_table(
        values='Average_Time_ms',
        index='Implementation',
        columns='Metric',
        aggfunc='mean'
    ).fillna(0)

    plt.figure(figsize=(15, 8))
    sns.heatmap(performance_data,
                annot=True,
                fmt='.3f',
                cmap='YlOrRd')
    plt.title('Operation Performance by Implementation (Lower is Better)')
    plt.ylabel('Implementation')
    plt.xlabel('Operation')
    plt.tight_layout()
    plt.savefig('operation_performance.png')
    plt.close()

def create_memory_usage_plot(df):
    print("Creating memory usage plot...")
    # Average memory usage by implementation and data type
    plt.figure(figsize=(15, 8))
    ax = sns.barplot(data=df,
                     x='DataType',
                     y='Average_Time_ms',  # This represents MB in memory data
                     hue='Implementation')
    plt.title('Memory Usage by Implementation and Data Type')
    plt.xlabel('Data Type')
    plt.ylabel('Memory Usage (MB)')
    plt.xticks(rotation=45)
    plt.legend(title='Implementation', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.savefig('memory_usage.png')
    plt.close()

def create_data_type_comparison(df):
    print("Creating data type comparison plot...")
    # Average performance by data type for each operation
    data_type_perf = df.pivot_table(
        values='Average_Time_ms',
        index='DataType',
        columns='Metric',
        aggfunc='mean'
    ).fillna(0)

    plt.figure(figsize=(15, 8))
    sns.heatmap(data_type_perf,
                annot=True,
                fmt='.3f',
                cmap='YlOrRd')
    plt.title('Operation Performance by Data Type (Lower is Better)')
    plt.ylabel('Data Type')
    plt.xlabel('Operation')
    plt.tight_layout()
    plt.savefig('data_type_performance.png')
    plt.close()

def print_summary_statistics(op_data, mem_data):
    print("\nSUMMARY STATISTICS")
    print("=" * 50)

    # Best performing implementation for each operation
    print("\nBest Implementation per Operation:")
    print("-" * 40)

    for metric in sorted(op_data['Metric'].unique()):
        subset = op_data[op_data['Metric'] == metric]
        if not subset.empty:
            min_idx = subset['Average_Time_ms'].idxmin()
            if pd.notna(min_idx):
                best_row = subset.loc[min_idx]
                print(f"{metric:22}: {best_row['Implementation']} ({best_row['Average_Time_ms']:.3f} ms)")

    # Memory usage summary
    print("\nMemory Usage Summary:")
    print("-" * 40)

    # Calculate mean memory usage for each implementation
    mem_summary = mem_data.groupby('Implementation')['Average_Time_ms'].mean().round(2)

    for impl in sorted(mem_summary.index):
        if pd.notna(mem_summary[impl]):
            print(f"{impl:22}: {mem_summary[impl]:.2f} MB")

    # Create summary text file
    with open('performance_summary.txt', 'w') as f:
        f.write("Performance Summary\n")
        f.write("=" * 50 + "\n\n")

        f.write("Best Implementation per Operation:\n")
        f.write("-" * 40 + "\n")
        for metric in sorted(op_data['Metric'].unique()):
            subset = op_data[op_data['Metric'] == metric]
            if not subset.empty:
                min_idx = subset['Average_Time_ms'].idxmin()
                if pd.notna(min_idx):
                    best_row = subset.loc[min_idx]
                    f.write(f"{metric:22}: {best_row['Implementation']} ({best_row['Average_Time_ms']:.3f} ms)\n")

        f.write("\nMemory Usage Summary:\n")
        f.write("-" * 40 + "\n")
        for impl in sorted(mem_summary.index):
            if pd.notna(mem_summary[impl]):
                f.write(f"{impl:22}: {mem_summary[impl]:.2f} MB\n")

def analyze_operation_patterns(df):
    print("Analyzing operation patterns...")

    # Group operations by type
    basic_ops = ['SELECT', 'INSERT', 'UPDATE', 'DELETE']
    complex_ops = ['COMPLEX_SELECT', 'COMPLEX_UPDATE', 'COMPLEX_DELETE']

    # Calculate average performance for basic vs complex operations
    plt.figure(figsize=(15, 6))

    # Basic operations subplot
    plt.subplot(1, 2, 1)
    basic_data = df[df['Metric'].isin(basic_ops)].pivot_table(
        values='Average_Time_ms',
        index='Implementation',
        columns='Metric',
        aggfunc='mean'
    ).fillna(0)

    sns.heatmap(basic_data,
                annot=True,
                fmt='.3f',
                cmap='YlOrRd')
    plt.title('Basic Operation Performance\n(Lower is Better)')

    # Complex operations subplot
    plt.subplot(1, 2, 2)
    complex_data = df[df['Metric'].isin(complex_ops)].pivot_table(
        values='Average_Time_ms',
        index='Implementation',
        columns='Metric',
        aggfunc='mean'
    ).fillna(0)

    sns.heatmap(complex_data,
                annot=True,
                fmt='.3f',
                cmap='YlOrRd')
    plt.title('Complex Operation Performance\n(Lower is Better)')

    plt.tight_layout()
    plt.savefig('implementation_patterns.png')
    plt.close()

def analyze_implementation_characteristics(df):
    print("Analyzing implementation characteristics...")

    # Calculate metrics that highlight implementation differences
    characteristics = pd.DataFrame()

    for impl in df['Implementation'].unique():
        impl_data = df[df['Implementation'] == impl]

        # Calculate various characteristics
        characteristics.loc[impl, 'Avg Basic Op Time'] = impl_data[
            impl_data['Metric'].isin(['SELECT', 'INSERT', 'UPDATE', 'DELETE'])
        ]['Average_Time_ms'].mean()

        characteristics.loc[impl, 'Avg Complex Op Time'] = impl_data[
            impl_data['Metric'].str.startswith('COMPLEX_')
        ]['Average_Time_ms'].mean()

        characteristics.loc[impl, 'Write Performance'] = impl_data[
            impl_data['Metric'].isin(['INSERT', 'UPDATE'])
        ]['Average_Time_ms'].mean()

        characteristics.loc[impl, 'Read Performance'] = impl_data[
            impl_data['Metric'].isin(['SELECT', 'COMPLEX_SELECT'])
        ]['Average_Time_ms'].mean()

        characteristics.loc[impl, 'Operation Consistency'] = impl_data['Average_Time_ms'].std()

    # Create visualization of implementation characteristics
    plt.figure(figsize=(15, 8))
    sns.heatmap(characteristics,
                annot=True,
                fmt='.3f',
                cmap='YlOrRd')
    plt.title('Implementation Characteristics\n(Lower is Better)')
    plt.tight_layout()
    plt.savefig('implementation_characteristics.png')
    plt.close()

def print_implementation_analysis(df):
    print("\nIMPLEMENTATION ANALYSIS")
    print("=" * 50)

    implementations = sorted(df['Implementation'].unique())

    with open('implementation_analysis.txt', 'w') as f:
        f.write("Implementation Analysis\n")
        f.write("=" * 50 + "\n\n")

        for impl in implementations:
            impl_data = df[df['Implementation'] == impl]

            # Calculate key metrics
            basic_perf = impl_data[
                impl_data['Metric'].isin(['SELECT', 'INSERT', 'UPDATE', 'DELETE'])
            ]['Average_Time_ms'].mean()

            complex_perf = impl_data[
                impl_data['Metric'].str.startswith('COMPLEX_')
            ]['Average_Time_ms'].mean()

            write_perf = impl_data[
                impl_data['Metric'].isin(['INSERT', 'UPDATE'])
            ]['Average_Time_ms'].mean()

            read_perf = impl_data[
                impl_data['Metric'].isin(['SELECT', 'COMPLEX_SELECT'])
            ]['Average_Time_ms'].mean()

            consistency = impl_data['Average_Time_ms'].std()

            # Write analysis
            analysis = f"\n{impl} Implementation:\n"
            analysis += "-" * 40 + "\n"
            analysis += f"Basic Operations Avg: {basic_perf:.3f} ms\n"
            analysis += f"Complex Operations Avg: {complex_perf:.3f} ms\n"
            analysis += f"Write Performance: {write_perf:.3f} ms\n"
            analysis += f"Read Performance: {read_perf:.3f} ms\n"
            analysis += f"Operation Consistency: {consistency:.3f} (lower is better)\n"

            # Add implementation-specific insights
            if impl == 'backwards':
                analysis += "\nKey Characteristics:\n"
                analysis += "- Stack-based implementation with batch processing\n"
                analysis += "- Sequential access pattern\n"
                analysis += "- Efficient for recent data access\n"
            elif impl == 'leaky':
                analysis += "\nKey Characteristics:\n"
                analysis += "- Two-tier storage with controlled data movement\n"
                analysis += "- Automatic memory management\n"
                analysis += "- Efficient for hot data access\n"
            elif impl == 'ping':
                analysis += "\nKey Characteristics:\n"
                analysis += "- Dual-list system with frequency-based promotion\n"
                analysis += "- Adaptive to access patterns\n"
                analysis += "- Good for mixed workloads\n"
            elif impl == 'random':
                analysis += "\nKey Characteristics:\n"
                analysis += "- Multiple queue distribution\n"
                analysis += "- Hash-based allocation\n"
                analysis += "- Good for parallel access patterns\n"

            print(analysis)
            f.write(analysis + "\n")

def plot_latency_distribution(df):
    plt.figure(figsize=(15, 8))
    sns.boxplot(data=df, x='Metric', y='Average_Time_ms', hue='Implementation')
    plt.title('Latency Distribution by Operation and Implementation')
    plt.xlabel('Operation')
    plt.ylabel('Latency (ms)')
    plt.legend(title='Implementation')
    plt.tight_layout()
    plt.savefig('latency_distribution.png')
    plt.close()

def analyze_conditional_queries(df):
    conditions = ['BOOLEAN_CONDITION', 'GREATER_THAN_CONDITION', 'DATE_CONDITION', 'UNKNOWN_CONDITION']
    cond_df = df[df['Metric'].isin(conditions)]
    cond_heatmap = cond_df.pivot_table(
        values='Average_Time_ms',
        index='Implementation',
        columns='Metric',
        aggfunc='mean'
    )
    plt.figure(figsize=(10, 6))
    sns.heatmap(cond_heatmap, annot=True, cmap='YlOrRd', fmt='.3f')
    plt.title('Performance of Conditional Queries by Implementation')
    plt.xlabel('Condition Type')
    plt.ylabel('Implementation')
    plt.tight_layout()
    plt.savefig('conditional_query_performance.png')
    plt.close()

def plot_trend_over_data_sizes(df):
    plt.figure(figsize=(15, 8))
    sns.lineplot(data=df, x='Data_Size', y='Average_Time_ms', hue='Implementation', style='Metric')
    plt.title('Trend of Operation Performance Over Data Sizes')
    plt.xlabel('Data Size')
    plt.ylabel('Average Latency (ms)')
    plt.legend(title='Implementation & Metric')
    plt.tight_layout()
    plt.savefig('performance_trend_over_data_sizes.png')
    plt.close()

def generate_statistical_summary(df):
    summary = df.groupby(['Implementation', 'Metric']).agg({
        'Average_Time_ms': ['mean', 'min', 'max', 'std']
    }).round(3)
    summary.columns = ['Mean', 'Min', 'Max', 'Std']
    summary.to_csv('statistical_summary.csv')
    print("Statistical summary saved to 'statistical_summary.csv'")

    # Removed the undefined 'heatmap_data' loop

def main():
    try:
        analyze_csv()
        # The analyze_csv function already prints the "Analysis completed successfully" and "Generated files" messages
    except Exception as e:
        print(f"Error during analysis: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
        main()
