import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Set style for all plots
plt.style.use('seaborn-v0_8')
plt.rcParams['figure.figsize'] = [12, 6]
plt.rcParams['figure.dpi'] = 100

def analyze_scalability(file_path='scalability_test_results.csv'):
    # Read data
    print("Reading data...")
    df = pd.read_csv(file_path)

    # Ensure that data is correctly read
    if df.empty:
        print("No data found in the CSV file.")
        return

    # Convert metrics to numeric, matching the exact column names in the CSV
    metrics = [
        'TotalTime(ms)', 'AvgTimePerOp(ms)', 'MinLatency(ms)', 
        'MaxLatency(ms)', '50thPercentile(ms)', '90thPercentile(ms)', 
        'MemoryOverhead(MB)'
    ]
    for metric in metrics:
        df[metric] = pd.to_numeric(df[metric], errors='coerce')

    # Plot total time taken for each implementation across row counts
    plot_total_time(df)
    # Plot average latency per operation
    plot_avg_time_per_op(df)
    # Compare latencies across implementations at 50th and 90th percentiles
    plot_percentiles(df)
    # Plot memory usage
    plot_memory_usage(df)
    # Generate a summary of performance across implementations
    print_summary_statistics(df)

    print("\nAnalysis completed successfully!")
    print("\nGenerated files:")
    print("1. scalability_total_time.png - Total time taken by each implementation across row counts")
    print("2. scalability_avg_time_per_op.png - Average latency per operation across implementations")
    print("3. scalability_percentiles.png - 50th and 90th percentile latencies")
    print("4. scalability_memory_usage.png - Memory usage across implementations")
    print("5. scalability_summary.txt - Summary of best implementations per row count")

def plot_total_time(df):
    print("Creating total time plot for scalability...")
    plt.figure(figsize=(12, 6))
    sns.lineplot(data=df, x='RowCount', y='TotalTime(ms)', hue='Implementation', marker='o')
    plt.title('Total Time Taken for Different Implementations')
    plt.xlabel('Rows Tested')
    plt.ylabel('Total Time (ms)')
    plt.legend(title='Implementation')
    plt.tight_layout()
    plt.savefig('scalability_total_time.png')
    plt.close()

def plot_avg_time_per_op(df):
    print("Creating average time per operation plot...")
    plt.figure(figsize=(12, 6))
    sns.lineplot(data=df, x='RowCount', y='AvgTimePerOp(ms)', hue='Implementation', marker='o')
    plt.title('Average Time per Operation by Implementation')
    plt.xlabel('Rows Tested')
    plt.ylabel('Avg Time per Operation (ms)')
    plt.legend(title='Implementation')
    plt.tight_layout()
    plt.savefig('scalability_avg_time_per_op.png')
    plt.close()

def plot_percentiles(df):
    print("Creating percentile comparison plot...")
    plt.figure(figsize=(12, 6))

    # Plot 50th percentile
    sns.lineplot(data=df, x='RowCount', y='50thPercentile(ms)', hue='Implementation', marker='o', linestyle='-')
    
    # Plot 90th percentile
    sns.lineplot(data=df, x='RowCount', y='90thPercentile(ms)', hue='Implementation', marker='o', linestyle='--')

    plt.title('Latency Percentiles (50th and 90th) by Implementation')
    plt.xlabel('Rows Tested')
    plt.ylabel('Latency (ms)')
    plt.legend(title='Implementation')
    plt.tight_layout()
    plt.savefig('scalability_percentiles.png')
    plt.close()

def plot_memory_usage(df):
    print("Creating memory usage plot...")
    plt.figure(figsize=(12, 6))
    sns.lineplot(data=df, x='RowCount', y='MemoryOverhead(MB)', hue='Implementation', marker='o')
    plt.title('Memory Overhead by Implementation')
    plt.xlabel('Rows Tested')
    plt.ylabel('Memory Overhead (MB)')
    plt.legend(title='Implementation')
    plt.tight_layout()
    plt.savefig('scalability_memory_usage.png')
    plt.close()

def print_summary_statistics(df):
    print("\nSUMMARY STATISTICS")
    print("=" * 50)

    # Best performing implementation for each row count based on average time per operation
    print("\nBest Implementation per Row Count (Avg Time per Operation):")
    print("-" * 40)

    row_counts = sorted(df['RowCount'].unique())
    summary_lines = []

    with open('scalability_summary.txt', 'w') as f:
        f.write("Scalability Test Summary\n")
        f.write("=" * 50 + "\n\n")
        f.write("Best Implementation per Row Count (Avg Time per Operation):\n")
        f.write("-" * 40 + "\n")
        for row_count in row_counts:
            subset = df[df['RowCount'] == row_count]
            best_impl = subset.loc[subset['AvgTimePerOp(ms)'].idxmin()]
            summary_line = f"Rows: {row_count} - Best Implementation: {best_impl['Implementation']} ({best_impl['AvgTimePerOp(ms)']:.3f} ms)\n"
            print(summary_line.strip())
            f.write(summary_line)
            summary_lines.append(summary_line)

        # Memory usage summary
        f.write("\n\nMemory Usage Summary:\n")
        f.write("-" * 40 + "\n")

        if 'MemoryOverhead(MB)' in df.columns:
            memory_summary = df.groupby('Implementation')['MemoryOverhead(MB)'].mean()
            for impl, avg_mem in memory_summary.items():
                mem_line = f"{impl}: Average Memory Overhead: {avg_mem:.2f} MB\n"
                print(mem_line.strip())
                f.write(mem_line)
                summary_lines.append(mem_line)
        else:
            no_memory_data = "Memory usage data is not available in the dataset.\n"
            print(no_memory_data.strip())
            f.write(no_memory_data)
            summary_lines.append(no_memory_data)

def main():
    try:
        analyze_scalability()
    except Exception as e:
        print(f"Error during analysis: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
