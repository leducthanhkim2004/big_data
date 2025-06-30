import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import os
import warnings
warnings.filterwarnings('ignore')

# Set plotting style
plt.style.use('default')
sns.set_palette("husl")

input_folder = r"D:\feb_to_apr"
output_folder = r"D:\eda_results"

# Create output directory
os.makedirs(output_folder, exist_ok=True)

# Define CSV files in dataset
dataset_files = {
    "adets": "adets.csv",
    "alogs": "alogs.csv",
    "cdets": "cdets.csv",
    "ddets": "ddets.csv",
    "pdets": "pdets.csv",
    "plogs": "plogs.csv",
    "sdets": "sdets.csv",
    "slogs": "slogs.csv",
    "tdets": "tdets.csv",
    "tlogs": "tlogs.csv"
}

def quick_eda_pandas(file_path, file_name, sample_size=1000):
    """Quick EDA function using regular pandas (much more reliable)"""
    print(f"\n{'='*60}")
    print(f"📊 Analyzing: {file_name}")
    print(f"{'='*60}")
    
    try:
        # Load only first 1000 rows for speed using regular pandas
        df = pd.read_csv(file_path, nrows=sample_size)
        print(f"✓ Loaded {len(df)} rows (limited to {sample_size} for speed)")
        
        # Basic info
        print(f"\n📋 Dataset Overview:")
        print(f"  Shape: {df.shape}")
        print(f"  Memory Usage: {df.memory_usage(deep=True).sum() / 1024:.1f} KB")
        
        # Missing values summary
        missing_summary = df.isnull().sum()
        missing_pct = (missing_summary / len(df)) * 100
        
        print(f"\n🔍 Data Quality:")
        print(f"  Total missing values: {missing_summary.sum()}")
        print(f"  Columns with missing data: {(missing_summary > 0).sum()}")
        
        if missing_summary.sum() > 0:
            print("  Top missing columns:")
            missing_df = pd.DataFrame({
                'Column': missing_summary.index,
                'Missing': missing_summary.values,
                'Percent': missing_pct.values
            }).sort_values('Missing', ascending=False)
            print(missing_df[missing_df['Missing'] > 0].head().to_string(index=False))
        
        # Identify column types
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
        
        # Remove ID columns
        id_cols = [col for col in df.columns if 'id' in col.lower() or col.lower().endswith('_id')]
        numeric_cols = [col for col in numeric_cols if col not in id_cols]
        
        print(f"\n📊 Column Types:")
        print(f"  Numeric: {len(numeric_cols)} columns")
        print(f"  Categorical: {len(categorical_cols)} columns")
        print(f"  ID columns (excluded): {len(id_cols)}")
        
        # Show first few column names
        if numeric_cols:
            print(f"  Numeric cols: {numeric_cols[:5]}{'...' if len(numeric_cols) > 5 else ''}")
        if categorical_cols:
            print(f"  Categorical cols: {categorical_cols[:5]}{'...' if len(categorical_cols) > 5 else ''}")
        
        # Quick numeric analysis
        if numeric_cols:
            print(f"\n📈 Numeric Summary (top 5 columns):")
            top_numeric = numeric_cols[:5]
            
            try:
                summary_stats = df[top_numeric].describe()
                print(summary_stats.round(2))
            except Exception as e:
                print(f"  Error getting summary stats: {str(e)[:50]}")
                # Fallback: basic stats
                for col in top_numeric[:3]:
                    try:
                        mean_val = df[col].mean()
                        std_val = df[col].std()
                        min_val = df[col].min()
                        max_val = df[col].max()
                        print(f"  {col}: mean={mean_val:.2f}, std={std_val:.2f}, min={min_val:.2f}, max={max_val:.2f}")
                    except:
                        print(f"  {col}: Unable to calculate stats")
            
            # Quick correlation for top numeric columns
            if len(top_numeric) > 1:
                try:
                    corr_matrix = df[top_numeric].corr()
                    
                    # Find highest correlations
                    high_corr = []
                    for i in range(len(corr_matrix.columns)):
                        for j in range(i+1, len(corr_matrix.columns)):
                            corr_val = corr_matrix.iloc[i, j]
                            if abs(corr_val) > 0.5:
                                high_corr.append((corr_matrix.columns[i], corr_matrix.columns[j], corr_val))
                    
                    if high_corr:
                        print(f"\n🔗 High Correlations (|r| > 0.5):")
                        for col1, col2, corr in sorted(high_corr, key=lambda x: abs(x[2]), reverse=True):
                            print(f"  {col1} ↔ {col2}: {corr:.3f}")
                    
                    # Quick correlation heatmap (max 6x6)
                    if len(top_numeric) <= 6:
                        plt.figure(figsize=(8, 6))
                        sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', center=0, fmt='.2f', square=True)
                        plt.title(f'Correlation Heatmap - {file_name}')
                        plt.tight_layout()
                        plt.savefig(os.path.join(output_folder, f'{file_name}_correlation.png'), dpi=150, bbox_inches='tight')
                        plt.close()  # Close to save memory
                        
                except Exception as e:
                    print(f"  Error in correlation analysis: {str(e)[:50]}")
            
            # Quick distribution plots (max 4 columns)
            plot_cols = top_numeric[:4]
            if plot_cols:
                try:
                    fig, axes = plt.subplots(2, 2, figsize=(12, 8))
                    axes = axes.flatten()
                    
                    plots_created = 0
                    for i, col in enumerate(plot_cols):
                        try:
                            data = df[col].dropna()
                            
                            if len(data) > 0:
                                sns.histplot(data, kde=True, ax=axes[i], stat='density')
                                axes[i].set_title(f'{col}')
                                
                                # Add basic stats to plot
                                mean_val = data.mean()
                                axes[i].axvline(mean_val, color='red', linestyle='--', alpha=0.7, label=f'Mean: {mean_val:.2f}')
                                axes[i].legend()
                                plots_created += 1
                        except Exception as e:
                            axes[i].text(0.5, 0.5, f'Error plotting {col}', ha='center', va='center', transform=axes[i].transAxes)
                    
                    # Hide empty subplots
                    for i in range(plots_created, 4):
                        axes[i].set_visible(False)
                    
                    plt.suptitle(f'Distributions - {file_name}', fontsize=14)
                    plt.tight_layout()
                    plt.savefig(os.path.join(output_folder, f'{file_name}_distributions.png'), dpi=150, bbox_inches='tight')
                    plt.close()
                    
                except Exception as e:
                    print(f"  Error creating distribution plots: {str(e)[:50]}")
        
        # Quick categorical analysis
        if categorical_cols:
            print(f"\n📊 Categorical Summary (top 3 columns):")
            for col in categorical_cols[:3]:
                try:
                    unique_count = df[col].nunique()
                    top_values = df[col].value_counts().head(5)
                    print(f"\n  {col}:")
                    print(f"    Unique values: {unique_count}")
                    print(f"    Top 5 values:")
                    for val, count in top_values.items():
                        pct = (count / len(df)) * 100
                        print(f"      {str(val)[:20]:22}: {count:4} ({pct:4.1f}%)")
                except Exception as e:
                    print(f"    Error analyzing {col}: {str(e)[:30]}")
        
        # Quick insights
        print(f"\n💡 Quick Insights:")
        insights = []
        
        # Data quality insights
        try:
            if missing_summary.sum() > 0:
                insights.append(f"Missing data in {(missing_summary > 0).sum()} columns")
        except:
            pass
        
        # Numeric insights
        if numeric_cols:
            try:
                # Check for potential outliers
                outlier_cols = []
                for col in numeric_cols[:5]:
                    try:
                        data = df[col].dropna()
                        if len(data) > 0:
                            Q1, Q3 = data.quantile([0.25, 0.75])
                            IQR = Q3 - Q1
                            if IQR > 0:  # Avoid division by zero
                                outliers = data[(data < Q1 - 1.5*IQR) | (data > Q3 + 1.5*IQR)]
                                if len(outliers) > len(data) * 0.1:  # More than 10% outliers
                                    outlier_cols.append(col)
                    except:
                        continue
                
                if outlier_cols:
                    insights.append(f"Potential outliers in: {', '.join(outlier_cols[:3])}")
                
                # Check for skewed distributions
                skewed_cols = []
                for col in numeric_cols[:5]:
                    try:
                        skew = df[col].skew()
                        if abs(skew) > 1:
                            skewed_cols.append(f"{col} ({skew:.1f})")
                    except:
                        continue
                
                if skewed_cols:
                    insights.append(f"Highly skewed: {', '.join(skewed_cols[:3])}")
                    
            except Exception as e:
                insights.append(f"Error in numeric analysis: {str(e)[:50]}")
        
        # Categorical insights
        if categorical_cols:
            try:
                high_card_cols = []
                for col in categorical_cols[:5]:
                    try:
                        unique_count = df[col].nunique()
                        if unique_count > len(df) * 0.8:  # High cardinality
                            high_card_cols.append(f"{col} ({unique_count})")
                    except:
                        continue
                
                if high_card_cols:
                    insights.append(f"High cardinality: {', '.join(high_card_cols[:2])}")
            except:
                pass
        
        # Duplicate check
        try:
            duplicates = df.duplicated().sum()
            if duplicates > 0:
                insights.append(f"Duplicate rows: {duplicates} ({duplicates/len(df)*100:.1f}%)")
        except:
            pass
        
        if insights:
            for i, insight in enumerate(insights, 1):
                print(f"  {i}. {insight}")
        else:
            print("  ✅ Data looks clean and ready for analysis!")
        
        print(f"\n✅ Analysis complete for {file_name}")
        
        return {
            'file_name': file_name,
            'rows_analyzed': len(df),
            'total_columns': len(df.columns),
            'numeric_columns': len(numeric_cols),
            'categorical_columns': len(categorical_cols),
            'missing_values': int(missing_summary.sum()),
            'insights': len(insights),
            'status': 'success'
        }
        
    except Exception as e:
        error_msg = str(e)[:100]
        print(f"❌ Error analyzing {file_name}: {error_msg}")
        return {
            'file_name': file_name,
            'status': 'failed',
            'error': error_msg
        }

def analyze_all_files():
    """Analyze all files quickly with 1000-row limit using pandas"""
    print(f"🚀 QUICK EDA FOR ALL FILES (1000 rows each) - Using Regular Pandas")
    print(f"Input: {input_folder}")
    print(f"Output: {output_folder}")
    print("="*80)
    
    results = []
    
    # Process each file
    for key, filename in dataset_files.items():
        file_path = os.path.join(input_folder, filename)
        
        if os.path.exists(file_path):
            result = quick_eda_pandas(file_path, key)
            results.append(result)
        else:
            print(f"⚠️  File not found: {filename}")
            results.append({
                'file_name': key,
                'status': 'not_found',
                'error': 'File not found'
            })
    
    # Summary report
    print(f"\n" + "="*80)
    print(f"📋 SUMMARY REPORT")
    print("="*80)
    
    successful = [r for r in results if r.get('status') == 'success']
    failed = [r for r in results if r.get('status') != 'success']
    
    print(f"✅ Successfully analyzed: {len(successful)}/{len(results)} files")
    print(f"❌ Failed: {len(failed)} files")
    
    if successful:
        print(f"\n📊 Dataset Overview:")
        print("File".ljust(12), "Rows".ljust(8), "Cols".ljust(6), "Numeric".ljust(8), "Categorical".ljust(12), "Missing".ljust(8), "Insights")
        print("-" * 70)
        
        total_rows = 0
        total_missing = 0
        total_cols = 0
        total_numeric = 0
        total_categorical = 0
        
        for r in successful:
            print(f"{r['file_name']:12} {r['rows_analyzed']:8} {r['total_columns']:6} {r['numeric_columns']:8} {r['categorical_columns']:12} {r['missing_values']:8} {r['insights']}")
            total_rows += r['rows_analyzed']
            total_missing += r['missing_values']
            total_cols += r['total_columns']
            total_numeric += r['numeric_columns']
            total_categorical += r['categorical_columns']
        
        print("-" * 70)
        print(f"{'TOTAL':12} {total_rows:8} {total_cols:6} {total_numeric:8} {total_categorical:12} {total_missing:8}")
        
        # Additional summary statistics
        print(f"\n📈 Overall Summary:")
        print(f"  Average columns per file: {total_cols/len(successful):.1f}")
        print(f"  Average numeric columns: {total_numeric/len(successful):.1f}")
        print(f"  Average categorical columns: {total_categorical/len(successful):.1f}")
        print(f"  Total missing data points: {total_missing}")
        print(f"  Missing data percentage: {(total_missing/(total_rows*total_cols/len(successful)))*100:.2f}%")
    
    if failed:
        print(f"\n❌ Failed files:")
        for r in failed:
            print(f"  {r['file_name']}: {r.get('error', 'Unknown error')}")
    
    # Save summary with error handling
    try:
        summary_df = pd.DataFrame(results)
        summary_path = os.path.join(output_folder, 'quick_eda_summary.csv')
        summary_df.to_csv(summary_path, index=False)
        print(f"\n📁 Results saved to: {output_folder}")
        print(f"📄 Summary report: {summary_path}")
        
        # Also save a detailed summary
        if successful:
            detailed_summary = {
                'total_files_analyzed': len(successful),
                'total_rows_analyzed': total_rows,
                'total_columns': total_cols,
                'total_numeric_columns': total_numeric,
                'total_categorical_columns': total_categorical,
                'total_missing_values': total_missing,
                'average_columns_per_file': total_cols/len(successful),
                'files_with_missing_data': len([r for r in successful if r['missing_values'] > 0]),
                'analysis_timestamp': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            detailed_df = pd.DataFrame([detailed_summary])
            detailed_path = os.path.join(output_folder, 'detailed_summary.csv')
            detailed_df.to_csv(detailed_path, index=False)
            print(f"📄 Detailed summary: {detailed_path}")
        
    except Exception as e:
        print(f"⚠️  Error saving summary: {str(e)[:50]}")
    
    print(f"\n🎉 Quick EDA Complete!")
    print(f"📊 Check {output_folder} for visualizations and reports")

# Alternative function for even faster analysis (just basic stats, no plots)
def ultra_quick_analysis():
    """Ultra-fast analysis with just basic statistics"""
    print(f"⚡ ULTRA-QUICK ANALYSIS (Basic Stats Only)")
    print("="*60)
    
    results = []
    
    for key, filename in dataset_files.items():
        file_path = os.path.join(input_folder, filename)
        
        if os.path.exists(file_path):
            try:
                # Just read first few rows to get basic info
                df_sample = pd.read_csv(file_path, nrows=100)
                
                # Basic info only
                result = {
                    'file_name': key,
                    'sample_rows': len(df_sample),
                    'total_columns': len(df_sample.columns),
                    'numeric_columns': len(df_sample.select_dtypes(include=[np.number]).columns),
                    'categorical_columns': len(df_sample.select_dtypes(include=['object']).columns),
                    'missing_in_sample': df_sample.isnull().sum().sum(),
                    'status': 'success'
                }
                
                print(f"{key:12} | Cols: {result['total_columns']:3} | Numeric: {result['numeric_columns']:3} | Categorical: {result['categorical_columns']:3} | Missing: {result['missing_in_sample']:3}")
                results.append(result)
                
            except Exception as e:
                print(f"{key:12} | ERROR: {str(e)[:40]}")
                results.append({'file_name': key, 'status': 'failed', 'error': str(e)[:50]})
        else:
            print(f"{key:12} | FILE NOT FOUND")
    
    return results

# Main execution
if __name__ == "__main__":
    # Choose your analysis level:
    
    # Option 1: Full analysis with plots (takes 2-3 minutes)
    analyze_all_files()
    
    # Option 2: Ultra-quick analysis (takes 10-15 seconds)
    # ultra_quick_analysis()