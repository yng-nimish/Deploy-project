from google.colab import drive
drive.mount('/content/drive')
# Install required libraries
!pip install pandas openpyxl matplotlib seaborn

# Import necessary libraries
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Define the numbers to analyze
numbers_to_analyze = ['000', '007', '069', '156', '234', '388', '421', '555', '694', '711', '777', '888', '999']

# Load the Excel file
file_path = '/content/drive/MyDrive/TCP_Analysis/TCP_Analysed.xlsx'
excel_data = pd.ExcelFile(file_path)

# Define the sheets to analyze
target_sheets = ['Z1', 'Z3', 'Z5', 'Z7', 'Z11', 'Z13', 'Z29']

# Initialize an empty dictionary to store the frequencies for each sheet
frequency_dict = {sheet: {num: 0 for num in numbers_to_analyze} for sheet in target_sheets}

# Function to count occurrences of each number in a sheet
def count_occurrences(sheet_name):
    df = excel_data.parse(sheet_name)  # Read the sheet into a DataFrame

    # Ensure all values are strings and explicitly format to preserve leading zeros, handling NaN values
    df = df.applymap(lambda x: f"{int(x):03d}" if isinstance(x, (int, float)) and pd.notna(x) else str(x).zfill(3) if pd.notna(x) else x)

    # Debugging: Show the first few rows to verify formatting
    print(f"Sample data from sheet {sheet_name}:\n", df.head(), "\n")

    # Iterate over each number to analyze
    for num in numbers_to_analyze:
        # Count occurrences of the number in the entire sheet (exact match)
        frequency_dict[sheet_name][num] = (df == num).sum().sum()  # Check exact matches for the number

# Count occurrences for each target sheet
for sheet in target_sheets:
    count_occurrences(sheet)

# Create a DataFrame from the frequency dictionary
frequency_df = pd.DataFrame(frequency_dict)

# Plot individual bar graphs for each sheet
for sheet in target_sheets:
    plt.figure(figsize=(10, 6))
    sns.barplot(x=frequency_df.index, y=frequency_df[sheet])
    plt.title(f'Frequency of Numbers in {sheet}')
    plt.xlabel('Numbers')
    plt.ylabel('Frequency')

    # Add the frequencies as text on the bars
    for i, value in enumerate(frequency_df[sheet]):
        plt.text(i, value + 0.05, f'{value}', ha='center', va='bottom')

    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

# Plot a combined bar graph for all sheets
combined_frequencies = frequency_df.sum(axis=1)
plt.figure(figsize=(10, 6))
sns.barplot(x=combined_frequencies.index, y=combined_frequencies)
plt.title('Combined Frequency of Numbers Across All Sheets')
plt.xlabel('Numbers')
plt.ylabel('Frequency')

# Add the frequencies as text on the bars
for i, value in enumerate(combined_frequencies):
    plt.text(i, value + 0.05, f'{value}', ha='center', va='bottom')

plt.ticks(rotation=45)
plt.tight_layout()
plt.show()

# Display the frequency table
print(frequency_df)


# Function to save the DataFrame as an image
def save_table_as_image(df, filename):
    fig, ax = plt.subplots(figsize=(12, 6))  # Set the size of the figure
    ax.axis('tight')
    ax.axis('off')
    table = ax.table(cellText=df.values, colLabels=df.columns, loc='center', cellLoc='center', rowLabels=df.index)

    # Customize table appearance (optional)
    table.auto_set_font_size(False)
    table.set_fontsize(12)
    table.scale(1.5, 1.5)  # Adjust table size
    table.auto_set_column_width(col=list(range(len(df.columns))))  # Adjust column width

    # Save the table as an image
    plt.savefig(filename, bbox_inches='tight', dpi=300)  # Save as image file (300 dpi for high quality)
    plt.close()

# Assuming `frequency_df` is the DataFrame containing frequencies
# frequency_df should already have numbers as the index and sheet names as columns
output_image_path = '/content/frequency_table_image2.png'
save_table_as_image(frequency_df, output_image_path)

# Print the file path of the saved image
print(f"Table saved as image at: {output_image_path}")
