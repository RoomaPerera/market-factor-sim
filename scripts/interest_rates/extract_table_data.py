import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
from pathlib import Path

# Define paths
RAW_DIR = Path("data/interest_rates/raw")
PROCESSED_DIR = Path("data/interest_rates/processed")

html_file = RAW_DIR / "CMB_LendingAndDeposit.aspx.html"
output_file = PROCESSED_DIR / "CBSL_Interest_Rates.csv"

# Ensure processed directory exists
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# Load the HTML file
with open(html_file, "r", encoding="utf-8") as f:
    html_content = f.read()

# Parse the table
soup = BeautifulSoup(html_content, "html.parser")
table = soup.find("table", {"id": "statTB"})

if table is None:
    raise ValueError("Table with id='statTB' not found in HTML file")

# Read the HTML table into adf
df = pd.read_html(StringIO(str(table)), header=[1, 2])[0]

# Flatten multi-index columns into single-level columns
df.columns = [
    "_".join([str(c) for c in col if c and "Unnamed" not in str(c)])
    .replace(" ", "_")
    for col in df.columns
]

# Find the 'End of Week' column dynamically and drop empty rows
end_week_cols = [c for c in df.columns if "Week" in c and "End" in c]

if not end_week_cols:
    raise ValueError("Could not find an 'End of Week' column")

end_week_col = end_week_cols[0]
df = df.dropna(subset=[end_week_col])

df.to_csv(output_file, index=False)

print(f"Conversion complete! Saved as '{output_file}'")