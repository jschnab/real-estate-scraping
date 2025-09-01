import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.lines import Line2D


# zipcode counts
"""
df = pd.read_csv("properties.csv")
zipcode_counts = df["zipcode"].value_counts().sort_values()
fig, ax = plt.subplots(figsize=(8, 6))
ax.scatter(zipcode_counts.values, range(len(zipcode_counts)))
ax.set_yticks(range(len(zipcode_counts)))
ax.set_yticklabels(zipcode_counts.index.values, fontsize=12)
ax.set_ylabel("Zipcode", fontsize=16) ax.set_xlabel("Number of listings", fontsize=16)
ax.tick_params(labelsize=12)
ax.grid(axis="y", linestyle=":", zorder=1)
ax.set_title(
    "Number of real estate listings per zipcode between 9/2023 and 3/2024"
)
"""

# zipcode, bedrooms counts
df = pd.read_csv("count_zipcode_beds.csv")
fig, ax = plt.subplots(figsize=(8, 48))

# groups on the same horizontal line
for idx, row in df.iterrows():
    beds_1 = ax.scatter(row["beds_1"], idx, c=["C0"], label="1 bedroom")
    beds_2 = ax.scatter(row["beds_2"], idx, c=["C1"], label="2 bedrooms")
    beds_3 = ax.scatter(row["beds_3"], idx, c=["C2"], label="3 bedrooms")
    beds_4 = ax.scatter(row["beds_4"], idx, c=["C3"], label="4 bedrooms")
    beds_5 = ax.scatter(row["beds_5_more"], idx, c=["C4"], label="> 4 bedrooms")
ax.set_yticks(range(len(df)))
ax.set_yticklabels(df["zipcode"].values, fontsize=12)
ax.set_ylabel("Zipcode", fontsize=16)
ax.set_xlabel("Number of listings", fontsize=16)
ax.grid(axis="y", linestyle=":")
ax.set_axisbelow(True)
handles, labels = ax.get_legend_handles_labels()
ax.legend(handles[:5], labels[:5])

# groups on different lines (too crowded, should use a table)
"""
FACT = 15
for idx, row in df.iterrows():
    beds_5 = ax.scatter(
        row["beds_5_more"],
        FACT * idx,
        c=["C4"],
        label=">= 5 bedrooms"
    )
    beds_4 = ax.scatter(
        row["beds_4"],
        FACT * idx + 2,
        c=["C3"],
        label="4 bedrooms"
    )
    beds_3 = ax.scatter(
        row["beds_3"],
        FACT * idx + 4,
        c=["C2"],
        label="3 bedrooms"
    )
    beds_2 = ax.scatter(
        row["beds_2"],
        FACT * idx + 6,
        c=["C1"],
        label="2 bedrooms"
    )
    beds_1 = ax.scatter(
        row["beds_1"],
        FACT * idx + 8,
        c=["C0"],
        label="1 bedroom"
    )

yticks = []
for idx in range(len(df["zipcode"].values)):
    yticks.extend([FACT*idx + 2*x for x in range(6)])
print(yticks)
ax.set_yticks(yticks)

ytick_labels = []
for zipcode in df["zipcode"].values:
    ytick_labels.extend(
        [">= 5 beds", "4 beds", "3 beds", "2 beds", "1 beds", zipcode]
    )
ax.set_yticklabels(ytick_labels, fontsize=9)
ax.grid(axis="y", linestyle=":")
ax.set_axisbelow(True)
"""

plt.show()
