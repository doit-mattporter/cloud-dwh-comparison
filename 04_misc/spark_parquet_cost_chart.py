#!/usr/bin/env python3
import matplotlib.pyplot as plt
import numpy as np

# Cost components names
components = [
    "gnomAD Conversion",
    "dbNSFP Conversion",
    "Database Join and Write to Parquet",
    "Total Cost of Parquet Conversion",
    "Monthly Bucket Storage",
    "Monthly Metastore",
]

# AWS and GCP costs for each component
aws_costs = [
    167.82,
    52.08,
    23.28,
    243.18,
    10.62,
    0.00,
]  # Assuming Glue Data Catalog cost as 0 for simplicity
gcp_costs = [
    136.89,
    50.33,
    24.65,
    211.87,
    9.23,
    244.80,  # Using Developer metastore cost for simplicity
]

# GCP Metastore costs that indicate cost variability
gcp_metastore_developer = 244.80
gcp_metastore_enterprise = 2462.40
gcp_metastore_enterprise_plus = 4924.80

# Adjusting the GCP costs list to use the Developer metastore cost for visualization
gcp_costs[-1] = gcp_metastore_developer

x = np.arange(len(components))
width = 0.35

fig, ax = plt.subplots(figsize=(14, 8))
rects1 = ax.bar(x - width / 2, aws_costs, width, label="AWS", color="#FF9900")
rects2 = ax.bar(x + width / 2, gcp_costs, width, label="GCP", color="#4285F4")


# Function to add a label above each bar
def add_labels(bars):
    for bar in bars:
        height = bar.get_height()
        ax.annotate(
            f"${height:.2f}",
            xy=(bar.get_x() + bar.get_width() / 2, height),
            xytext=(0, 3),  # 3 points vertical offset
            textcoords="offset points",
            ha="center",
            va="bottom",
        )


add_labels(rects1)
add_labels(rects2)

ax.set_ylabel("Costs ($)")
ax.set_title("Parquet Conversion Cost Comparison Between AWS EMR and GCP Dataproc")
ax.set_xticks(x)
ax.set_xticklabels(components)
ax.legend()

# Annotate the variable metastore costs for GCP with a line
highest_bar_height = max(aws_costs + gcp_costs)
offset_right = 0.5
label_height_offset = 20
metastore_label_position = (
    x[-1] + width / 2 + offset_right,
    highest_bar_height + label_height_offset,
)
metastore_bar_position = (x[-1] + width / 2, gcp_costs[-1])

ax.text(
    *metastore_label_position,
    f"Developer: ${gcp_metastore_developer}\n"
    f"Enterprise: ${gcp_metastore_enterprise}\n"
    f"Enterprise Plus: ${gcp_metastore_enterprise_plus}",
    ha="right",
    va="bottom",
)

# Draw a line from the bar to the label
ax.annotate(
    "",
    xy=metastore_bar_position,
    xycoords="data",
    xytext=metastore_label_position,
    textcoords="data",
    arrowprops=dict(arrowstyle="->", connectionstyle="arc3", color="black"),
)

plt.xticks(rotation=30)
plt.tight_layout()

# plt.show()
plt.savefig("spark_parquet_cost_chart.png", dpi=600)
