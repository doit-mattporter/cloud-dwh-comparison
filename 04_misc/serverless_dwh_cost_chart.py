#!/usr/bin/env python3
import matplotlib.pyplot as plt
import numpy as np

cost_labels = ["Cost to Load Data", "Cost to Run Analytics", "Monthly Storage Cost"]
cost_aws_athena = [0, 0.29, 1.47]
cost_aws_redshift = [21.01, 6.4, 10.06]
cost_gcp_bigquery = [0, 3.18, 7.25]

runtime_labels = ["Data Load Runtime", "Analytics Runtime"]
runtime_aws_athena = [0, 120]
runtime_aws_redshift = [1677, 33]
runtime_gcp_bigquery = [163, 30]

x_cost = np.arange(len(cost_labels))
x_runtime_adjusted = np.arange(len(runtime_labels))
width_adjusted = 0.2


# Function to add value labels
def add_value_labels(ax, data, chart_type, spacing=5):
    for idx, rect in enumerate(ax.patches):
        y_value = rect.get_height()
        x_value = rect.get_x() + rect.get_width() / 2

        if chart_type == "cost":
            label = "${:.2f}".format(y_value)
            # Append custom label for 'Cost to Run Analytics' for AWS Athena and GCP BigQuery
            if idx == 1:  # AWS Athena 'Cost to Run Analytics'
                label += "\n(57.68 GB processed)"
            elif idx == 7:  # GCP BigQuery 'Cost to Run Analytics'
                label += "\n(509.21 GB processed)"
        elif chart_type == "runtime":
            label = "{}s".format(int(y_value))
        else:
            label = str(y_value)

        ax.annotate(
            label,
            (x_value, y_value),
            xytext=(0, spacing),
            textcoords="offset points",
            ha="center",
            va="bottom",
        )


# Cost Comparison figure
fig, ax = plt.subplots(figsize=(10, 8))
ax.bar(
    x_cost - width_adjusted,
    cost_aws_athena,
    width_adjusted,
    label="AWS Athena",
    color="#FFB366",
)
ax.bar(
    x_cost,
    cost_aws_redshift,
    width_adjusted,
    label="AWS Redshift Serverless",
    color="#FF9900",
)
ax.bar(
    x_cost + width_adjusted,
    cost_gcp_bigquery,
    width_adjusted,
    label="GCP BigQuery",
    color="#4285F4",
)
ax.set_ylabel("Costs ($)")
ax.set_title(
    "Analytical Query Cost Comparison Between AWS and GCP Serverless Data Warehousing Services"
)
ax.set_xticks(x_cost)
ax.set_xticklabels(cost_labels)
ax.legend()
ax.grid(False)
add_value_labels(ax, cost_aws_athena, "cost")
plt.tight_layout()
# plt.show()
plt.savefig("serverless_dwh_cost_comparison.png", dpi=600)
plt.close()

# Runtime Comparison figure
fig, ax = plt.subplots(figsize=(10, 8))
ax.bar(
    x_runtime_adjusted - width_adjusted,
    runtime_aws_athena,
    width_adjusted,
    label="AWS Athena",
    color="#FFB366",
)
ax.bar(
    x_runtime_adjusted,
    runtime_aws_redshift,
    width_adjusted,
    label="AWS Redshift Serverless",
    color="#FF9900",
)
ax.bar(
    x_runtime_adjusted + width_adjusted,
    runtime_gcp_bigquery,
    width_adjusted,
    label="GCP BigQuery",
    color="#4285F4",
)
ax.set_ylabel("Time (seconds)")
ax.set_title("Runtime Comparison Between AWS and GCP Serverless Data Warehousing Services")
ax.set_xticks(x_runtime_adjusted)
ax.set_xticklabels(runtime_labels)
ax.legend()
ax.grid(False)
add_value_labels(ax, runtime_aws_athena, "runtime")
plt.tight_layout()
# plt.show()
plt.savefig("serverless_dwh_runtime_comparison.png", dpi=600)
plt.close()
