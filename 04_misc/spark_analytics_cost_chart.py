#!/usr/bin/env python3
import matplotlib.pyplot as plt
import numpy as np

labels = ["AWS EMR", "GCP Dataproc"]
cost_to_run_analytics = [1.06, 1.91]
monthly_storage_cost = [1.47, 1.27]
job_runtime_seconds = [112, 137]

x = np.arange(len(labels))
width = 0.2

colors_costs = ["navy", "purple"]
color_runtime = "seagreen"

fig, ax1 = plt.subplots(figsize=(10, 6))

ax1.bar(x - width, cost_to_run_analytics, width, label="Cost to Run Analytics ($)", color=colors_costs[0])
ax1.bar(x, monthly_storage_cost, width, label="Monthly Storage Cost ($)", color=colors_costs[1])

ax2 = ax1.twinx()
runtime_bars = ax2.bar(x + width, job_runtime_seconds, width, label="Job Runtime (s)", color=color_runtime)

ax1.grid(False)
ax2.grid(False)

ax1.set_xlabel("Cloud Provider Service")
ax1.set_ylabel("Costs in $")
ax2.set_ylabel("Time in Seconds")
ax1.set_title("Analytical Query Cost Comparison Between AWS EMR and GCP Dataproc")
ax1.set_xticks(x)
ax1.set_xticklabels(labels)

ax2.set_ylim(0, 160)

ax1.legend(loc='upper left', bbox_to_anchor=(0, 1), shadow=True, ncol=1)
ax2.legend(loc='upper right', bbox_to_anchor=(1, 1), shadow=True, ncol=1)

for rect in ax1.patches:
    height = rect.get_height()
    ax1.annotate(f"${height}", (rect.get_x() + rect.get_width() / 2, height), ha="center", va="bottom")

for rect in runtime_bars:
    height = rect.get_height()
    ax2.annotate(f"{height}s", (rect.get_x() + rect.get_width() / 2, height), ha="center", va="bottom")

plt.tight_layout()

# plt.show()
plt.savefig("spark_analytics_cost_chart.png", dpi=600)
