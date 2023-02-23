import matplotlib.pyplot as plt
import numpy as np
import time

# code for tracking time
start = time.time()

# code body for one of the DBs

end = time.time()

# uo_small_time as an example but obvioulsy do it for all
uo_small_time = start - end


databases = ( #altered
    "SmallDB",
    "MediumDB",
    "LargeDB",
)
expiriments = {
    # need to track time and store it in the variables in the array so it can plot bars accordingly
    "User-Optimized": np.array([uo_small_time, uo_medium_time, uo_large_time]),
    "Self-Optimized": np.array([so_small_time, so_medium_time, so_large_time]),
    "Uninformed": np.array([u_small_time, u_medium_time, u_large_time]),
}
width = 0.5

fig, ax = plt.subplots()
bottom = np.zeros(3)

for boolean, experiment in experiments.items():
    p = ax.bar(databases, experiment, width, label=boolean, bottom=bottom)
    bottom += experiment

ax.set_title("Query 1 (runtime in ms)") # altered
ax.legend(loc="upper left")

plt.show()
