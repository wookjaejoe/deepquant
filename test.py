import pandas as pd
import matplotlib.pyplot as plt

# define figure and axes
fig, ax = plt.subplots()

# hide the axes
fig.patch.set_visible(False)
ax.axis('off')
ax.axis('tight')

# create data
df = pd.DataFrame([[1, 2], [3, 4]], columns=['First', 'Second'])

# create table
table = ax.table(cellText=df.values, colLabels=df.columns, loc='center')

# display table
fig.tight_layout()
plt.show()
