# pandas.concat(objs, axis=0, join='outer', join_axes=None, ignore_index=False, keys=None, levels=None, names=None, verify_integrity=False, sort=None, copy=True)

import pandas as pd
	
df_1 = pd.DataFrame(
	[['Somu', 68, 84, 78, 96],
	['Kiku', 74, 56, 88, 85],
	['Ajit', 77, 73, 82, 87]],
	columns=['name', 'physics', 'chemistry','algebra','calculus'])

df_2 = pd.DataFrame(
	[['Amol', 72, 67, 91, 83],
	['Lini', 78, 69, 87, 92]],
	columns=['name', 'physics', 'chemistry','geometry','calculus'])	

frames = [df_1, df_2]


# reset index
# df.reset_index(drop=True, inplace=True)
#concatenate dataframes
df = pd.concat(frames, sort=False)

#print dataframe
print("df_1\n------\n",df_1)
print("\ndf_2\n------\n",df_2)
print("\ndf\n--------\n",df)