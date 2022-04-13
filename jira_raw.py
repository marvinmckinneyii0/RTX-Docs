# %% [markdown]
# Raw Data Transformation
# 

# %%
### Import Packages
import pandas as pd
import numpy as np
import os
import glob
#from Jira_Raw_Data import Jira_Raw_Data_Transform


### Transforms go here

# %%
### Pull in and load csvs into one dataframe
all_files = glob.glob("C:/Users/Marvin/Desktop/RTX Docs/RTX_Python_ETL/Jira_Data/" + "*csv")
csv_list = [pd.read_csv(filename, index_col=None, header=0) for filename in all_files]
df_JiraRaw = pd.concat(csv_list, axis=0, ignore_index=True)

# %%
df_JiraRaw

# %%
df_JiraRaw.head(1)

# %%
df_JiraRaw['Project description']

# %%
df_JiraRaw['Issue key']

# %%
df_JiraRaw['Issue key'].isnull()

# %%
df_JiraRaw['Issue key'].values

# %%
df_JiraRaw.rename(columns={"Custom field (Start date)": "Start date"})

# %%
df_JiraRaw.columns.str.replace("Custom field ()", '')

# %%
df_JiraRaw['Task mode'].values

# %%

#Will need to replace all open parentheses with a singular column name
# needs to be more dynamic



"""df_JiraRaw.rename(columns={"Custom field (Start date)": "Start date","Custom field (Steps Count)": "Steps Count",
"Custom field (Story Points)":"Story Points","Custom field (Stretch)":"Stretch", "Custom field (Target end)":"Target end","Custom field (Target start)":"Target start",
"Custom field (Task mode)":"Task mode", "Custom field (Task progress)":"Task progress","Custom field (Team)":"Team", "Custom field (Tech Support Call)":"Tech Support Call",
"Custom field (Technical Debt)": "Technical Debt","Custom field (Test Count)":"Test Count","Custom field (Test Execution Status)":"Test Execution Status",	
"Custom field (Test Plan Status)":"Test Plan Status",	"Custom field (Test Repository Path)":"Test Repository Path", "Custom field (Test Set Status)":"Test Set Status",
"Custom field (Test Type)":"Test Type","Custom field (TestRunStatus)":"TestRunStatus","Custom field (Urgent)":"Urgent",	"Custom field (WSJF)":"WSJF",
"Custom field (Weighted Risk)":"Weighted Risk",	"Custom field (Xray End Date)":"Xray End Date"})"""


# %%
#Outward issue link (Belongs)


df_JiraRaw.columns.str.replace("Outward issue link ()", '')

#df_JiraRaw.rename({"Outward issue link (Belongs)": "Belongs"})

#df_JiraRaw.rename({"Inward issue link (Relates)":"Relates"})

df_JiraRaw[['Belongs','Relates']]

# %%
#Inward issue link (Relates)

#df_JiraRaw.rename({"Inward issue link (Relates)":"Relates"})

# %%
# Filter [Status] to exclude following values Cancelled, Duplicate

JiraRaw_filt = (df_JiraRaw['Status'] == 'Cancelled') | (df_JiraRaw['Status'] == 'Duplicate')

df_JiraRaw.loc[~JiraRaw_filt, 'Status']

# %%
#Convert datetime column to just date

df_JiraRaw['Created'] = pd.to_datetime(df_JiraRaw['Created'])



# %%
#Updated


df_JiraRaw['Updated'] = pd.to_datetime(df_JiraRaw['Updated'])



# %%
#Last Viewed

df_JiraRaw['Last Viewed'] = pd.to_datetime(df_JiraRaw['Last Viewed'])

# %%
#Baseline Start Date

df_JiraRaw['Baseline start date'] = pd.to_datetime(df_JiraRaw['Baseline start date'])

# %%
#Baseline end date

df_JiraRaw['Baseline end date'] = pd.to_datetime(df_JiraRaw['Baseline end date'])

# %%
#Start date

df_JiraRaw['Start date'] = pd.to_datetime(df_JiraRaw['Start date'])

# %%
#End date

df_JiraRaw['End date'] = pd.to_datetime(df_JiraRaw['End date'])

# %%
df_JiraRaw['Resolved'] = pd.to_datetime(df_JiraRaw['Resolved'])

#df_JiraRaw['Resolved'] = df_JiraRaw['Resolved'].astype('datetime64[ns]')

# %%
df_JiraRaw['Resolution'].sort_values

# %%
df_JiraRaw['Resolution'].isnull().sum()

# %%
df_JiraRaw[['Resolution', 'Agile Team']] = df_JiraRaw[['Resolution', 'Agile Team']].replace(np.nan,'null')

# %%
df_JiraRaw[['Resolution', 'Agile Team']]

# %%

df_JiraRaw.rename({'Agile Team':'Agile Team Old'})

# %%
df_JiraRaw['Agile Team Old']

# %%
df_JiraRaw[['Agile Team Old']] = df_JiraRaw[['Agile Team Old']].fillna('null')

# %%
df_JiraRaw['Agile Team Old'].sort_values

# %%
#df_JiraRaw['Agile Team'] = np.where(df_JiraRaw['Agile Team Old']!='null',['Agile Team Old'], ['Project key'])

# %%
agileteam_projectkey = {'NOPT':'PMO','TIHSNW':'Data Analytics','TBPSNW':'Data Analytics','NVCAP':'Cloud','NSECA':'Cloud','NPORTM':'Cloud','NPLAT':'Cloud','NMCA':'Cloud',
'NCOSM':'Cloud','NCADPT':'Cloud','NAZURE':'Cloud','NAWS':'Cloud',}

df_JiraRaw['Agile Team'] = np.where(df_JiraRaw['Agile Team Old']!='null',df_JiraRaw['Agile Team Old'], df_JiraRaw['Project key'].map(agileteam_projectkey))

# %%
df_JiraRaw[['Agile Team', 'Project key']].values

# %%
df_JiraRaw['Custom Field 1'].values

# %%
#df_JiraRaw['Custom Field 1'] = df_JiraRaw['Custom Field 1'].replace(np.nan, 0)


# %%
df_JiraRaw['Custom Field 1']/100

# %%
#df_JiraRaw['Custom Field 1'] = (df_JiraRaw['Custom Field 1'].astype(int))

# %%
#df_JiraRaw[['Agile Team', 'Custom Field 1']].sort_values

df_JiraRaw['Custom Field 1'].unique

# %%
#‒	Create new column named - using the [Resolved] field to determine the Saturday of the week of the [Resolution] date and convert to DATETIME and then to DATE

weekdays = pd.DataFrame(pd.to_datetime(df_JiraRaw["Resolved"]))
weekdays['Resolved day of week'] = weekdays["Resolved"].dt.dayofweek
weekdays['idx'] = ((12 - weekdays['Resolved day of week']) % 7) - 1
weekdays['Next Friday'] = weekdays["Resolved"] + pd.to_timedelta(weekdays['idx'],
                                                             unit='D')

# %%
#‒	Create new column named [Planned Closed] using the [Baseline end date] field to determine the Saturday of the week of the [Resolution] date and convert to DATETIME and then to DATE

weekdays = pd.DataFrame(pd.to_datetime(df_JiraRaw["Baseline end date"]))
weekdays['Planned Closed'] = weekdays["Baseline end date"].dt.dayofweek
weekdays['idx'] = ((12 - weekdays['Planned Closed']) % 7) - 1
weekdays['Next Friday'] = weekdays["Baseline end date"] + pd.to_timedelta(weekdays['idx'],
                                                             unit='D')










# %%
#‒	Create new column named [Resolution Month] using the [Resolution] field to determine the Month of the [Resolution] date and convert to DATETIME and then to DATE

df_JiraRaw['Resolution Month']= pd.to_datetime(df_JiraRaw['Resolved'])

df_JiraRaw['Resolution Month']= df_JiraRaw['Resolution Month'].dt.month

# %%
#‒	Create new column named [Planned Month] using the [Baseline end date] field to determine the Month of the [Resolution] date 

df_JiraRaw['Planned Month'] = pd.to_datetime(df_JiraRaw['Baseline end date'])

df_JiraRaw['Planned Month']= df_JiraRaw['Planned Month'].dt.month

# %%
#‒	Create new column named [Resolution Year] using the [Resolution] field to determine the Year of the [Resolution] date 

df_JiraRaw['Resolution Year'] = pd.to_datetime(df_JiraRaw['Resolved'])

df_JiraRaw['Resolution Year'] = df_JiraRaw['Resolution Year'].dt.year

# %%
#‒	Create new column named [Planned Year] using the [Baseline end date] field to determine the Year of the [Resolution] date

df_JiraRaw['Planned Year'] = pd.to_datetime(df_JiraRaw['Baseline end date'])

df_JiraRaw['Planned Year'] = df_JiraRaw['Planned Year'].dt.year
 



# %%
#‒	Create new column named [Created EOW] using the [Created] field to determine the Saturday of the week of the [Resolution] date and convert to DATETIME and then to DATE

weekdays = pd.DataFrame(pd.to_datetime(df_JiraRaw["Created"]))
weekdays['Created EOW'] = weekdays["Created"].dt.dayofweek
weekdays['idx'] = ((12 - weekdays['Created EOW']) % 7) - 1
weekdays['Next Friday'] = weekdays["Created"] + pd.to_timedelta(weekdays['idx'],
                                                             unit='D')


# %%
#‒	Create new column named [Created EOM] using the [Created] field to determine the Month of the [Created] date 

df_JiraRaw['Created EOM'] = pd.to_datetime(df_JiraRaw['Created'])

df_JiraRaw['Created EOM'] = df_JiraRaw['Created EOM'].dt.month

# %%
#‒	Create new column named [Created Year] using the [Created] field to determine the Year of the [Revised] date 

df_JiraRaw['Created Year'] = pd.to_datetime(df_JiraRaw['Created'])

df_JiraRaw['Created Year'] = df_JiraRaw['Created Year'].dt.year



# %%
#‒#Create new column named [Revised EOW] using the [Created] field to determine the Saturday of the week of the [Resolution] date and convert to DATETIME and then to DATE

weekdays = pd.DataFrame(pd.to_datetime(df_JiraRaw["Created"]))
weekdays['Revised EOW'] = weekdays["Created"].dt.dayofweek
weekdays['idx'] = ((12 - weekdays['Revised EOW']) % 7) - 1
weekdays['Next Friday'] = weekdays["Created"] + pd.to_timedelta(weekdays['idx'],
                                                             unit='D')

# %%
#‒	Create new column named [Revised EOM] using the [Created] field to determine the Month of the [Revised] date


df_JiraRaw['Revised EOM'] = pd.to_datetime(df_JiraRaw['Created'])

df_JiraRaw['Revised EOM'] = df_JiraRaw['Revised EOM'].dt.month
 


