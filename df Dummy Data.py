#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
from datetime import datetime
# Create a dummy DataFrame
data = {
    "EmployeeID": range(1, 11),
    "Name": ["Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Hannah", "Ian", "Julia"],
    "Department": ["HR", "Finance", "IT", "Finance", "IT", "HR", "Finance", "IT", "HR", "Finance"],
    "Salary": [50000, 60000, 70000, 80000, 60000, 50000, 90000, 75000, 52000, 87000],
    "JoiningDate": pd.date_range(start="2020-01-01", periods=10, freq="6M"),
    "PerformanceScore": np.random.randint(1, 6, 10),
    "RemoteWorkingDays": np.random.randint(0, 20, 10),
}
df = pd.DataFrame(data)

df


# In[2]:


# Display the first 5 rows of the DataFrame.
df.head(5)


# In[3]:


# Check for missing values in the DataFrame.
df.isnull().sum()


# In[4]:


#Get a summary of statistics for numeric columns.
x=df.describe()
x[["PerformanceScore","Salary","RemoteWorkingDays"]]


# In[5]:


# Select all employees from the "Finance" department.
finance=df[df["Department"]=="Finance"]
finance


# In[6]:


# Find employees with a salary greater than 70,000.
high=df[df["Salary"]>70000]["Name"]
high


# In[7]:


# List employees who joined after January 1, 2021.
recent_joiner=df[df["JoiningDate"]>"2021-01-01"]["Name"]
recent_joiner


# In[8]:


# Sort the DataFrame by Salary in descending order.
df.sort_values("Salary",ascending=False)


# In[9]:


#Sort by Department and then by PerformanceScore.
df.sort_values(["Department","PerformanceScore"])


# In[10]:


# Find the average salary of employees in each department.
df.groupby("Department")["Salary"].mean()


# In[11]:


# Calculate the total RemoteWorkingDays for all employees.
df["RemoteWorkingDays"].sum()


# In[12]:


# Group employees by Department and get the average PerformanceScore.
av= df.groupby("Department")["PerformanceScore"].mean()
av


# In[13]:


# Find the highest salary in each department.
high=df.groupby("Department")["Salary"].max()
high


# In[14]:


# Add a new column Tenure that calculates the number of days each employee has been with the company.
today=datetime.today()
df["Days Of Working"]=(today-df["JoiningDate"]).dt.days
df


# In[15]:


# Create a column Bonus as 10% of the Salary.
df["Bonus"]=df["Salary"]/10
df


# In[16]:


maxreuser=df[df["RemoteWorkingDays"]==df["RemoteWorkingDays"].max()]
maxreuser


# In[17]:


# Create a pivot table to display the average Salary by Department and PerformanceScore
pivot_table = df.pivot_table(values="Salary", index="Department", columns="PerformanceScore", aggfunc="mean")

print(pivot_table)


# 
# 
