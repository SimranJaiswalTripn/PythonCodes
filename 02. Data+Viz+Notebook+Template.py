#!/usr/bin/env python
# coding: utf-8

# ## Libraries

# In[1]:


import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
sns.set()
import pandas as pd


# ## Bar Chart

# In[5]:


df_used_cars=pd.read_csv("04. bar_chart_data.csv")
df_used_cars


# In[40]:


plt.figure(figsize=(9,6))
plt.bar(x=df_used_cars["Brand"],
        height=df_used_cars["Cars Listings"],
        color="midnightblue"
        )
plt.xticks(rotation=45,fontsize=14)
plt.yticks(fontsize=14)
plt.title("Car Listing By Brands",fontsize=16,fontweight='bold')
plt.ylabel("Number of listings",fontsize=14)
plt.show
plt.savefig("Used Car Bar.png")


# In[ ]:





# ## Pie Chart

# In[42]:


df_fuel_engine_by_type=pd.read_csv("06. pie_chart_data.csv")
df_fuel_engine_by_type


# In[61]:


sns.set_palette('colorblind')


# In[66]:


plt.figure(figsize=(10,8))
plt.pie(df_fuel_engine_by_type["Number of Cars"],
       labels=df_fuel_engine_by_type["Engine Fuel Type"].values,
       autopct="%.2f%%",
       textprops={'size':'x-large',
                 'fontweight':'bold',
                 'rotation':30,
                 'color':'w'})
plt.title("Car By Fuel Engine Type",fontsize=16,fontweight='bold')
plt.legend()
plt.show


# In[ ]:





# In[ ]:





# ## Stacked Area Chart

# In[70]:


df_fuel_engine_type=pd.read_csv("07. stacked_area_chart_data.csv")
df_fuel_engine_type


# In[106]:


colors=("#011638","#7e2987","#ef2026")
sns.set_style("white")
label=["Gas","Petrol","Diesel"]
plt.figure(figsize=(12,6))
plt.stackplot(df_fuel_engine_type["Year"],
              df_fuel_engine_type["Gas"],
              df_fuel_engine_type["Petrol"],
              df_fuel_engine_type["Diesel"],
              colors=colors,
              edgecolor='none')
plt.xticks(df_fuel_engine_type["Year"],rotation=45)
plt.legend(label,loc="upper left")
plt.ylabel("No. Of Cars",fontsize=13)
plt.title("Popularity of engine fuel type (1982-2016)",fontweight="bold",fontsize=14)
sns.despine()
plt.show


# In[ ]:





# ## Line Chart

# In[4]:


data=pd.read_csv("09. line_chart_data.csv")
data


# In[5]:


data["NewDate"]=pd.to_datetime(data["Date"])
data


# In[8]:


plt.figure(figsize=(20,8))
plt.plot(data["NewDate"],data["GSPC500"])
plt.plot(data["NewDate"],data["FTSE100"])
plt.title("GSPC500 Vs FTSE100 (2000-2010)",fontsize=18,fontweight="bold")
plt.xlabel("Date")
plt.ylabel("Return")
labels=["S&P500","FTSE100"]
plt.legend(labels,fontsize=16)
plt.show


# In[10]:


Data_H2_08=data[(data["NewDate"]>='2008-07-01')&
                (data["NewDate"]<='2008-12-31')]
Data_H2_08


# In[13]:


plt.figure(figsize=(20,8))
plt.plot(Data_H2_08["NewDate"],Data_H2_08["GSPC500"],color='midnightblue')
plt.plot(Data_H2_08["NewDate"],Data_H2_08["FTSE100"],color='crimson')
plt.title("GSPC500 Vs FTSE100 H2 2008",fontsize=18,fontweight="bold")
plt.xlabel("Date")
plt.ylabel("Return")
labels=["S&P500","FTSE100"]
plt.legend(labels,fontsize=16)
plt.show


# In[ ]:





# In[ ]:





# In[ ]:





# ## Histogram

# In[2]:


data=pd.read_csv("11. histogram_data.csv")
data


# In[29]:


sns.set_style('white')
plt.figure(figsize=(8,6))
plt.hist(data["Price"],
         bins=8,
         color='#108A99')
plt.title("Distribution of Real-Estate by Prices", fontsize=15, fontweight='bold')
plt.xlabel("Price (in .000$)")
plt.ylabel("No. of Properties")
sns.despine()
plt.show


# In[ ]:





# ## Scatter Plot

# In[33]:


sns.set_style('white')
plt.figure(figsize=(6,4))
scatter=plt.scatter(data["Area (ft.)"],
            data["Price"],
            c=data["Building Type"],
            cmap='mako',
            alpha=0.6)
plt.xlabel("Area (ft.)",fontweight='bold',fontsize=10)
plt.ylabel("Price",fontweight='bold',fontsize=10)
plt.title("Relation btw Area and Price",fontweight='bold',fontsize=14)
plt.legend(*scatter.legend_elements(),
           loc="upper left",
            title='Building Type')
sns.despine()
plt.show


# In[65]:


sns.set_style('darkgrid')
plt.figure(figsize=(6,4))
sns.scatterplot(x=data["Area (ft.)"],
                y=data["Price"],
                hue=data["Building Type"],
               palette=["black","DarkBlue","Purple","Pink","white"])

plt.xlabel("Area (ft.)",fontweight='bold',fontsize=10)
plt.ylabel("Price",fontweight='bold',fontsize=10)
plt.title("Relation btw Area and Price",fontweight='bold',fontsize=14)
plt.show()


# In[ ]:





# In[ ]:





# ## Regression Plot

# In[51]:


data1=pd.read_csv("16. regression_plot_homework.csv")
data1


# In[66]:


plt.figure(figsize=(10,8))
sns.regplot(data=data1,
            x="Budget",
            y="Sales",
            scatter_kws={'color':'k'},
            line_kws={'color':'red'})
plt.xlabel("Budget in ($'000)'",fontweight='bold',fontsize=10)
plt.ylabel("Sale in '000 Unit'",fontweight='bold',fontsize=10)
plt.title("Relation btw ad budget and goods sold",fontweight='bold',fontsize=14)
plt.show


# In[68]:


plt.figure(figsize=(10,8))
sns.lmplot(data=data1,
            x="Budget",
            y="Sales",
            scatter_kws={'color':'k'},
            line_kws={'color':'red'})
plt.xlabel("Budget in ($'000)'",fontweight='bold',fontsize=10)
plt.ylabel("Sale in '000 Unit'",fontweight='bold',fontsize=10)
plt.title("Relation btw ad budget and goods sold",fontweight='bold',fontsize=14)
plt.show


# In[ ]:





# ## Bar and Line Chart

# In[3]:


from matplotlib.ticker import PercentFormatter


# In[4]:


df_data=pd.read_csv("17. bar_line_chart_data.csv")
df_data


# In[27]:


fig, ax=plt.subplots(figsize=(10,8))

ax.bar(df_data["Year"],
       df_data["Participants"],
       color="k")
ax.set_ylabel("No. Of Participants",
              weight="bold")

ax.set_xlabel("Year",
              weight="bold")

ax1=ax.twinx()
ax1.set_ylim(0,1)
ax1.yaxis.set_major_formatter(PercentFormatter(xmax=1.0))
ax1.plot(df_data["Year"],
         df_data["Python Users"],
         color="#b60000",
         marker="d")

ax1.set_ylabel("% Of Python Users",
               weight="bold",
               color="#b60000")

ax.set_title("Survey Python User (2012-2019)",
              size=14,
              weight="bold")

plt.show()


# In[ ]:




