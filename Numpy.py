#!/usr/bin/env python
# coding: utf-8

# ## NUMPY Tutorial

# In[1]:


import numpy as np


# Create a one dimension Numpy Array
# 

# In[2]:


array1=np.array([1,3,6], np.int8)
array1


# Create a two dimension Numpy Array

# In[3]:


array2=np.array([[2.1, 4.0, 6.7, 8.7],
                 [4.3, 5.3, 6.3, 9.3]])
array2


# Get dimesion of array1 and array 2

# In[4]:


array1.ndim


# In[5]:


array2.ndim


# Get shape of array1 and array2

# In[6]:


array1.shape


# In[7]:


array2.shape


# Get the type for array1 and array2

# In[8]:


array1.dtype


# In[9]:


array2.dtype


# Get the size of each items of array1 and array2

# In[10]:


array1.itemsize


# In[11]:


array2.itemsize


# Get the size of array1 and array2

# In[12]:


array1.size


# In[13]:


array2.size


# ## Accessing/Changing Specific element,row,column

# In[14]:


a=np.array([[1,2,3,4,5,6,7],[8,9,10,11,12,13,14]])
print(a)


# Get the elment 13 from the array

# In[15]:


a[1,5]


# Get the elment 13 from the array using negative indexing

# In[16]:


a[-1,-2]


#  Get the specific row (first row)

# In[17]:


a[0]


# Get the specific row (last row)

# In[18]:


a[-1]


#  Get the specific row (first column)

# In[19]:


a[:,0]


#  Get the specific row (last column)

# In[20]:


a[:,-1]


# liitle fancy (Extract 3,5,7)

# In[21]:


a[0, 2: :2]


# Change element 5 with 50

# In[22]:


a[0,4]=50


# In[23]:


a


# replace all element of 2 column to 40

# In[24]:


a[:,1]=40


# In[25]:


a


# change element of 2 column with 5,50

# In[26]:


a[:,1]=(5,50)


# In[27]:


a


# In[28]:


b=np.array([[[1,2],[3,4]],[[5,6],[7,8]]])
b


# extract value 4

# In[29]:


b[0,1,1]


# extract value 6

# In[30]:


b[1,0,1]


# Extract 1,2 and 5,6

# In[31]:


b[:,0,:]


# replace  1,2 and 5,6 with 23,24 and 25,26

# In[32]:


b[:,0,:]=[[23,24],[25,26]]


# In[33]:


b


# ## Initalizing different type of array

# Create all zeros (4,6) matrix

# In[34]:


c=np.zeros((4,6),dtype=int)
c


# Create all zeros (10,6) matrix

# In[35]:


d=np.ones((10,6),dtype=int)
d


# Create a matrix of shape 4X4 with all element as 99

# In[36]:


e=np.full((4,4),99,dtype=int)
e


# create a array with all value as 85 and shape a e

# In[37]:


f=np.full_like(e,85)
f


# create a 5X5 natrix with random numbers

# In[38]:


g=np.random.rand(5,5)
g


# create a 5X5 natrix with random int numbers

# In[39]:


h=np.random.randint(5,100,(5,5))
h


# Create identity matrix

# In[40]:


i=np.identity(5)
i


# Create an array which repeat [1,2,3]

# In[41]:


arr=np.array([[1,2,3]])
j=np.repeat(arr,8,axis=0)
j


# In[42]:


zer=np.zeros((5,5),dtype=int)
print(zer)
on=np.ones((3,3),dtype=int)
on[1,1]=9
print(on)

zer[1:4,1:4]=on
zer


# In[43]:


#Be careful while copy the array 
# don't use a=b as if there will be any change to a then it will reflect  in b
# Instead use a=b.copy


# ## Mathematics

# In[44]:


k=np.array([1,2,3,4])
k


# In[45]:


k+1


# In[46]:


k-2


# In[47]:


k*2


# In[48]:


k/2


# In[49]:


l=np.array([2,3,4,5])


# In[50]:


k+l


# In[51]:


k-l


# In[52]:


k*l


# In[53]:


k/l


# In[54]:


np.sin(k)


# In[55]:


np.cos(k)


# # statistics

# In[56]:


stats=([4,5,8],[9,11,7])
stats


# In[57]:


np.min(stats)


# In[58]:


np.min(stats,axis=0)


# In[59]:


np.min(stats,axis=1)


# In[60]:


np.max(stats)


# In[61]:


np.max(stats,axis=0)


# In[62]:


np.max(stats,axis=1)


# In[63]:


np.sum(stats)


# In[64]:


np.sum(stats,axis=1)


# In[65]:


np.sum(stats,axis=0)


# ## Reorganise array

# In[66]:


before=np.array([[1,2,3,4],[5,6,7,8]])
print(before)
after=before.reshape((8,1))
print(after)


# In[67]:


v1=np.array([[1,2,3,4]])
v2=np.array([[5,6,7,8]])

np.vstack([v1,v2])


# In[68]:


v1=np.array([[1,2,3,4]])
v2=np.array([[5,6,7,8]])

np.vstack([v1,v2,v2,v1])


# In[69]:


h1=np.zeros((4,2))
h2=np.ones((4,2))
np.hstack([h1,h2])


# ## Miscellaneous

# Load Data from file

# In[70]:


data=np.genfromtxt("Lending-company.csv",dtype=np.int16,delimiter=",")
data


# ## Boolean Masking And Advanced Indexing

# In[71]:


data>50


# In[72]:


((data>50) & (data<100))


# In[73]:


data[data>50]


# In[74]:


data[((data>50) & (data<100))]

