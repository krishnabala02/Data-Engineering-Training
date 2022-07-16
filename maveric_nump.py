import numpy as np
import cv2
import matplotlib.pyplot as plt

data = np.array(([450,650,782,482,865,400,500,656]))
data1 = np.array([[784,766865],[456,89000],[456,87900]])
print(type(data))
print(data.ndim)
print(data.shape)
print(data.astype("float"))
img = cv2.imread("")
plt.imshow(img)
print(img.shape)
