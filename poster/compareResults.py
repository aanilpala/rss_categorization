import pylab as pl
import scipy as sp
import numpy as np
import matplotlib.pyplot as plt
import time
import pdb
import math
import mpl_toolkits.mplot3d.axes3d as p3
from scipy.interpolate import interp1d

def plotting():
	

	fig = plt.figure()
	plt.xlabel('test items')
	plt.ylabel('accurency in %')
	pl.ylim(55, 71)

	#batch-bruteforce with ca 600 testpoints
	#batch1 = np.array([[600,1100,1700,2150,2850],[60.0,62.73,60.26,58.01,60.49]])
	#bruteforce = np.array([[600,1100,1860,2480,3000],[60.0, 63.87,63.78,66.13,65.41]])
	#retrain1 = np.array([[600,1100,1860,2480,3000],[60.0, 63.87,63.78,66.13,65.41]])

	#threshold vs batch on 300 testpoints
	batch2 =  np.array([[400,730,1000,1210,1560,1873,2180,2520,2900],[61.84,56.97,64.34,60.66,56.65,65.17, 57.37, 55.26, 60.06]])
	threshold = np.array([[300,600,900,1200,1728,2128,2236,2536,2836],[60.3,60.3,61.0,70.0,64.9,65.3,64.9,62.0,60.0]])
	retrain2 = np.array([[300,600,900,1728,2236,2536,2836],[60.3,60.3,61.0,64.9,64.9,62.0,60.0]])
	
	#plt.plot(batch1[0,:],batch1[1,:],'r-',bruteforce[0,:],bruteforce[1,:],'g-')
	#plt.scatter(retrain1[0,:],retrain1[1,:])

	th = np.array([[300,3000],[65,65]])
	plt.plot(batch2[0,:],batch2[1,:],'r-',threshold[0,:],threshold[1,:],'b-',th[0,:],th[1,:],'y-')
	plt.scatter(retrain2[0,:],retrain2[1,:])
	plt.legend(['batch', 'threshold-driven', 'threshold', 'retrain'], loc='best')
	pl.xlim(600, 2900)
	plt.grid(True)

	plt.savefig("batchVSerror.png")
	plt.show()

	
	return