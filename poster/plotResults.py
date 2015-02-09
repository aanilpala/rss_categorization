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
	#My = np.array([[1,2,3],[5,6,7],[4,5,6]])
	#Batch:
	M2D = np.array([[1000,2000,3000],[60.89,61.34,59.56]])
	#bruteforce
	#M2D = np.array([[400,1400,2400, 3000],[60.35,57.99,65.9,63.37]])
	#errorprtriggered:
	#M2D = np.array([[300,600,900, 1200, 2280,2600, 3000],[60.3,60.3,61.0,69.0,62.9,58.6, 62.0]])

	fig = plt.figure()
	plt.xlabel('test items')
	plt.ylabel('accurency in %')
	pl.ylim(55, 70)
	#pl.xlim(300, 3000)
	#threshold:
	#pl.plot([300,3000], [63,63], c='r')
	pl.plot(M2D[0,:],M2D[1,:], c='b')
	#pl.scatter([300,600,900, 2280,2600, 3000],[60.3,60.3,61.0,62.9,58.6, 62.0], c='b')
	#plt.legend(['threshold', 'acc', 'retrain'], loc='best')
	
	#bruteforce
	#pl.scatter(M2D[0,:],M2D[1,:], c='b')
	#plt.legend(['acc', 'retrain'], loc='best')

	#total:
	#batch = np.array([[1000,2000,3000],[60.89,61.34,59.56]])
	#bruteforce = np.array([[1400,2400, 3000],[57.99,65.9,63.37]])
	#error = np.array([[400,900, 1200, 2280,2600, 3000],[60.35,61.0,69.0,62.9,58.6, 62.0]])
	#plt.plot(batch[0,:],batch[1,:],'r-',bruteforce[0,:],bruteforce[1,:],'g-',error[0,:],error[1,:],'b-')
	pl.xlim(500, 3000)
	plt.grid(True)

	plt.savefig("batchPlot.png")
	#ax = fig.add_subplot(111, projection='3d')
	#ax.plot(My[0,:],My[1,:],My[2,:], c='r')
	plt.show()

	
	return