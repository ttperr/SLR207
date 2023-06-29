import matplotlib.pyplot as plt

X = [3, 5, 10, 15, 30]
Yconnection = [0.04, 0.077, 0.095, 0.373, 0.472]
Ymap = [3.876, 3.871, 4.064, 4.193, 4.302]
Yshuffle = [3.976, 4.569, 2.059, 1.131, 0.882]
Yreduce = [2.272, 2.536, 0.997, 0.461, 0.302]
Ytot = [10.174, 11.062, 7.228, 6.173, 5.998]

plt.plot(X, Yconnection, label='Connection')
plt.plot(X, Ymap, label='Map')
plt.plot(X, Yshuffle, label='Shuffle')
plt.plot(X, Yreduce, label='Reduce')
plt.plot(X, Ytot, label='Total')
plt.legend()
plt.xlabel('Number of machines')
plt.ylabel('Time (s)')
plt.title('Time spent in each phase')
plt.savefig('../data/plot.png')
plt.show()