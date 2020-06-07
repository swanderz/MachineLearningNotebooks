import pandas as pd
import numpy as np
import matplotlib
from matplotlib import pyplot as plt
plt.switch_backend('agg')

from mpl_toolkits.mplot3d import Axes3D
from matplotlib.ticker import NullFormatter
from collections import OrderedDict

from time import time

from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn import manifold, datasets
from sklearn.manifold import TSNE


from azureml.core import Run, VERSION as amlversion

from gensim import __version__ as gsversion
from sklearn import __version__ as skversion
from scipy import __version__ as spversion
from pandas import __version__ as pdversion
from numpy import __version__ as npversion

print(f"Azure ML version: {amlversion}")
print(f"gensim version: {gsversion}")
print(f"sklearn version: {skversion}")
print(f"scipy version: {spversion}")
print(f"numpy version: {npversion}")
print(f"pandas version: {pdversion}")

random_state = 42
n_points = 1000
X, color = datasets.make_s_curve(n_points, random_state=random_state)
n_neighbors = 10
n_components = 2

# Create figure
fig = plt.figure(figsize=(15, 8))
fig.suptitle("Manifold Learning with %i points, %i neighbors"
             % (1000, n_neighbors), fontsize=14)

# Add 3d scatter plot
ax = fig.add_subplot(251, projection='3d')
ax.scatter(X[:, 0], X[:, 1], X[:, 2], c=color, cmap=plt.cm.Spectral)
ax.view_init(4, -72)


methods = OrderedDict()
methods['PCA'] = PCA(n_components=n_components, random_state = 42)
methods['t-SNE'] = manifold.TSNE(n_components=n_components, init='pca',
                                 verbose=1, perplexity=40, n_iter=300, 
                                 random_state=42)


run = Run.get_context()

results = {}
# Plot results
for i, (label, method) in enumerate(methods.items()):
    t0 = time()
    Y = method.fit_transform(X)
    if label == 't-SNE':
        run.log("kl_divergence", method.kl_divergence_)
    elif label == 'PCA':
        run.log_list("PCA means", method.mean_)
    results['{}_two'.format(label)] = Y[:1][0]
    run.log('{}_two_one'.format(label), Y[:1][0][0])
    run.log('{}_two_two'.format(label), Y[:1][0][1])
    t1 = time()
    run.log(label, "%.2g sec" % (t1 - t0))
    ax = fig.add_subplot(2, 5, 2 + i + (i > 3))
    ax.scatter(Y[:, 0], Y[:, 1], c=color, cmap=plt.cm.Spectral)
    ax.set_title("%s (%.2g sec)" % (label, t1 - t0))
    ax.xaxis.set_major_formatter(NullFormatter())
    ax.yaxis.set_major_formatter(NullFormatter())
    ax.axis('tight')


plt.savefig('outputs/plot.png')
run.log_image(name="results", plot=plt)

df_results = pd.DataFrame(results)
print(df_results.head())

