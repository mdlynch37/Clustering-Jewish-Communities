import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from scipy.stats import distributions, probplot


class ProbPlots(object):
    """Constructs prob plots for various distributions.

    Distribution parameters are estimated for probability plots
    that require parameters. Plotting functions each return a
    matplotlib.axes._subplots.AxesSubplot object.

    Distribution parameters are lazy properties that only fit
    and set on first access since that can be very expensive
    computationally.

    Parameters
    ----------
    col : pandas.Series
    """
    # This method of implementing a lazy attribute is slightly
    # less efficient that using a descriptor (because it must
    # route through the function on each get instead of simply
    # looking up the attribute in the instance's dictionary).
    # However, this method disallows setting the attribute.
    # (Not terribly important here, but great practice!)
    def dist_params_property(name, dist):
        """Returns a lazy property, dist.fit can be slow."""
        storage_name = '_' + name

        def fit_dist(self, dist):
            return dist.fit(self.col)

        @property
        def prop(self):
            if not hasattr(self, storage_name):
                setattr(self, storage_name, fit_dist(self, dist))
            return getattr(self, storage_name)

        return prop  # calls property above

    # Example of alternative method using descriptor class
    # Excerpted from David Beazley. "Python Cookbook".
    class lazyproperty:
        def __init__(self, func):
            self.func = func

        def __get__(self, instance, cls):
            if instance is None:
                return self
            else:
                value = self.func(instance)
                setattr(instance, self.func.__name__, value)
                return value

    # could use metaclasses instead, allows for abritrary num of dists
    # with loop to construct arbitrary num of properties
    DISTS = dict(
        norm=distributions.norm,
        expon=distributions.expon,
        pareto=distributions.pareto)
    # TODO: These probably should be bound explicitly to self, not just class
    #       Avoids changing class along with instance.

    # on access, params are fit and set as 'lazy property'
    norm_params = dist_params_property('norm', DISTS['norm'])
    expon_params = dist_params_property('expon', DISTS['expon'])
    pareto_params = dist_params_property('pareto', DISTS['pareto'])

    def __init__(self, col):
        self.col = col
        self.dists = self.DISTS  # bind to instance variable??
        # But changes will apply to class variable since
        # DISTS is a collection (dict).
        # why lower case dists? new dists can't really be added
        # without using metaclasses.

    def plot_probplot(self, dist_name='norm', ax=plt):
        # TODO: Make lognorn dsit property. Done manually with
        #       log operation on data with norm distribution
        #       because scipy's lognorn dist buggy iwht param fit.
        if dist_name == 'lognorm':
            col = np.log(self.col + 1)
            dist_name = 'norm'
        else:
            col = self.col
        params = getattr(self, '_'.join([dist_name, 'params']))
        dist = self.dists[dist_name]
        probplot(col, sparams=params, dist=dist, plot=ax)

        return plt


# TODO: Write equivalent function that loops through all
#       distributions for a single column
# TODO: Jupyter kernel crashes with this occasionally
def plot_dists_pps(df, dist_name='lognorm', fillna=0):
    # df can be list-like, Series or dfFrame
    df = pd.DataFrame(df)
    df = df.select_dtypes(['float', 'int'])
    df = df.fillna(fillna)
    nrows = df.shape[1]
    fig, axs = plt.subplots(nrows=nrows, ncols=2, figsize=(9, 5.5 * nrows))

    axs = [axs] if nrows == 1 else axs
    for i, ax in enumerate(axs):
        col = df.iloc[:, i]
        sns.distplot(col, ax=ax[0])
        pp = ProbPlots(col)
        pp.plot_probplot(dist_name, ax=ax[1])
