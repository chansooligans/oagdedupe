from abc import ABCMeta, abstractmethod


class BaseBlocker(metaclass=ABCMeta):
    """ Abstract base class for all blockers to inherit
    """

    @abstractmethod
    def get_block_maps(self, df, attributes):
        return


class BaseBlockAlgo(metaclass=ABCMeta):
    """ Abstract base class for all blocking algos to inherit
    """

    @abstractmethod
    def get_block(self):
        return


class BaseDistance(metaclass=ABCMeta):
    """ Abstract base class for all distance configurations to inherit
    """

    @abstractmethod
    def distance(self):
        return

    # @abstractmethod
    # def config(self):
    #     return


class BaseCluster(metaclass=ABCMeta):
    """ Abstract base class for all clustering algos to inherit
    """

    @abstractmethod
    def get_df_cluster(self):
        return
