from utils import Alpha
import numpy as np
import pandas as pd

class Alpha2(Alpha):

    def __init__(self, insts, dfs, start, end):
        super().__init__(insts, dfs, start, end)
    
    
    def pre_compute(self, trade_range):
        self.alphas = {}
        for inst in self.insts:
            inst_df = self.dfs[inst]
            alpha = -1 * (1-(inst_df.open/inst_df.close)).rolling(12).mean()
            self.alphas[inst] = alpha
        return


    def post_compute(self, trade_range):
        for inst in self.insts:
            self.dfs[inst]["alpha"] = self.alphas[inst]
            self.dfs[inst]["alpha"] = self.dfs[inst]["alpha"].fillna(method="ffill")
            self.dfs[inst]["eligible"] = self.dfs[inst]["eligible"] \
                & (~pd.isna(self.dfs[inst]["alpha"])) 
        return

    def compute_signal_distribution(self, eligibles, date):
        forecasts = {}

        for inst in eligibles:
            forecasts[inst] = self.dfs[inst].loc[date, "alpha"]
        return forecasts, np.sum(np.abs(list(forecasts.values())))