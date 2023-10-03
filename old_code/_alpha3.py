import pandas as pd
import numpy as np
from utils import get_pnl_stats


class Alpha3():
    
    def __init__(self, insts, dfs, start, end):
        self.insts = insts
        self.dfs = dfs
        self.start = start
        self.end = end

    def init_portfolio_settings(self, trade_range):
        portfolio_df = pd.DataFrame(index=trade_range)\
            .reset_index()\
            .rename(columns={"index":"datetime"})
        portfolio_df.loc[0, "capital"] = 10000
        return portfolio_df

    def compute_meta_info(self, trade_range):
        """
         ma_faster > ma_slower ? buy : 0
         1. fast_crossover
         2. medium_crossover
         3. slow_crossover
        plus (
            mean_10(close) > mean_20(close),
            mean_20(close) > mean_100(close),
            mean_50(close) > mean_200(close),
        )
        0 1 2 3
        """
        for inst in self.insts:
            df = pd.DataFrame(index=trade_range)
            inst_df = self.dfs[inst]
            fast = np.where(inst_df.close.rolling(10).mean() > inst_df.close.rolling(50).mean(), 1, 0)
            medium = np.where(inst_df.close.rolling(20).mean() > inst_df.close.rolling(100).mean(), 1, 0)
            slow = np.where(inst_df.close.rolling(50).mean() > inst_df.close.rolling(200).mean(), 1, 0)
            alpha = fast + medium + slow
            self.dfs[inst]["alpha"] = alpha

            self.dfs[inst] = df.join(self.dfs[inst]).fillna(method="ffill").fillna(method="bfill")
            self.dfs[inst]["ret"] = -1 + self.dfs[inst]["close"] / self.dfs[inst]["close"].shift(1)
            self.dfs[inst]["alpha"] = self.dfs[inst]["alpha"].fillna(method="ffill")
            sampled = self.dfs[inst]["close"] != self.dfs[inst]["close"].shift(1).fillna(method="bfill")
            eligible = sampled.rolling(5).apply(lambda x: int(np.any(x))).fillna(0)
            self.dfs[inst]["eligible"] = eligible.astype(int) \
                & (self.dfs[inst]["close"] > 0).astype(int) \
                & (~pd.isna(self.dfs[inst]["alpha"])) 
        return 

    def run_simulation(self):
        print("running sim")
        date_range = pd.date_range(start=self.start, end=self.end, freq="D")
        self.compute_meta_info(trade_range=date_range)
        portfolio_df = self.init_portfolio_settings(trade_range=date_range)
        for i in portfolio_df.index:
            date = portfolio_df.loc[i, "datetime"]
            
            eligibles = [inst for inst in self.insts if self.dfs[inst].loc[date, "eligible"] == 1]
            non_eligibles = [inst for inst in self.insts if inst not in eligibles]

            if i != 0:
                date_prev = portfolio_df.loc[i-1, "datetime"]
                day_pnl, capital_ret = get_pnl_stats(
                    date=date,
                    prev=date_prev,
                    portfolio_df=portfolio_df,
                    insts=self.insts,
                    idx=i,
                    dfs=self.dfs
                )

            alpha_scores = {}
            for inst in eligibles:
                alpha_scores[inst] = self.dfs[inst].loc[date, "alpha"]


            for inst in non_eligibles:
                portfolio_df.loc[i, "{} w".format(inst)] = 0
                portfolio_df.loc[i, "{} units".format(inst)] = 0

            
            #input(alpha_scores) 
            absolute_scores = np.abs([score for score in alpha_scores.values()])
            forecast_chips = np.sum(absolute_scores)
            nominal_tot = 0    

            for inst in eligibles:
                forecast = alpha_scores[inst]
                dollar_allocation = portfolio_df.loc[i, "capital"] / forecast_chips if forecast_chips != 0 else 0
                position = forecast * dollar_allocation / self.dfs[inst].loc[date, "close"]
                portfolio_df.loc[i, inst + " units"] = position
                nominal_tot += abs(position * self.dfs[inst].loc[date,"close"])

            for inst in eligibles:
                units = portfolio_df.loc[i, inst + " units"]
                nominal_inst = units * self.dfs[inst].loc[date, "close"]
                inst_w = nominal_inst / nominal_tot
                portfolio_df.loc[i, inst + " w"] = inst_w

            portfolio_df.loc[i, "nominal"] = nominal_tot
            portfolio_df.loc[i, "leverage"] = nominal_tot / portfolio_df.loc[i, "capital"]
            if i %100 == 0: print(portfolio_df.loc[i])
        return portfolio_df












