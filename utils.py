import lzma
import dill as pickle
import pandas as pd
import numpy as np
import random
from copy import deepcopy
from collections import defaultdict
import time
from functools import wraps


def timeme(func):
    @wraps(func)
    def timediff(*args, **kwargs):
        a = time.time()
        result = func(*args, **kwargs)
        b = time.time()
        print(f"@timeme: {func.__name__} took {b - a} seconds")
        return result
    return timediff

def load_pickle(path):
    with lzma.open(path, "rb") as fp:
        file = pickle.load(fp)
    return file

def save_pickle(path, obj):
    with lzma.open(path, "wb") as fp:
        pickle.dump(obj, fp)

def get_pnl_stats(date, prev, portfolio_df, insts, idx, dfs):
    day_pnl = 0
    nominal_ret = 0
    for inst in insts:
        units = portfolio_df.at[idx - 1, "{} units".format(inst)]
        if units != 0:
            delta = dfs[inst].at[date, "close"] - dfs[inst].at[prev, "close"]
            inst_pnl = delta * units
            day_pnl += inst_pnl
            nominal_ret += portfolio_df.at[idx -1, "{} w".format(inst)] * dfs[inst].at[date, "ret"]

    capital_ret = nominal_ret * portfolio_df.at[idx - 1, "leverage"]
    portfolio_df.at[idx, "capital"] = portfolio_df.at[idx -1, "capital"] + day_pnl
    portfolio_df.at[idx, "day_pnl"] = day_pnl
    portfolio_df.at[idx, "nominal_ret"] = nominal_ret
    portfolio_df.at[idx, "capital_ret"] = capital_ret
    return day_pnl, capital_ret


class AbstractImplementationException(Exception):
    pass


class Alpha():
    
    def __init__(self, insts, dfs, start, end, portfolio_vol=0.20):
        self.insts = insts
        self.dfs = deepcopy(dfs)
        self.start = start
        self.end = end
        self.portfolio_vol = portfolio_vol

    def init_portfolio_settings(self, trade_range):
        portfolio_df = pd.DataFrame(index=trade_range)\
            .reset_index()\
            .rename(columns={"index":"datetime"})
        portfolio_df.at[0, "capital"] = 10000
        portfolio_df.at[0, "day_pnl"] = 0.0
        portfolio_df.at[0, "capital_ret"] = 0.0
        portfolio_df.at[0, "nominal_ret"] = 0.0
        return portfolio_df

    def pre_compute(self, trade_range):
        pass

    def post_compute(self, trade_range):
        pass

    def compute_signal_distribution(self, eligibles, date):
        raise AbstractImplementationException("No concrete implementation for signal generation")

    def compute_meta_info(self, trade_range):
        self.pre_compute(trade_range=trade_range)

        for inst in self.insts:
            df = pd.DataFrame(index=trade_range)
            inst_vol = (-1 + self.dfs[inst]["close"] / self.dfs[inst]["close"].shift(1)).rolling(30).std()
            self.dfs[inst] = df.join(self.dfs[inst]).fillna(method="ffill").fillna(method="bfill")
            self.dfs[inst]["ret"] = -1 + self.dfs[inst]["close"] / self.dfs[inst]["close"].shift(1)
            self.dfs[inst]["vol"] = inst_vol
            self.dfs[inst]["vol"] = self.dfs[inst]["vol"].fillna(method="ffill").fillna(0)
            self.dfs[inst]["vol"] = np.where(self.dfs[inst]["vol"] < 0.005, 0.005, self.dfs[inst]["vol"])
            sampled = self.dfs[inst]["close"] != self.dfs[inst]["close"].shift(1).fillna(0)
            eligible = (sampled.rolling(5).sum() > 0).astype(int).fillna(0)
            self.dfs[inst]["eligible"] = eligible.astype(int) & (self.dfs[inst]["close"] > 0).astype(int)

        self.post_compute(trade_range=trade_range)
        return 

    def get_strat_scalar(self, target_vol ,ewmas ,ewstrats):
        ann_realized_vol = np.sqrt(ewmas[-1] * 253)
        return target_vol / ann_realized_vol * ewstrats[-1]
        
    @timeme
    @profile
    def run_simulation(self):
        print("running sim")
        date_range = pd.date_range(start=self.start, end=self.end, freq="D")
        self.compute_meta_info(trade_range=date_range)
        portfolio_df = self.init_portfolio_settings(trade_range=date_range)
        self.ewmas, self.ewstrats = [0.01], [1]
        self.strat_scalars = []
        for i in portfolio_df.index:
            date = portfolio_df.at[i, "datetime"]
            eligibles = [inst for inst in self.insts if self.dfs[inst].loc[date, "eligible"] == 1]
            non_eligibles = [inst for inst in self.insts if inst not in eligibles]
            strat_scalar = 2

            if i != 0:
                date_prev = portfolio_df.at[i-1, "datetime"]

                strat_scalar = self.get_strat_scalar(
                    target_vol = self.portfolio_vol,
                    ewmas = self.ewmas,
                    ewstrats = self.ewstrats
                )
                day_pnl, capital_ret = get_pnl_stats(
                    date=date,
                    prev=date_prev,
                    portfolio_df=portfolio_df,
                    insts=self.insts,
                    idx=i,
                    dfs=self.dfs
                )
                self.ewmas.append(0.06 * (capital_ret**2) + 0.94  * self.ewmas[-1] if capital_ret != 0 else self.ewmas[-1])
                self.ewstrats.append(0.06 * strat_scalar + 0.94 * self.ewstrats[-1] if capital_ret != 0 else self.ewstrats[-1])

            self.strat_scalars.append(strat_scalar)

 
            forecasts, forecast_chips = self.compute_signal_distribution(eligibles, date)
            
            for inst in non_eligibles:
                portfolio_df.at[i, "{} w".format(inst)] = 0
                portfolio_df.at[i, "{} units".format(inst)] = 0

            vol_target = self.portfolio_vol / np.sqrt(253) * portfolio_df.at[i, "capital"]

            nominal_tot = 0    

            for inst in eligibles:
                forecast = forecasts[inst]
                scaled_forecast = forecast / forecast_chips if forecast_chips != 0 else 0

                dollar_allocation = portfolio_df.at[i, "capital"] / forecast_chips if forecast_chips != 0 else 0

                position = strat_scalar * scaled_forecast * vol_target / (self.dfs[inst].at[date, "vol"] * self.dfs[inst].loc[date,"close"])

                portfolio_df.at[i, inst + " units"] = position
                nominal_tot += abs(position * self.dfs[inst].at[date,"close"])

            for inst in eligibles:
                units = portfolio_df.at[i, inst + " units"]
                nominal_inst = units * self.dfs[inst].at[date, "close"]
                inst_w = nominal_inst / nominal_tot
                portfolio_df.at[i, inst + " w"] = inst_w

            portfolio_df.at[i, "nominal"] = nominal_tot
            portfolio_df.at[i, "leverage"] = nominal_tot / portfolio_df.at[i, "capital"]
            if i %100 == 0: print(portfolio_df.loc[i])
        return portfolio_df.set_index("datetime", drop=True)


class Portfolio(Alpha):

    def __init__ (self, insts, dfs, start, end, stratdfs):
        super().__init__(insts, dfs, start, end)
        self.stratdfs = stratdfs

    def post_compute(self, trade_range):
        self.positions = {}
        for inst in self.insts:
            inst_weights = pd.DataFrame(index=trade_range)
            for i in range(len(self.stratdfs)):
                inst_weights[i] = self.stratdfs[i]["{} w".format(inst)]\
                    * self.stratdfs[i]["leverage"]
                inst_weights[i] = inst_weights[i].fillna(method="ffill").fillna(0.0)
            self.positions[inst] = inst_weights
        return

    def compute_signal_distribution(self, eligibles, date):
        forecasts = defaultdict(float)
        for inst in self.insts:
            for i in range(len(self.stratdfs)):
                forecasts[inst] += self.positions[inst].loc[date, i] * (1/len(self.stratdfs))
                # risk parity alloc
        return forecasts, np.sum(np.abs(list(forecasts.values())))
