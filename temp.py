import pstats

p = pstats.Stats("stats.txt")
p.strip_dirs().sort_stats("cumulative").print_stats(20)

p.print_callers(20, "__getitem__")


'''
Mon Oct  9 10:28:30 2023    stats.txt

         50733743 function calls (50282829 primitive calls) in 43.910 seconds

   Ordered by: cumulative time
   List reduced from 6061 to 20 due to restriction <20>

   ncalls  tottime  percall  cumtime  percall filename:lineno(function)
    696/1    0.004    0.000   43.910   43.910 {built-in method builtins.exec}
        1    0.006    0.006   43.910   43.910 main.py:1(<module>)
        1    0.000    0.000   40.838   40.838 main.py:95(main)
        1    0.000    0.000   37.367   37.367 utils.py:13(timediff)
        1    0.998    0.998   37.367   37.367 utils.py:104(run_simulation)
        1    0.002    0.002   14.107   14.107 utils.py:82(compute_meta_info)
   553876    1.559    0.000   13.260    0.000 frame.py:3592(_get_value)
   454581    0.730    0.000   11.980    0.000 indexing.py:2262(__getitem__)
   454581    0.672    0.000   10.647    0.000 indexing.py:2211(__getitem__)
       30    0.000    0.000    9.967    0.332 rolling.py:529(_apply)
       30    0.000    0.000    9.966    0.332 rolling.py:434(_apply_blockwise)
       30    0.000    0.000    9.966    0.332 rolling.py:415(_apply_series)
       30    0.000    0.000    9.962    0.332 rolling.py:564(homogeneous_func)
       30    0.000    0.000    9.961    0.332 rolling.py:570(calc)
       10    0.000    0.000    9.959    0.996 rolling.py:1822(apply)
       10    0.000    0.000    9.959    0.996 rolling.py:1274(apply)
   355383    1.207    0.000    9.959    0.000 datetimes.py:622(get_loc)
       10    0.316    0.032    9.956    0.996 rolling.py:1338(apply_func)
   149606    0.605    0.000    9.848    0.000 indexing.py:954(__getitem__)
79638/66576    0.201    0.000    7.287    0.000 {built-in method numpy.core._multiarray_umath.implement_array_function}


   Ordered by: cumulative time
   List reduced from 6061 to 20 due to restriction <20>
   List reduced from 20 to 3 due to restriction <'__getitem__'>

Function                       was called by...
                                   ncalls  tottime  cumtime
indexing.py:2262(__getitem__)  <-   48995    0.084    1.752  alpha1.py:38(compute_signal_distribution)
                                   140492    0.217    2.986  utils.py:31(get_pnl_stats)
                                   265094    0.429    7.242  utils.py:104(run_simulation)
indexing.py:2211(__getitem__)  <-  454581    0.672   10.647  indexing.py:2262(__getitem__)
indexing.py:954(__getitem__)   <-   50260    0.141    4.160  rolling.py:1338(apply_func)
                                    49046    0.235    2.755  utils.py:104(run_simulation)
                                    50300    0.230    2.932  utils.py:114(<listcomp>)


==============================================================
82                                               @profile
83                                               def compute_meta_info(self, trade_range):
84         1      87531.4  87531.4      0.7          self.pre_compute(trade_range=trade_range)
85                                           
86        11         11.5      1.0      0.0          for inst in self.insts:
87        10       2018.8    201.9      0.0              df = pd.DataFrame(index=trade_range)
88        10      10876.4   1087.6      0.1              inst_vol = (-1 + self.dfs[inst]["close"] / self.dfs[inst]["close"].shift(1)).rolling(30).std()
89        10      18046.4   1804.6      0.1              self.dfs[inst] = df.join(self.dfs[inst]).fillna(method="ffill").fillna(method="bfill")
90        10      14323.8   1432.4      0.1              self.dfs[inst]["ret"] = -1 + self.dfs[inst]["close"] / self.dfs[inst]["close"].shift(1)
91        10      12614.9   1261.5      0.1              self.dfs[inst]["vol"] = inst_vol
92        10       5312.0    531.2      0.0              self.dfs[inst]["vol"] = self.dfs[inst]["vol"].fillna(method="ffill").fillna(0)
93        10       3509.0    350.9      0.0              self.dfs[inst]["vol"] = np.where(self.dfs[inst]["vol"] < 0.005, 0.005, self.dfs[inst]["vol"])
94        10       5059.6    506.0      0.0              sampled = self.dfs[inst]["close"] != self.dfs[inst]["close"].shift(1).fillna(0)
95        10    8471460.9 847146.1     69.0              eligible = sampled.rolling(5).apply(lambda x: int(np.any(x))).fillna(0)
96        10      13045.2   1304.5      0.1              self.dfs[inst]["eligible"] = eligible.astype(int) & (self.dfs[inst]["close"] > 0).astype(int)
97                                           
98         1    3633764.4    4e+06     29.6          self.post_compute(trade_range=trade_range)
99         1          0.4      0.4      0.0          return 
'''                                    