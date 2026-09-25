"""Bounded, interpretable rolling baselines."""
from collections import deque
from math import sqrt
class Baseline:
    def __init__(self, max_observations=180, minimum_observations=20):
        self.values=deque(maxlen=max_observations); self.minimum_observations=minimum_observations
    def add(self, value):
        if value is not None: self.values.append(float(value))
    @property
    def ready(self): return len(self.values)>=self.minimum_observations
    @property
    def mean(self): return sum(self.values)/len(self.values) if self.values else None
    @property
    def std(self):
        if len(self.values)<2:return 0.0
        m=self.mean; return sqrt(sum((x-m)**2 for x in self.values)/(len(self.values)-1))
    def zscore(self,value):
        if not self.ready or self.std==0:return None
        return (float(value)-self.mean)/self.std
    def percentile(self,value):
        if not self.values:return None
        return sum(x<=float(value) for x in self.values)/len(self.values)
    def ewma(self,alpha=.2):
        result=None
        for value in self.values: result=float(value) if result is None else alpha*float(value)+(1-alpha)*result
        return result
