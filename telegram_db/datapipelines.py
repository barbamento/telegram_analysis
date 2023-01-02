import pandas as pd
import numpy as np
from typing import List
import igraph as ig


class channel:
    def __init__(self,path:str) -> None:
        self.df=pd.read_csv(path)
        self.id=path.split("/")[-1].removeprefix("fwd_").removesuffix(".csv")

    def create_forwards(self)->List[int]:
        return self.df["from_channel_id"].value_counts()

class graph:
    def __init__(self):
        pass