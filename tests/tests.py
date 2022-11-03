import pandas as pd

def check_merge():
    posts=pd.read_csv("posts.csv")
    results=pd.read_csv("results.csv")
    print(pd.concat([posts,results]))


if __name__=="__main__":
    check_merge()