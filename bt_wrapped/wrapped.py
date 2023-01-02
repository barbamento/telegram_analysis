import pandas as pd
import datetime
import os
import matplotlib.pyplot as plt
import seaborn as sns
from wordcloud import WordCloud
import re
from key.key import API
from telethon.sync import TelegramClient


def json_to_csv(path:str)->None:
    """
    Returns a csv of the Telegram messages in the same folder as the json
    """
    df = pd.read_json(path)
    print(df.columns)
    print(df.iloc[-1,:])
    df = pd.DataFrame().from_records(df["messages"])
    df.to_csv(path.removesuffix(".json")+".csv",index=False)

class wrapped:
    channel_json_path="bt_wrapped/data/channel.json"
    channel_csv_path=channel_json_path.removesuffix(".json")+".csv"
    group_json_path="bt_wrapped/data/group.json"
    group_csv_path=group_json_path.removesuffix(".json")+".csv"

    def __init__(self)->None:
        if not os.path.exists(self.channel_csv_path):
            json_to_csv(self.channel_json_path)
        if not os.path.exists(self.group_csv_path):
            json_to_csv(self.group_json_path)
        self.channel_df=pd.read_csv(self.channel_csv_path,index_col=False)
        self.group_df=pd.read_csv(self.group_csv_path,index_col=False)
        self.channel_df=self.channel_df[pd.to_datetime(self.channel_df["date"])>=datetime.datetime(2022,1,1,0,0,0)]
        self.group_df=self.group_df[pd.to_datetime(self.group_df["date"])>=datetime.datetime(2022,1,1,0,0,0)]
        self.replies_only()
        print("job's done")

    def most_replied_posts(self):
        df=self.replies_df.groupby("root").size().sort_values(ascending=False)
        most_replied_post=pd.DataFrame(index=df.head(20).index,columns=["link"])
        most_replied_post["link"]=["https://t.me/c/1155308424/"+str(i) for i in df.head(20).index]
        print(most_replied_post)

    def most_reacted_post(self):
        def get_reactions_from_post_id(chat_id, message_id):
            with TelegramClient("tmp", API().app_id, API().api_hash) as client:
                print("client created")
                message = client.get_messages(chat_id, message_id)
                print(message)
                post_reactions = message.reactions
                print(post_reactions)
                reactions = {}
                for react in post_reactions.reactions:
                    reactions[react.emoji] = react.count
            return reactions
        print(self.channel_df.iloc[-1,:])
        print(get_reactions_from_post_id(1486151887, 21070))

    def most_active_day(self) -> pd.Series:
        df=pd.DataFrame()
        df["date"]=self.group_df["date"].str.split("T").str[0]
        df=df.groupby("date").size()
        print(df.sort_values(ascending=False).head(50).sort_index())
        g=sns.lineplot(data=df)
        g.set_xticklabels(labels=[])
        plt.savefig("yearly_recap.svg")
        return df.sort_values(ascending=False).head(50).sort_index()
    
    def word_cloud(self,df:pd.DataFrame,filename:str,column:str="text"):
        def remove_list(i):
            with open("bt_wrapped/wordcloud/stopwords.txt", "r") as f:
                stopwords = f.read().split("\n")
            if isinstance(i, list):
                t = ""
                for j in i:
                    if isinstance(j, str):
                        t += j
                    elif isinstance(j, dict):
                        if j["type"] not in [
                            "link",
                            "text_link",
                            "code",
                            "bot_command",
                            "mention",
                            "email",
                            "phone",
                            "pre",
                            "cashtag",
                            "mention_name",
                            "hashtag",
                            "bank_card",
                            "strikethrough",
                        ]:
                            t += j["text"]
                return " ".join([j for j in re.sub(r'[^\w\s]', '',t).lower().split(" ") if j not in stopwords])
            elif isinstance(i, str):
                return " ".join([j for j in re.sub(r'[^\w\s]', '',i).lower().split(" ") if j not in stopwords])
            else:
                raise TypeError(f"{i} is of {type(i)}. Not supported")
        df[column] = df[column].fillna("").apply(lambda x: remove_list(x))
        plt.close()
        wc=WordCloud(width=800,height=600).generate(re.sub(r'[^\w\s]', '',df[column].sum()))
        plt.imshow(wc,interpolation="bilinear")
        wc.to_file(f"bt_wrapped/wordcloud/{filename}.png")

    def most_active_time(self):
        df=self.group_df
        df["date"]=df["date"].str.split("T").str[1].str.split(":").str[0]+":"+df["date"].str.split("T").str[1].str.split(":").str[1]
        df=df.groupby("date").size()
        g=sns.lineplot(data=df)
        print(df)
        plt.savefig("time.pdf")

    def replies_only(self):
        roots = self.group_df[
            (self.group_df["from"] == "Best Timeline")
            &(self.group_df["forwarded_from"] == "Best Timeline")
        ]
        roots["root"] = roots["id"]
        results = roots
        ids = roots
        j = 0
        while not ids.empty:
            tmp = self.group_df[self.group_df["reply_to_message_id"].isin(ids["id"])]
            right = results.loc[:, ["id", "root"]].rename({"id": "root_id"}, axis=1)
            tmp = tmp.merge(right, left_on="reply_to_message_id", right_on="root_id").drop(
                "root_id", axis=1
            )
            ids = tmp
            results = pd.concat([results, tmp])
            j += 1
        self.replies_df=results
        

    def admins(self):
        admin_posts_raw=self.channel_df.groupby("author").size().sort_values(ascending=False)
        admin_posts_processed=pd.DataFrame(columns=["value"])
        for i in admin_posts_raw.index:
            if "cat" in i and "drunken" in i:
                j="drunken cat"
            elif i in ["Pétta️️️️    ","Pétta","Pétta️️️️"]:
                j="petta"
            elif "golden" in i:
                j="golden doggo"
            elif "Gianni" in i:
                j="Gianni Confuso"
            else:
                j=i
            if not j in admin_posts_processed.index:
                admin_posts_processed.loc[j,:]=admin_posts_raw.loc[i]
            else:
                admin_posts_processed.loc[j,:]+=admin_posts_raw.loc[i]
        print(admin_posts_processed)
        plt.close()
        admin_posts_processed.plot.pie(subplots=True,legend=False)
        plt.savefig("bt_wrapped/admins_pie.svg")
        plt.close()
        comments_generated=pd.DataFrame()
        comments_generated["size"]=self.replies_df.groupby("root").size()
        comments_generated["text"]=self.replies_df.loc[:,["root","text"]].groupby("root").first()
        comments_generated=comments_generated.drop_duplicates("text")
        post_made=self.channel_df.loc[:,["text","author"]]
        post_made=post_made.merge(right=comments_generated,on=["text"],how="inner")
        post_made=post_made.loc[:,["author","size"]].groupby("author").sum()
        comments_generated=pd.DataFrame(columns=["size"])
        for i in post_made.index:
            if "cat" in i and "drunken" in i:
                j="drunken cat"
            elif i in ["Pétta️️️️    ","Pétta","Pétta️️️️"]:
                j="petta"
            elif "golden" in i:
                j="golden doggo"
            elif "Gianni" in i:
                j="Gianni Confuso"
            else:
                j=i
            if not j in comments_generated.index:
                comments_generated.loc[j,:]=post_made.loc[i,"size"]
            else:
                comments_generated.loc[j,:]+=post_made.loc[i,"size"]
        print(comments_generated)

if __name__=="__main__":
    w=wrapped()
    w.most_reacted_post()