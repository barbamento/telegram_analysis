from telethon.sync import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types.messages import ChannelMessages
from telethon import TelegramClient as T
from key.key import API
from typing import Union
import pandas as pd
import os

with TelegramClient("tmp", API().app_id, API().api_hash) as client:
    client: T=client
    channel_username='bestimeline' # your channel
    channel_entity=client.get_entity(channel_username)
    posts=pd.DataFrame()
    limit=100
    offset=1+limit
    i=0
    keep_on_going=True
    while keep_on_going:
        tmp:ChannelMessages = client(GetHistoryRequest(
            peer=channel_entity,
            limit=100,
            offset_date=None,
            offset_id=offset,
            max_id=0,
            min_id=0,
            add_offset=0,
            hash=0))
        results=pd.DataFrame().from_records(tmp.to_dict()["messages"])
        try:
            keep_on_going=results["date"].to_list()[-1]!=posts["date"].to_list()[-1]
        except:
            keep_on_going=True
        if keep_on_going:
            print(i,results["date"].to_list()[-1])
            i+=1
            try:
                posts=pd.concat([posts,results])
            except:
                if not os.path.exists(f"errors/{channel_username}"):
                    os.mkdir(f"errors/{channel_username}")
                    files=os.listdir(f"errors/{channel_username}/")
                    results.to_csv(f"errors/{channel_username}/{i}.csv")
            offset+=limit
        else:
            print(i,results["date"].to_list()[-1],posts["date"].to_list()[-1])
    files=os.listdir(f"errors/{channel_username}/")
    files=[pd.read_csv(f"errors/{channel_username}/"+i) for i in files]
    posts=pd.concat([posts]+files,ignore_index=True)
    #posts=posts.drop_duplicates()
    print(posts)
    posts.to_csv(f"db/{channel_username}.csv")


