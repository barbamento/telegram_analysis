from telethon.sync import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types.messages import ChannelMessages
from telethon.tl.types import TypeInputPeer
from telethon import TelegramClient as T
from key.key import API
import pandas as pd
import numpy as np
import os
from typing import Union,Iterable,Callable
import shutil
import re

class Telegram_Scraper:
    paths={
        "database_path":"db",
        "dataset_path":"dataset",
        "error_path":"errors",
    }
    def __init__(self,start_from:Union[str,int],**args):
        self.inizialization(**args)
        with TelegramClient("tmp", API().app_id, API().api_hash) as client:
            channel_entities=[client.get_entity(start_from)]
        channel_name=channel_entities[0].title
        d=0
        while 1:

            forwarded_channels=[]
            for public_channel in channel_entities:
                print(f"Working on {public_channel.title}, at distace {d} from {channel_name}")
                dump=self.dump_channels(public_channel)
                print(f"Channel {public_channel.title} dumped")
                if "fwd_from" in dump.columns:
                    forwarded_channels+=list(np.unique([
                        i.split("channel_id': ")[1].split("}")[0]
                        for i in dump[dump["fwd_from"].notna()]["fwd_from"] 
                        if "channel_id" in i
                    ]))
            forwarded_channels=self.check_forwarded(forwarded_channels)
            print(forwarded_channels)
            exit()
            d+=1
        print("exiting from the job")

    def check_forwarded(self,forwarded_channels_id:Iterable[int]):
        existing_channel=[i.removesuffix(".csv") for i in os.listdir("db")]
        final_results=[]
        for i in forwarded_channels_id:
            with TelegramClient("tmp", API().app_id, API().api_hash) as client:
                try:
                    channel=client.get_entity(int(i))
                    channel_name=re.sub('/', ' ', channel.title)
                    if not channel_name in existing_channel:
                        print(f"{channel.title} (id : {i})is a new channel. I'm dumping it")
                        final_results+=[channel]
                except Exception as e:
                    print(f"Can't dump {i} for the following reason: {e}. I'm skipping it")
        
        return final_results
        
    def search_for_channels(self,df:pd.DataFrame,dumped_channel):
        """
        Finds a new channel to dump

        Old version. Check if i should keep it or not
        """
        existing_channel=[i.removesuffix(".csv") for i in os.listdir("db")]
        last_dumped_channel=dumped_channel
        if "fwd_from" in df.columns:
            forwarded_channels=np.unique([
                i.split("channel_id': ")[1].split("}")[0]
                for i in df[df["fwd_from"].notna()]["fwd_from"] 
                if "channel_id" in i
            ])
            for i in forwarded_channels:
                with TelegramClient("tmp", API().app_id, API().api_hash) as client:
                    try:
                        channel=client.get_entity(int(i))
                        channel_name=re.sub('/', ' ', channel.title)
                        if not channel_name in existing_channel:
                            print(f"{channel.title} (id : {i}) is a new channel. I'm dumping it")

                    except:
                        print(f"{i} is a private channel. I'm skipping it")
        else:
            print("no fwd_from in columns")
        print(f"No usable forwarded channel in {dumped_channel.title}")
        for i in os.listdir("db"):
            df=pd.read_csv(f"db/{i}")
            if "fwd_from" in df.columns:
                forwarded_channels=np.unique([
                    i.split("channel_id': ")[1].split("}")[0]
                    for i in df[df["fwd_from"].notna()]["fwd_from"] 
                    if "channel_id" in i
                ])
                for c in forwarded_channels:
                    with TelegramClient("tmp", API().app_id, API().api_hash) as client:
                        try:
                            channel=client.get_entity(int(c))
                            channel_name=re.sub('/', ' ', channel.title)
                            if (not channel_name in existing_channel) & (channel_name!=last_dumped_channel.title):
                                print(f"{channel.title} (id : {c})is a new channel. I'm dumping it")
                                return channel
                        except Exception as e:
                            print(f"Can't dump {c} for the following reason: {e}. I'm skipping it")
        raise ValueError ("No channel available")
            
    def inizialization(self,**args):
        """
        Create all the directory needed for the script & delete all the temporary folder
        """
        for i in self.paths.keys():
            if i in args.keys():
                self.paths=args[i]
            if not os.path.exists(self.paths[i]):
                os.mkdir(self.paths[i])
        if os.listdir("errors")!=[]:
            for i in os.listdir("errors"):
                shutil.rmtree(f"errors/{i}",ignore_errors=True)
        print("directory loaded & cleaned")

    def dump_channels(self,channel_entity,limit:int=100,safe_mode:bool=True):#riscrivi usando la channel entity al posto del nome
        channel_username=re.sub('/', ' ', channel_entity.title)
        if channel_username=="bestimeline":
            return pd.read_csv("db/bestimeline.csv")
        existing_channels=[i.removesuffix(".csv") for i in os.listdir(self.paths["database_path"])]
        if channel_username in existing_channels:
            print("Channel already exists. Updating existing files")
            posts=pd.read_csv(f"{self.paths['database_path']}/{channel_username}.csv")
            if safe_mode:
                return posts
            offset=max(posts["id"])+limit
        else:
            print(f"{channel_username} is a new channel. Dumping all messages")
            posts=pd.DataFrame()
            offset=1+limit
        with TelegramClient("tmp", API().app_id, API().api_hash) as client:
            client: T=client
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
                if i!=0:
                    keep_on_going=results["date"].to_list()[-1]!=last_iteration_data
                else:
                    keep_on_going=True
                last_iteration_data=results["date"].to_list()[-1]
                if keep_on_going:
                    print(i,results["date"].to_list()[-1])
                    i+=1
                    try:
                        posts=pd.concat([posts,results])
                    except:
                        if not os.path.exists(f"errors/{channel_username}"):
                            os.mkdir(f"errors/{channel_username}")
                        results.to_csv(f"errors/{channel_username}/{i}.csv")
                    offset+=limit
                else:
                    print(i,results["date"].to_list()[-1],posts["date"].to_list()[-1])
            if os.path.exists(f"errors/{channel_username}"):
                files=os.listdir(f"errors/{channel_username}/")
                files=[pd.read_csv(f"errors/{channel_username}/"+i) for i in files]
                posts=pd.concat([posts]+files,ignore_index=True)
                shutil.rmtree(f"errors/{channel_username}",ignore_errors=True)
            posts.to_csv(f"db/{channel_username}.csv")
        return posts

if __name__=="__main__":
    pm="PoliticalMemes"
    bt="bestimeline"
    Telegram_Scraper(bt)