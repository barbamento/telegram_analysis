from telethon.sync import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types.messages import ChannelMessages
from telethon import TelegramClient as T
from key.key import API
import pandas as pd
import numpy as np
import datetime
import os
from typing import Union,Iterable,Callable,Any
import shutil
import re
from db_interface import database_postgres as db
from db_interface import columns as db_columns

class Telegram_Scraper:
    paths={
        "database_path":"db",
        "dataset_path":"dataset",
        "error_path":"errors",
    }
    
    def __init__(self,start_from:Union[str,int],**args):
        self.db=db("telegram_scraper","127.0.0.1","admin","admin")
        self.pd_conn=self.db.get_engine()
        self.inizialization(**args)
        with TelegramClient("tmp", API().app_id, API().api_hash) as client:
            channel_entities=[client.get_entity(start_from)]
        channel_name=channel_entities[0].title
        distance_from_start=0
        main=True
        while main:
            forwarded_channels=[]
            for public_channel in channel_entities:
                print(f"Working on {public_channel.title}, at distace {distance_from_start} from {channel_name}")
                self.dump_channels(public_channel)
                print(f"Channel {public_channel.title} dumped")
                forwarded_channels+=self.get_forwarded_channels(public_channel)
            if forwarded_channels==[]:
                print(f"Job's done. Distance reached : {distance_from_start}")
                raise ValueError("Forwarded channel is empty. No channel to work on")
            forwarded_channels=set(forwarded_channels)
            distance_from_start+=1

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
        try:
            pd.read_sql("select * from dumped_channels where 1=2",con=self.pd_conn)
        except Exception as e:
            print(e)
            self.db.create_table(table_name="dumped_channels",columns={"id":"integer","name":"varchar","datetime":"timestamp"})

        print("directory loaded & cleaned")

    def dump_channels(self,channel_entity,limit:int=100):
        channel_username=channel_entity.title
        channel_id=channel_entity.id
        existing_channels=self.get_existing_channels(conn=self.pd_conn)
        if (
            (channel_username in existing_channels["name"].to_list()) 
            or (channel_id in existing_channels["id"].to_list())
        ): #Questo IF setta l'offset iniziale
            offset=pd.read_sql(
                f"SELECT id FROM id_{channel_id}",con=self.pd_conn
            )["id"].max()+1+limit
            print(offset)
        else:
            pd.DataFrame(
                [[channel_id,channel_username,datetime.datetime.now()]],
                columns=["id","name","datetime"],
            ).to_sql("dumped_channels",con=self.pd_conn,index=False,if_exists="append")
            self.db.create_telegram_table(f"id_{channel_id}")
            offset=1+limit
        keep_on_going=True
        with TelegramClient("tmp", API().app_id, API().api_hash) as client:
            while keep_on_going:
                tmp:ChannelMessages = client(GetHistoryRequest(
                    peer=channel_entity,
                    limit=100,
                    offset_date=None,
                    offset_id=offset,
                    max_id=0,
                    min_id=offset-limit-1,
                    add_offset=0,
                    hash=0))
                results=pd.DataFrame(columns=db_columns.keys()).from_records(tmp.to_dict()["messages"])
                results=results.sort_values(by="id",ascending=True)
                results=results.astype(dict([(i,"string") for i in db_columns.keys() if ((db_columns[i] in ["varchar","text"]) and (i in results.columns)) ]))
                keep_on_going=results["id"].min()>=offset-limit
                results=results[results["id"]>=offset-limit]
                offset=results["id"].max()+1+limit
                results.to_sql(f"id_{channel_id}",con=self.pd_conn,index=False,if_exists="append")

    def get_forwarded_channels(self,channel_entity) -> list[Any]:
        channel_id=channel_entity.id
        query=(
            f"SELECT from_id FROM ID_{channel_id}"
            "WHERE from_id IS NOT NULL"
        )
        df=pd.read_sql(query,con=self.pd_conn)
        with TelegramClient("tmp", API().app_id, API().api_hash) as client:
            channel_entities=[client.get_entity(i) for i in df["from_id"]]
        return channel_entities

    def get_existing_channels(self,conn) -> pd.DataFrame:
        df=pd.read_sql("select * from dumped_channels",con=conn)
        return df

if __name__=="__main__":
    pm="PoliticalMemes"
    bt="bestimeline"
    Telegram_Scraper(bt)