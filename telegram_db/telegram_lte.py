from telethon.sync import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types.messages import ChannelMessages
from telethon.tl.types import PeerChannel
from telethon import TelegramClient as T
from key.key import API
import pandas as pd
import numpy as np
import datetime
import os
from typing import Union,Iterable,Callable,Any
import shutil
from tqdm import tqdm
import re
from db_interface import database_postgres as db
from db_interface import columns as db_columns

def processing_only_forwards(results:pd.DataFrame)->pd.DataFrame:
    """
    results_lte=results[results["fwd_from"].notna()]
    results_lte=pd.DataFrame.from_records(results_lte["fwd_from"].to_list())
    if not results_lte.empty:
        results_lte_nested=pd.DataFrame.from_records(results_lte[results_lte["from_id"].notna()]["from_id"].to_list())
        if not results_lte_nested.empty:
            try:
                if not "channel_id" in results_lte_nested:
                    results_lte=pd.DataFrame(columns=results_lte.columns.to_list()+["from_channel_id"])
                elif "user_id" in results_lte_nested.columns:
                    results_lte["from_channel_id"]=results_lte_nested[
                        (results_lte_nested["channel_id"].notna())
                        &(results_lte_nested["user_id"].isna())
                    ]["channel_id"]
                else:
                    results_lte["from_channel_id"]=results_lte_nested[results_lte_nested["channel_id"].notna()]["channel_id"]
            except Exception as e:
                print(e)
                results_lte.to_csv("errors_tmp.csv")
                raise ValueError("Error in generation of result_lte")
            results_lte.to_csv("result_lte.csv")
    """
    results.to_csv("1.csv")
    if not "fwd_from" in results.columns:
        print("now fwd_from")
        return pd.DataFrame()
    results_lte=pd.DataFrame.from_records(
        results[results["fwd_from"].notna()]["fwd_from"].to_list()
    )
    results_lte.to_csv("2.csv")
    if results_lte.empty:
        print("result_lte empty")
        return pd.DataFrame()
    results_lte_nested=pd.DataFrame.from_records(results_lte[results_lte["from_id"].notna()]["from_id"].to_list())
    results_lte_nested.to_csv("3.csv")
    if not "channel_id" in results_lte_nested.columns:
        print("now channel_id")
        return pd.DataFrame()
    results_lte_nested=results_lte_nested[results_lte_nested["channel_id"].notna()]    
    results_lte_nested.to_csv("4.csv")
    results_lte["from_channel_id"]=results_lte_nested["channel_id"]
    results_lte.to_csv("5.csv")
    results_lte=results_lte[results_lte["from_channel_id"].notna()]
    results_lte.to_csv("6.csv")
    return results_lte

class Telegram_Scraper_lte:
    paths={
        "dataset_path":"db",
        "error_path":"errors",
    }
    
    def __init__(self,start_from:Union[str,int],**args):
        self.inizialization(**args)
        with TelegramClient("tmp", API().app_id, API().api_hash) as client:
            channel_entities=[client.get_entity(start_from)]
        channel_name=channel_entities[0].title
        distance_from_start=0
        main=True
        while main:
            print([i.title for i in channel_entities])
            forwarded_channels=[]
            for public_channel in tqdm(channel_entities):
                print(f"Working on {public_channel.title}, at distace {distance_from_start} from {channel_name}")
                self.dump_channels(public_channel)
                print(f"Channel {public_channel.title} dumped")
                forwarded_channels+=self.get_forwarded_channels(public_channel)
            if forwarded_channels==[]:
                print(f"Job's done. Distance reached : {distance_from_start}")
                raise ValueError("Forwarded channel is empty. No channel to work on")
            #forwarded_channels=np.unique(forwarded_channels)
            distance_from_start+=1
            channel_entities=forwarded_channels
        
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
        if os.path.exists(f"{self.paths['dataset_path']}/downloaded_channels.csv"):
            self.downloaded_channels=pd.read_csv(f"{self.paths['dataset_path']}/downloaded_channels.csv",index_col=False)
        else:
            self.downloaded_channels=pd.DataFrame(columns=["name","id","last_download_date","max_offset"])
            self.downloaded_channels.to_csv(f"{self.paths['dataset_path']}/downloaded_channels.csv",index=False)
        print("directory loaded & cleaned")

    def dump_channels(
            self,
            channel_entity,
            limit:int=100,
            result_processing:Callable[[pd.DataFrame],pd.DataFrame]=processing_only_forwards,
        ):
        channel_username=channel_entity.title
        channel_id=channel_entity.id
        if (
            (channel_username in self.downloaded_channels["name"].to_list()) 
            or (channel_id in self.downloaded_channels["id"].to_list())
        ):
            offset=self.downloaded_channels[self.downloaded_channels["id"]==channel_id]["max_offset"].to_list()[0]+1
        else:
            pd.DataFrame([channel_username,channel_id,str(datetime.datetime.now()),0])
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
                #persisting downloaded_channels.csv
                if channel_id in self.downloaded_channels["id"].to_list():
                    channel_index=self.downloaded_channels[self.downloaded_channels["id"]==channel_id].index
                    self.downloaded_channels.loc[channel_index,:]=[[channel_username,channel_id,str(datetime.datetime.today()),offset]]
                else:
                    self.downloaded_channels=(
                        pd.concat([
                            self.downloaded_channels,
                            pd.DataFrame(
                                [[channel_username,channel_id,str(datetime.datetime.today()),offset]],
                                columns=self.downloaded_channels.columns
                                )
                        ],
                        ignore_index=True
                        )
                    )
                self.downloaded_channels.to_csv(
                    f"{self.paths['dataset_path']}/downloaded_channels.csv",
                    index=False,
                )
                # finding forwarded_messages
                if not results.empty:
                    processed_result=result_processing(results)
                    if not processed_result.empty:
                        print(results["date"].max(),len(processed_result))
                        if os.path.exists(f"{self.paths['dataset_path']}/fwd_{channel_id}.csv"):
                            processed_result=processed_result.to_csv(
                                f"{self.paths['dataset_path']}/fwd_{channel_id}.csv",
                                mode='a',
                                index=False,
                                header=False,
                            )
                        else:
                            processed_result.to_csv(
                                f"{self.paths['dataset_path']}/fwd_{channel_id}.csv",
                                index=False,
                            )
                    keep_on_going=results["id"].min()>=offset-limit
                    offset=results["id"].max()+1+limit
                else:
                    keep_on_going=False
                        

    def get_forwarded_channels(self,channel_entity) -> list[Any]:
        channel_id=channel_entity.id
        downloaded_channels=[int(i.removeprefix("fwd_").removesuffix(".csv")) for i in os.listdir("db") if i.startswith("fwd_")]
        channel_entities=[]
        if os.path.exists(f"{self.paths['dataset_path']}/fwd_{channel_id}.csv"):
            df=pd.read_csv(
                f"{self.paths['dataset_path']}/fwd_{channel_id}.csv",
            )
            df=df[df["from_channel_id"].notna()]
            with TelegramClient("tmp", API().app_id, API().api_hash) as client:
                for i in [int(i) for i in df["from_channel_id"].unique()]:
                    if not i in downloaded_channels:
                        try:
                            channel_entities+=[client.get_entity(int(i))]
                        except Exception as e:
                            print(e)
                            if str(e)!=(
                                "The channel specified is private and you lack permission to access it."
                                " Another reason may be that you were banned"
                                " from it (caused by GetChannelsRequest)"
                            ):
                                exit()
        return channel_entities

if __name__=="__main__":
    pm="PoliticalMemes"
    bt="bestimeline"
    Telegram_Scraper_lte(bt)