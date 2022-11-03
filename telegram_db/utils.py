import pandas as pd
from key.key import API
import os
from telethon import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import PeerChannel
from typing import Union,Callable
import asyncio

async def get_all_messages_from_channel(id:int=1486151887):
    api=API()
    loop = asyncio.new_event_loop()
    client=TelegramClient("tmp",api.app_id,api.api_hash,loop=loop) 
    await client.connect()
    offset_id = 0
    limit = 100
    all_messages = []
    total_messages = 0
    total_count_limit = 0
    print(PeerChannel(id))
    #while True:
    print("Current Offset ID is:", offset_id, "; Total Messages:", total_messages)
    channel=PeerChannel(id)
    #history = client(
    req=GetHistoryRequest(
        peer=channel,
        offset_id=offset_id,
        offset_date=None,
        add_offset=0,
        limit=limit,
        max_id=0,
        min_id=0,
        hash=0
    )
        #print(history)

class telegram:
    def __init__(self,db_path:str,client:Union[TelegramClient,None]=None) -> None:
        if not os.path.exists(db_path):
            os.mkdir(db_path)
        self.db_path=db_path
        if client==None:
            self.client=self.start_client()
        else:
            self.client=client
        #print(client.is_user_authorized())
        #await client.connect()
        self.get_all_messages_from_channel(self.client)
        

    def start_client(self) -> TelegramClient:
        api=API()
        loop = asyncio.new_event_loop()
        client=TelegramClient("tmp",api.app_id,api.api_hash,loop=loop) 
        return client

    async def get_all_messages_from_channel(self,client:TelegramClient,id:int=1486151887):
        client.connect()
        offset_id = 0
        limit = 100
        all_messages = []
        total_messages = 0
        total_count_limit = 0
        print(PeerChannel(id))
        #while True:
        print("Current Offset ID is:", offset_id, "; Total Messages:", total_messages)
        channel=PeerChannel(id)
        #history = client(
        req=GetHistoryRequest(
            peer=channel,
            offset_id=offset_id,
            offset_date=None,
            add_offset=0,
            limit=limit,
            max_id=0,
            min_id=0,
            hash=0
        )
        await client(req)
        #print(history)
        """
        if not history.messages:
            break
        messages = history.messages
        for message in messages:
            all_messages.append(message.to_dict())
        offset_id = messages[len(messages) - 1].id
        total_messages = len(all_messages)
        if total_count_limit != 0 and total_messages >= total_count_limit:
            break
        """


class simplified_db:
    def __init__(self,channel_dump:str):
        df=self.read_json(channel_dump)
        print(self.simplified_data(df))

    def read_json(self,dump_path:str,save_df:bool=True):
        """
        serializes the json dump from the telegram desktop app.
        It returns and saves a correctly formatted dataframe.
        """
        df=pd.read_json(dump_path)
        df=df.rename({"type":"channel_type","id":"channel_id"},axis=1)
        messages=pd.DataFrame.from_records(df["messages"].to_list())
        df=df.drop("messages",axis=1)
        df=pd.concat([df,messages],axis=1)
        if save_df:
            df.to_csv(path.split("/")[1].removesuffix(".json")+".csv")
        return df

    def simplified_data(self,df:pd.DataFrame):
        network=pd.DataFrame()
        network["to"]=df["forwarded_from"].dropna()
        network["from"]="best timeline"
        network["data"]=df[df["forwarded_from"].notna()]["date"]
        return network
    

if __name__=="__main__":
    path="dataset/best_timeline.json"
    get_all_messages_from_channel()
    #simplified_db(path)
