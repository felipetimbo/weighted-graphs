import os
import pandas as pd
import numpy as np

def preprocess_copenhagen():
    calls_df = pd.read_csv(os.path.abspath(os.path.join(os.path.dirname( __file__ ), 'unweighted-graphs','copenhagen-interaction','calls.csv')), header=0, names=['timestamp', 'orig','dest','duration'])
    sms_df = pd.read_csv(os.path.abspath(os.path.join(os.path.dirname( __file__ ), 'unweighted-graphs','copenhagen-interaction','sms.csv')), header=0, names=['timestamp', 'orig','dest'])

    df = pd.concat([calls_df[['orig', 'dest']], sms_df[['orig','dest']]]).reset_index(drop=True)
    
    # adding two new columns to get min and max origin and destination users
    df['user1'] = df[['orig','dest']].min(axis=1)
    df['user2'] = df[['orig','dest']].max(axis=1)

    # sorting and concatenating unique 'orig' and 'dest' ids  
    ids = np.sort(np.unique(np.hstack((df['orig'].to_numpy(),df['dest'].to_numpy()))))
    dict_ids = dict( zip( ids, list(range(len(ids)))))

    # sorting and reordering ids for node1 and node2   
    df = df.sort_values(by=['user1', 'user2'])
    df = df.replace({'user1': dict_ids}) 
    df = df.replace({'user2': dict_ids}) 

    # weight is the sum of calls and messages among two users 
    df = df.groupby(['user1', 'user2']).size().reset_index(name='weight')

    df.to_csv(os.path.abspath(os.path.join(os.path.dirname( __file__ ), 'weighted-graphs','copenhagen-interaction.csv')), index=False, header=False)

def preprocess_reality_call():
    df = pd.read_csv(os.path.abspath(os.path.join(os.path.dirname( __file__ ), 'unweighted-graphs','reality-call','ia-reality-call.csv')), header=0, names=['orig','dest','timestamp','duration'])

    df = df[df['orig'] != df['dest']].reset_index(drop=True)

    # adding two new columns to get min and max origin and destination users
    df['user1'] = df[['orig','dest']].min(axis=1)
    df['user2'] = df[['orig','dest']].max(axis=1)

    # sorting and concatenating unique 'orig' and 'dest' ids  
    ids = np.sort(np.unique(np.hstack((df['orig'].to_numpy(),df['dest'].to_numpy()))))
    dict_ids = dict( zip( ids, list(range(len(ids)))))

    # sorting and reordering ids for node1 and node2   
    df = df.sort_values(by=['user1', 'user2'])
    df = df.replace({'user1': dict_ids}) 
    df = df.replace({'user2': dict_ids}) 

    # weight is the sum of calls and messages among two users 
    df = df.groupby(['user1', 'user2']).size().reset_index(name='weight')

    df.to_csv(os.path.abspath(os.path.join(os.path.dirname( __file__ ), 'weighted-graphs','reality-call.csv')), index=False, header=False)

def preprocess_contacts_dublin():
    df = pd.read_csv(os.path.abspath(os.path.join(os.path.dirname( __file__ ), 'unweighted-graphs','contacts-dublin','ia-contacts-dublin.csv')), header=0, names=['orig','dest','timestamp'])

    # adding two new columns to get min and max origin and destination users
    df['user1'] = df[['orig','dest']].min(axis=1)
    df['user2'] = df[['orig','dest']].max(axis=1)

    # sorting and concatenating unique 'orig' and 'dest' ids  
    ids = np.sort(np.unique(np.hstack((df['orig'].to_numpy(),df['dest'].to_numpy()))))
    dict_ids = dict( zip( ids, list(range(len(ids)))))

    # sorting and reordering ids for node1 and node2   
    df = df.sort_values(by=['user1', 'user2'])
    df = df.replace({'user1': dict_ids}) 
    df = df.replace({'user2': dict_ids}) 

    # weight is the sum of calls and messages among two users 
    df = df.groupby(['user1', 'user2']).size().reset_index(name='weight')

    df.to_csv(os.path.abspath(os.path.join(os.path.dirname( __file__ ), 'weighted-graphs','contacts-dublin.csv')), index=False, header=False)

def preprocess_digg_reply():
    df = pd.read_csv(os.path.abspath(os.path.join(os.path.dirname( __file__ ), 'unweighted-graphs','digg-reply','ia-digg-reply.csv')), delimiter=' ', header=0, names=['orig','dest','weight','timestamp'])

    df = df[df['orig'] != df['dest']].reset_index(drop=True)

    # adding two new columns to get min and max origin and destination users
    df['user1'] = df[['orig','dest']].min(axis=1)
    df['user2'] = df[['orig','dest']].max(axis=1)

    # sorting and concatenating unique 'orig' and 'dest' ids  
    ids = np.sort(np.unique(np.hstack((df['orig'].to_numpy(),df['dest'].to_numpy()))))
    dict_ids = dict( zip( ids, list(range(len(ids)))))

    # sorting and reordering ids for node1 and node2   
    df = df.sort_values(by=['user1', 'user2'])
    df = df.replace({'user1': dict_ids}) 
    df = df.replace({'user2': dict_ids}) 

    # weight is the sum of calls and messages among two users 
    df = df.groupby(['user1', 'user2']).size().reset_index(name='weight')

    df.to_csv(os.path.abspath(os.path.join(os.path.dirname( __file__ ), 'weighted-graphs','digg-reply.csv')), index=False, header=False)


if __name__ == "__main__":
    # preprocess_copenhagen()
    # preprocess_reality_call()
    # preprocess_contacts_dublin()
    preprocess_digg_reply()
    