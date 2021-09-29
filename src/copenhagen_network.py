import os
import pandas as pd
import numpy as np

def main():
    calls_df = pd.read_csv(os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', 'unweighted-graphs','copenhagen-interaction','calls.csv')), header=0, names=['timestamp', 'orig','dest','duration'])
    sms_df = pd.read_csv(os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', 'unweighted-graphs','copenhagen-interaction','sms.csv')), header=0, names=['timestamp', 'orig','dest'])

    df = pd.concat([calls_df[['orig', 'dest']], sms_df[['orig','dest']]]).reset_index(drop=True)

    # sorting and concatenating unique 'orig' and 'dest' ids  
    ids = np.sort(np.unique(np.hstack((df['orig'].to_numpy(),df['dest'].to_numpy()))))

    dict_ids = dict( zip( ids, list(range(len(ids)))))

    df['node1'] = df[['orig','dest']].min(axis=1)
    df['node2'] = df[['orig','dest']].max(axis=1)

    df = df.sort_values(by=['node1', 'node2'])
    df = df.replace({'node1': dict_ids}) 
    df = df.replace({'node2': dict_ids}) 

    df = df.groupby(['node1', 'node2']).size().reset_index(name='weight')
    # sorted_sms_df = sms_df.min(axis=1)

if __name__ == "__main__":
    main()
    