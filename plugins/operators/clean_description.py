from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import logging
import json
import pandas as pd 
import numpy as np 


class creating_label_df(BaseOperator):
    '''
    Creating DF based on SAS-Labels
    '''
    @apply_defaults
    def __init__(self,file='',*args, **kwargs):
        '''
        Creates JSON-Dictionary for each Variable in Text-Description.
        Arguments: 
            file = name of SAS-Textfile
        '''
        super(creating_label_df,self).__init__(*args,**kwargs)
        self.file               = file 

    def execute(self,context):
        '''
        Execute function
        '''
        with open('data/'+self.file) as f:
            label = f.readlines()
        label_df=pd.DataFrame(label)

        drop_rows=['(.*)/']

        label_dfs=label_df[~label_df[0].str.contains('|'.join(drop_rows))].reset_index(drop=True)[5:]

        label_dfs['len']=label_dfs[0].apply(lambda x: len(x))
        label_dfs=label_dfs[label_dfs['len']>=10].reset_index(drop=True)

        label_dfs[0]=label_dfs[0].apply(lambda x: x.replace(';',''))
        label_dfs[0]=label_dfs[0].apply(lambda x: x.strip())
        label_dfs[0]=label_dfs[0].apply(lambda x: x.replace("'",''))
        label_dfs[0]=label_dfs[0].apply(lambda x: x.replace('\t',''))
        label_dfs=label_dfs[~label_dfs[0].str.contains('permament format')].reset_index(drop=True)
        
        label_dfs['key']=label_dfs[0].apply(lambda x: x.split('=')[0])
        label_dfs['values']=label_dfs[0].apply(lambda x: x.split('=')[-1])

        label_dfs=label_dfs.drop([0,'len'],axis=1)

        label_dfs=label_dfs.set_index('key')

        string = label_dfs.index.str.contains('value').cumsum()
        var_dict = {f'label_dfs{i}': g for i, g in label_dfs.groupby(string)}

        code_list=['i94cntyl','i94prtl','i94model','i94addrl','i94visa']

        for keys,json_name in zip(var_dict.keys(),code_list):
            temp=var_dict[keys].reset_index()[1:]
            temp=dict(zip(temp['key'],temp['values']))
            with open('/opt/airflow/data/{}.json'.format(json_name),'w') as json_dict:
                logging.info('{} JSON DF created'.format(json_name))
                json.dump(temp, json_dict)

