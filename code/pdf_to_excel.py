from bs4 import BeautifulSoup
import pandas as pd
import json 
import numpy as np
import subprocess
from os import path
import warnings
warnings.filterwarnings("ignore")


def read_and_extract_data(html_file_path):
    
    table = BeautifulSoup(open(path.relpath(html_file_path.replace('~/','')),'r').read())

    extract_lines = [line  for block in table.find_all('block')[4:-3] for line in block.findAll('line')]
    ymin_values =sorted( set([float(line['ymin']) for line in extract_lines]))
    sorted_lines =[[ word  for line in extract_lines  for word in line.find_all("word",{"ymin":"{:.6f}".format(ymin)}) ] for ymin in ymin_values ]
    sort_line_words = [ sorted(line, key= lambda x: float(json.loads(str(x).split("xmin=")[1].split(" ")[0])))
                            for line in sorted_lines ]
    return sort_line_words


def extract_column_names(column_data_list):
    
    new_column_names_list =[]
    for i in range(0,len(column_data_list)):
        lis =[]
        for j in range(i,len(column_data_list)):

            if float(column_data_list[j]['xmin'])  -float(column_data_list[i]['xmax'])   <15:

                lis.append(column_data_list[j])
        
        if str(lis[0]) not in str(new_column_names_list):
            new_column_names_list.append(lis)
    
    column_name_df = pd.DataFrame([ [' '.join([column_name_data.get_text() for column_name_data in column]), column[0]['xmin'],column[-1]['xmax']] for column in new_column_names_list],columns =['column_name','xmin','xmax'])
    return column_name_df





def extract_table_data(column_name_df,sort_line_words):
    
    column_list = column_name_df['column_name'].unique().tolist()
    df =pd.DataFrame([])
    for i in range(0,len(column_list)):


        column_name = column_list[i]
        first_column_xmin = float(column_name_df[column_name_df['column_name']==column_name]['xmin'])
        if column_name == column_list[-1]:
            second_column_xmin =float(column_name_df[column_name_df['column_name']==column_name]['xmax'])+15

        else: 

            second_column_xmin = float(column_name_df[column_name_df['column_name']==column_list[i+1]]['xmin'])

        index_with_data =[[sort_line_words[1:].index(line) ,' '.join([word.get_text() 
                                                                      for word in line 
                 if first_column_xmin <=float(word['xmax']) <= second_column_xmin])]
                                                                     for line in sort_line_words[1:] ]

        temp_df= pd.DataFrame(index_with_data,columns =['index',column_name]).T
        temp_df =temp_df.rename(columns=temp_df.iloc[0]).drop(temp_df.index[0])
        df= df.append(temp_df) 
        
    df=df.T
    df=df.apply(lambda x: x if len(''.join(x.values))>3 else pd.Series([np.nan]*len(x),index =x.index),axis=1)
    df= df.replace('', np.nan).dropna(how='all')
    return df



def structure_data(table_data_df)  :
    
    df =table_data_df.iloc[:-1,:]
    carried_data =table_data_df.iloc[-1:,:]
    df['Booking Date'] =df['Booking Date']+'_'+df.index.astype(str)
    df['Booking Date']=df['Booking Date'].fillna(method='ffill')
    df= df.replace(np.nan,'')

    aggregation_functions = {key: lambda x: " ".join(x.values) for key in df.columns[1:]}
    df=df.groupby('Booking Date').aggregate(aggregation_functions).reset_index()
    df['Booking Date'] = df['Booking Date'].apply(lambda x:x.split('_')[0])

    df[['Booking Date', 'Txn Date','Value Date']] =  df[['Booking Date', 'Txn Date','Value Date']].apply( pd.to_datetime).astype(str)
    df= df.append(carried_data)
    return df



def convert_pdf_to_html(file):
    
    cmd =r"""pdftotext -f 1 -l 1 -r 300  -bbox-layout {0} {1}""" .format( file, file.replace('.pdf','.html'))
    p=subprocess.call(cmd,shell=True, stderr=subprocess.STDOUT)

    print(p)
    return file.replace('.pdf','.html')


def pdf_to_excel(file):
    
    
    html_file_path =convert_pdf_to_html(file)
    sort_line_words =read_and_extract_data(html_file_path)
    column_name_df =extract_column_names(sort_line_words[0])
    table_data_df =extract_table_data(column_name_df,sort_line_words)
    final_df =structure_data(table_data_df)
    final_df.to_excel(html_file_path.replace('.html','.xlsx').replace('input','output'))
    print(html_file_path.replace('.html','.xlsx').replace('input','output'))
    
