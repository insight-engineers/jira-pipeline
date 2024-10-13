import os
import pandas as pd
from datetime import datetime
import json
import time

time.sleep(3)

output_dir = os.getenv('OUTPUT_DIR', os.getcwd())
csv_filename = os.path.join(output_dir, f'test_{datetime.now().strftime("%Y-%m-%d")}.csv')
df = pd.read_csv(csv_filename)

df['description'] = df['description'].apply(lambda x: x.replace("'", '"') if isinstance(x, str) else x)

def parse_json(description):
    if isinstance(description, str):  
        try:
            return json.loads(description)  
        except json.JSONDecodeError:
            return None  
    return None 


df['description'] = df['description'].apply(parse_json)

# Transform cột description 
def extract_text_from_description(description):
    result = []
    if description and 'content' in description:
   
        for item in description['content']:
            # Kiểm tra nếu là một bullet list
            if item.get('type') == 'bulletList' and 'content' in item:
                for sub_item in item['content']:
                    
                    if sub_item.get('type') == 'listItem' and 'content' in sub_item:
                        for sub_sub_item in sub_item['content']:
                    
                            if sub_sub_item.get('type') == 'paragraph' and 'content' in sub_sub_item:
                                for text_item in sub_sub_item['content']:
                                    
                                    if text_item.get('type') == 'text' and 'text' in text_item:
                                        result.append(text_item['text'])
    return '\n'.join(result)

# Chuyển đổi kiểu dữ liệu
def convert_data_types(df):
    string_columns = ['parent', 'key', 'type', 'url', 'summary', 'status', 'assignee', 'reporter']
    df[string_columns] = df[string_columns].astype(str)

    date_columns = ['created', 'updated', 'due']
    for col in date_columns:
        df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
    
    for col in date_columns:
        if not pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = pd.to_datetime(df[col], errors='coerce')
        
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime('%Y-%m-%d') 
            
    return df



# Đổi tên cột một cách user-friendly
def rename_columns(df):
    df.rename(columns={
        'key': 'Task ID', 
        'parent': 'Parent Task ID', 
        'type': 'Type', 
        'summary': 'Task Name',
        'description': 'Description',
        'assignee': 'Assignee', 
        'reporter': 'Reporter',
        'status': 'Status', 
        'created': 'Created Date', 
        'updated': 'Updated Date',
        'due': 'Due Date',
        'url': 'URL',
    }, inplace=True)
    return df
    
def reorder_columns(df): 
    column_order = ['Task ID', 'Parent Task ID', 'Type', 'Task Name', 'Description', 'Assignee', 'Reporter', 
                    'Status', 'Created Date', 'Updated Date', 'Due Date', 'URL']

    df = df[column_order]
    return df

df = convert_data_types(df)
df['description'] = df['description'].apply(extract_text_from_description)
df = rename_columns(df)
df = reorder_columns(df)
df.to_parquet(os.path.join(output_dir, f'test_{datetime.now().strftime("%Y-%m-%d")}.parquet'), index=False)
