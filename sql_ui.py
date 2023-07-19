import pandas as pd
import streamlit as st

def get_data(file_name:str) -> pd.DataFrame:
    df = pd.read_excel(file_name)
    df.columns = df.iloc[1]
    df = df[2:]
    return df


def convert_dtype(df:pd.DataFrame, db_type:str) -> pd.DataFrame:
    db2_dtype_mapping = {'char':'Varchar2',
                         'varchar':'Varchar2',
                         'time':'Varchar2(15)',
                         'smallint':'Number(5)',
                         'integer':'Number(10)',
                         'bigint':'Number(19)',
                         'timestmp':'Timestamp(6)',
                         'date':'Date',
                         'boolean':'Number(1)',
                         'decimal':'Number',
                         'numeric':'Number',
                         'double':'Binary_Double',
                         'float':'Binary_Double',
                         'real':'Binary_Double',
                         'int':'Number(10)',
                         'nvarchar':'Varchar2'
                        }

    mssql_dtype_mapping = {'char':'Varchar2',
                           'varchar':'Varchar2',
                           'smallint':'Number(5)',
                           'integer':'Number(10)',
                           'bigint':'Number(20)',
                           'datetime':'Date',
                           'decimal':'Number',
                           'numeric':'Number',
                           'float':'Float(53)',
                           'real':'Float(24)',
                           'bit':'Number(3)',
                           'money':'Number(19,4)',
                           'tinyint':'Number(3)',
                           'text':'Long',
                           'timestamp':'Raw',
                           'uniqueidentifier':'Varchar2(36)',
                           'int':'Number(10)',
                           'nvarchar':'Varchar2'
                          }
    
    if db_type == 'DB2':
        df['YeniVeriTipi'] = df['VeriTipi'].map(db2_dtype_mapping).fillna(df['VeriTipi'])

    if db_type == 'Mssql':
        df['YeniVeriTipi'] = df['VeriTipi'].map(mssql_dtype_mapping).fillna(df['VeriTipi'])

    return df


def generate_sql_script(df:pd.DataFrame, schema:str) -> str:
    length_contained_types = ['Varchar2(15)', 'Number(5)', 'Number(10)', 'Number(19)', 'Timestamp(6)',
                              'Number(1)', 'Number(20)', 'Float(53)', 'Float(24)', 'Number(3)',
                              'Number(19,4)', 'Varchar2(36)']

    sql_script = f"CREATE TABLE {schema}.{df.iloc[0]['TabloAd']} (\n"

    for index, row in df.iterrows():
        col_name = row['KolonAd']
        data_type = row['YeniVeriTipi']

        if data_type in length_contained_types:
            data_length = data_type
        else:
            data_length = f'{data_type}({row["VeriUzunluk"]})'
        
        sql_script += f"\t{col_name} {data_length},\n"

    # Add DWH Date Time
    sql_script += "\tVA_AKTAR_TAR DATE DEFAULT trunc(sysdate),\n"
    sql_script += "\tVA_AKTAR_ZMN VARCHAR2(15 BYTE) DEFAULT to_CHAR(sysdate, 'HH24:MI:SS')\n"
    sql_script += f"\t) TABLESPACE TBS_{schema.upper()};"

    return sql_script


def run(filename:str, db_type:str, schema:str) -> str:
    df = get_data(file_name=filename)
    df = convert_dtype(df, db_type=db_type)
    script = generate_sql_script(df, schema=schema)
    return script


st.set_page_config(
  page_title = 'SQL Formatter',
  page_icon = '💿',
  )


def main():
    st.title("SQL Code Generator")

    col1, col2 = st.columns(2)
    with col1:
        schema  = st.text_input('Schema Name', 'wods5')
        st.write("### Upload the Excel File")

    with col2:
        db_type = st.selectbox("Select Source Database Type?", ("Mssql", "DB2"),)

    uploaded_file = st.file_uploader("File Type", type=["xlsx", "xls"], label_visibility='hidden')

    if uploaded_file is not None:
        script = run(filename=uploaded_file, db_type=db_type, schema=schema)
        st.code(script, language='sql') 

# Run the app
if __name__ == '__main__':
    main()
