# %pip install --quiet "evadb[document]"
# %pip install --quiet marvin
#!apt install postgresql
#!service postgresql start
#!sudo -u postgres psql -c "CREATE USER eva WITH SUPERUSER PASSWORD 'password'"
#!sudo -u postgres psql -c "CREATE DATABASE evadb"
#pip install psycopg2-binary
import evadb
import warnings
import os
import marvin
from marvin import ai_classifier
from enum import Enum
import re
import pandas as pd
from evadb.configuration.constants import EvaDB_INSTALLATION_DIR

#connect to EvaDB
cursor = evadb.connect().cursor()
warnings.filterwarnings("ignore")
#set OpenAI API key
os.environ['OPENAI_API_KEY'] = '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'
open_ai_key = os.environ.get('OPENAI_API_KEY')
marvin.settings.openai.api_key = os.environ.get('OPENAI_API_KEY')

#Create Database (only once)
params = {
    "user": "eva",
    "password": "password",
    "host": "localhost",
    "port": "5432",
    "database": "evadb",
}
#query = f"CREATE DATABASE postgres_data WITH ENGINE = 'postgres', PARAMETERS = {params};"
#cursor.query(query).execute()

#Create tables and add some data for testing
cursor.query("""
USE postgres_data {
  DROP TABLE IF EXISTS employees
}
""").execute()
cursor.query("""
USE postgres_data {
  DROP TABLE IF EXISTS personal_data
}
""").execute()
cursor.query("""
USE postgres_data {
  CREATE TABLE employees (emplid CHAR(10) PRIMARY KEY, company VARCHAR(100), country CHAR(3), hire_date DATE NOT NULL, comprate FLOAT(9), department_id CHAR(10))
}
""").execute()
cursor.query("""
USE postgres_data {
  CREATE TABLE personal_data (emplid CHAR(10) PRIMARY KEY, dateofbirth DATE NOT NULL, placeofbirth VARCHAR(100), first_name VARCHAR(50), last_name VARCHAR(50), gender CHAR(1))
}
""").execute()
cursor.query("""
USE postgres_data {
 INSERT INTO employees (emplid, company, country, hire_date, comprate, department_id) VALUES ('001', 'Hogwarts INC', 'USA', '2022-01-01', 1000.00, 'HR'),
  ('002', 'Ministry of Magic', 'UK', '2022-02-15', 1500.00, 'IT'), ('003', 'Hogsmeade', 'CAN', '2023-03-10', 1200.00, 'Sales')
}""").execute()
cursor.query("""
USE postgres_data {
 INSERT INTO personal_data (emplid, dateofbirth, placeofbirth, first_name, last_name, gender) VALUES ('001', '2000-07-15', 'UK', 'Harry', 'Potter', 'M'),
  ('002', '1990-12-20', 'CAN', 'Ron', 'Weasley', 'M'), ('003', '1995-03-19','UK', 'Hermione', 'Granger', 'F')
}""").execute()


#Create a summary of all table schemas to be fed to the LLM as context
def InitializeSummary():
  cursor.query("""USE postgres_data {DROP TABLE IF EXISTS Summary}""").df()
  cursor.query("""USE postgres_data{CREATE TABLE Summary (table_name VARCHAR(60), table_schema VARCHAR(500))}""").execute()
  tables = cursor.query("""
  USE postgres_data {select table_name from information_schema.TABLES where table_schema='public' and table_name<>'summary'}""").df()
  for i in range(len(tables)):
    table_name=tables.iloc[i,0]
    actual_query = f"select column_name, data_type, character_maximum_length, column_default, is_nullable, is_identity from INFORMATION_SCHEMA.COLUMNS where table_name ='{table_name}' order by ordinal_position"
    query_text = "USE postgres_data{"+actual_query+"}"
    table_data = cursor.query(query_text).df()
    cursor.query(query_text).df()
    csv_list = table_data.to_csv(index=False, header=False, sep=',').strip()
    insert_text = f"INSERT into Summary (table_name, table_schema) values ('{table_name}','{table_name}: {csv_list}')"
    query_text = "USE postgres_data{"+insert_text+"}"
    cursor.query(query_text).df()

#Create a vector index of table schemas to be used for similarity search
def InitializeVectorIndex():
  InitializeSummary()
  cursor.query("""DROP TABLE IF EXISTS index_summary""").df()
  cursor.query("""CREATE TABLE index_summary (table_name TEXT UNIQUE, table_schema TEXT)""").execute()
  tables = cursor.query("""
USE postgres_data {select table_name from information_schema.TABLES where table_schema='public' and table_name<>'summary'}""").df()
  for i in range(len(tables)):
    table_name=tables.iloc[i,0]
    actual_query = f"select column_name from INFORMATION_SCHEMA.COLUMNS where table_name ='{table_name}' order by ordinal_position"
    query_text = "USE postgres_data{"+actual_query+"}"
    table_data = cursor.query(query_text).df()
    cursor.query(query_text).df()
    csv_list = table_data.to_csv(index=False, header=False, sep=',').strip()
    insert_text = f"INSERT into index_summary (table_name, table_schema) values ('{table_name}','{csv_list}')"
    cursor.query(insert_text).df()

  #create embeddings of table schemas using SentenceFeatureExtractor (can add parameter for model name)
  cursor.query("DROP FUNCTION IF EXISTS SentenceFeatureExtractor;").df()
  cursor.query(f"""CREATE FUNCTION SentenceFeatureExtractor IMPL '{EvaDB_INSTALLATION_DIR}/functions/sentence_feature_extractor.py'""").df()
  #create vector index for similarity search using FAISS
  cursor.query("""DROP INDEX IF EXISTS table_search_index""").df()
  cursor.query("CREATE INDEX table_search_index ON index_summary(SentenceFeatureExtractor(table_schema)) USING FAISS").df()

#Use EvaDB's Similiarity function to find closest matching table names
def ClosestMatch(question: str):
  response = cursor.query(f"""
    SELECT table_name, Similarity(
      SentenceFeatureExtractor('{question}'),
      SentenceFeatureExtractor(table_schema)
    )
    from index_summary
    order by Similarity(
      SentenceFeatureExtractor('{question}'),
      SentenceFeatureExtractor(table_schema)
    )
    limit 2
""").df()
  return response

#If user wants information from some tables
def SelectQuestion(question: str):
  print("===========================================")
  ans = str(input("🪄 Would you like the results from a specific table? (y/n): "))
  flag=False
  table_condition=" "
  if(ans.lower()=="y"):
    table_name = str(input("Table name: "))
    actual_query = f"select 'x' from INFORMATION_SCHEMA.TABLES where table_name = '{table_name}'"
    query_text = "USE postgres_data{"+actual_query+"}"
    if cursor.query(query_text).df().empty:
      print("That table doesn't exist in this database :( but I'll try to find something for you")
      flag=False
    else:
      flag=True
  print("===========================================")
  print("🪄 Finding the answer...")
  if(flag):
    prompt = f"You are given the schema of the table {table_name} in the context in the order (table_name: column_name,data_type,character_maximum_length,column_default,is_nullable, is_identity). Use that to generate an sql statement which answers the question"
    table_condition=f""" where table_name='{table_name}'"""
  else:
    #prompt = " You are an expert classifier. In the context, you are given the list of tables with their schemas in the form (table_name: column_name,data_type,character_maximum_length,column_default,is_nullable, is_identity). Use this list to find out which table or combination of tables will most likely contain the data requested by the user and provide the SQL statement to fetch the data."
   #run similarity search
    matches = ClosestMatch(question)
    prompt = f"You are an expert in SQL. The table schemas in the form (table_name: column_names) are as follows - "
    # Loop through the DataFrame and concatenate values with quotes
    for i in range(len(matches)):
      table_name=matches.iloc[i,0]
      get_tables = f"""select table_schema from index_summary where table_name = '{table_name}'"""
      schemas=cursor.query(get_tables).df()
      table_schema=schemas.iloc[0,0]
      prompt += f"({table_name}: {table_schema}), "
    prompt = prompt.replace("\n", " ")
    prompt = prompt.rstrip(", ")
    prompt += "Use this list to generate an SQL SELECT statement to fulfil the request. Use only the tables relevant to the request."
  prompt=prompt+f" Format the SQL statement as ```sql<statement here>```."
  chatgpt_udf = f"""select ChatGPT('{question}',table_schema,'{prompt}') from postgres_data.summary"""+table_condition
  response=cursor.query(chatgpt_udf).df().to_string(index=False, header=False)
  ExecuteResponse(response)

#If user wants to insert data into a table/if they want dummy data for testing
def InsertQuestion(question: str):
  print("===========================================")
  ans = str(input("🪄 Would you like to insert into a specific table? (y/n): "))
  flag=False
  table_condition=" "
  if(ans.lower()=="y"):
    table_name = str(input("Table name: "))
    actual_query = f"select 'x' from INFORMATION_SCHEMA.TABLES where table_name = '{table_name}'"
    query_text = "USE postgres_data{"+actual_query+"}"
    if cursor.query(query_text).df().empty:
      print("That table doesn't exist in this database :( but I'll try to find something for you")
      flag=False
    else:
      flag=True
  print("===========================================")
  print("🪄 Generating...")
  if(flag):
    prompt = f"You are an expert in SQL. The user will provide text that you need to parse into SQL INSERT statements for the table {table_name}. You are given the schema of the table in the context in the order (table_name: column_name,data_type,character_maximum_length,column_default,is_nullable, is_identity). Use the schema to extract, deduce, or infer any parameters and provide the SQL insert statements for the table accordingly."
    table_condition= f""" where table_name='{table_name}'"""
  else:
    prompt = "You are an SQL expert. he user will provide text that you need to parse into SQL INSERT statements. In the context, you are given tables with their schemas in the form (table_name: column_name,data_type,character_maximum_length,column_default,is_nullable, is_identity). Use the provided text and context to extract, deduce, or infer any parameters and provide only the relevant tables to insert the data."
  prompt=prompt+f" Format the SQL statement as ```sql<statement here>```."
  chatgpt_udf = f"""select ChatGPT('{question}',table_schema,'{prompt}') from postgres_data.summary"""+table_condition
  response=cursor.query(chatgpt_udf).df().to_string(index=False, header=False)
  print(response.replace("\\n"," "))

#If user wants to update some data
def UpdateQuestion(question: str):
  print("===========================================")
  ans = str(input("🪄 Would you like to update a specific table? (y/n): "))
  flag=False
  if(ans.lower()=="y"):
    table_name = str(input("Table name: "))
    actual_query = f"select 'x' from INFORMATION_SCHEMA.TABLES where table_name = '{table_name}'"
    query_text = "USE postgres_data{"+actual_query+"}"
    if cursor.query(query_text).df().empty:
      print("That table doesn't exist in this database :( but I'll try to find something for you")
      flag=False
    else:
      flag=True
  if(flag):
    prompt = f"You are an SQL expert. The user will provide some data to be changed in the table '{table_name}'. You are given the schema of the table in the context in the order (table_name: column_name,data_type,character_maximum_length,column_default,is_nullable, is_identity). Use the schema to extract, deduce, or infer any parameters and provide the SQL update statements for the table accordingly. If the user provides some condition, add it to the SQL statement."
    chatgpt_udf = f"""select ChatGPT('{question}',table_schema,'{prompt}') from postgres_data.summary where table_name='{table_name}'"""
  else:
    prompt = "You are an SQL expert. The user will provide text that you need to parse into SQL UPDATE statements. In the context, you are given tables with their schemas in the form (table_name: column_name,data_type,character_maximum_length,column_default,is_nullable, is_identity). Use the provided text and context to extract, deduce, or infer any parameters and provide only the relevant tables to be updated."
    chatgpt_udf = f"""select ChatGPT('{question}',table_schema,'{prompt}') from postgres_data.summary"""
  print(cursor.query(chatgpt_udf).df().to_string(index=False, header=False, max_rows=1))


#Loop to keep asking user questions
def OriginalQuestion(dummy: str):
  print("===========================================")
  print("🪄 Would you like to ask another question?")
  ans = str(input("y/n: "))
  if(ans.lower()=="y"):
   print("🪄 What would you like to know?")
   question = str(input("Question: "))
  else:
    return "exit"
  return question

#parse SQL query from LLM response and execute
def ExecuteResponse(response: str):
  response =response.replace("\\n"," ")
  print(response)
  print("===========================================")
  print("🪄 Would you like me to execute the query?")
  ans = str(input("y/n: "))
  if(ans.lower()=="y"):
    response = response.replace(';','')
    actual_query=re.search(r'```sql(.*?)```',response).group()
    query_text = 'USE postgres_data{'+actual_query[7:len(actual_query)-4]+'}'
    print(cursor.query(query_text).df())

#Marvin classifier which routes instructions
@ai_classifier
class InstructionRouter(Enum):
  """Represents distinct functions called based on the instruction"""
  INSERT = dict(tool=InsertQuestion, description="When the user wants to add new data to a table or when user wants you to generate data for a table")
  SELECT = dict(tool=SelectQuestion, description="When the user wants some information about the stored data")
  UPDATE = dict(tool=UpdateQuestion, description="When the user wants to update existing data")
  QUESTION = dict(tool=OriginalQuestion, description="When the user wants wants more information or wants to ask a different question")
 

#Initialize summary and vector index
flag = True
InitializeSummary()
InitializeVectorIndex()
#cursor.query("""USE postgres_data{select * from Summary}""").execute()

#user interface
print("===========================================")
print("🪄 What would you like to know?")
question = str(input())
while(flag):
  result= InstructionRouter(question)
  operation = result.value['tool'](question)
  question = OriginalQuestion(question)
  if question=="exit":
    flag = False
print("===========================================")
print("✅Hope I was of some help!")