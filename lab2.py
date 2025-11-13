import requests
import pandas as pd
import chdb

# Lab 2.1
file_path="https://raw.githubusercontent.com/statsbomb/open-data/master/data/matches/223/282.json"
response=requests.get(file_path)

# Lab 2.3
file_path="s3://datasets-documentation/amazon_reviews/*.parquet"



df_matches=pd.json_normalize(response.json(), sep="_")
print(df_matches.iloc[0])

def run_pd():
    response=requests.get(file_path)
    df_matches=pd.json_normalize(response.json(), sep="_")
    return df_matches


def run_ch_describe(df_matches):
    query="""
    DESCRIBE Python(df_matches)
    SETTINGS describe_compact_output=1
    """
    query=chdb.query(query, "DataFrame")
    print(query)

def run_ch(df_matches):
    query="""
    WITH 
        home AS
        (
            SELECT home_team_home_team_name as team, count() as count
            FROM Python(df_matches)
            GROUP BY home_team_home_team_name
        ),
        away AS
        (
            SELECT away_team_away_team_name as team, count() as count
            FROM Python(df_matches)
            GROUP BY away_team_away_team_name
        ),
        combine AS
        (
            SELECT *
            FROM home
            UNION ALL
            SELECT *
            FROM away
        )
    SELECT team, sum(count) as count
    FROM combine
    GROUP BY team
    ORDER BY count DESC
    """
    query=chdb.query(query, "DataFrame")
    print(query)

if __name__=="__main__":
    df=run_pd()
    