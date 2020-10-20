import pylinkedcmd
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import os

cmd_sb = pylinkedcmd.pylinkedcmd.Sciencebase()

#usgs_staff = cmd_sb.get_active_usgs_staff()
usgs_staff = cmd_sb.get_staff_by_email(["mtmartin@usgs.gov","meaton@usgs.gov"])

sbid_list_for_query = str([i["identifier_sbid"]
                           for i in usgs_staff]).replace('"', "'").replace("[", "(").replace("]", ")")

df_people = pd.DataFrame(usgs_staff)
df_people = df_people.drop_duplicates(subset='identifier_sbid', keep="first")
try:
    df_people.loc[df_people['identifier_orcid'].duplicated(), 'identifier_orcid'] = np.NaN
except:
    pass
df_people.loc[df_people['identifier_email'].duplicated(), 'identifier_email'] = np.NaN

pg_user = os.environ["PG_USER"]
pg_pass = os.environ["PG_PASS"]
pg_host = os.environ["PG_HOST"]
pg_port = os.environ["PG_PORT"]
pg_db = os.environ["PG_DB"]

pg_engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

with pg_engine.connect() as con:
    con.execute(f"DELETE FROM people where identifier_sbid IN {sbid_list_for_query}")
    con.close()

df_people.to_sql(
    "people",
    pg_engine,
    index=False,
    if_exists="append",
    chunksize=1000
)
