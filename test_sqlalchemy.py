import os

import pandas as pd
from collections import defaultdict

from sqlalchemy import *
from sqlalchemy.sql import select
from sqlalchemy.schema import *
from sqlalchemy.orm import sessionmaker

from invoke import task

@task
def sqlserver():
    db = create_engine("mssql+pymssql://test:test@127.0.0.1/pengtao_test",
              deprecate_large_types=True)
    sql = "select id,name from sysobjects where xtype='U'and name<>'dtproperties' order by name"

    df = pd.read_sql_query(sql, db)
    print df

@task
def oracle():
    """
    """
    os.environ['NLS_LANG'] = '.AL32UTF8'

    db_engine=create_engine('oracle://pengtao:B5u4*Wi2@172.18.1.17:1521/orcl', echo=False)
    sql = "select * from cdr_sc_2.patient where rownum < 10"
    df_patient = pd.read_sql_query(sql, db_engine)
    print df_patient.gender_name


