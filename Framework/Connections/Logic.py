import polars as pl
import pandas as pd

from Framework import Validation
from Framework.Connections.Connections import Connections


class PolarSystem:
    def __init__(self):
        self

    def ReaderFromQueries(
        database: str, schema: str, table: str, value: str, alchemy
    ) -> pl.DataFrame:
        conns = Connections()
        return pl.read_database(
            conns.QueryReturns(database, schema, table, value),
            connection=alchemy,
            infer_schema_length=None,
        )

    def toggleIdentityInsert(
        database: str, schema: str, table: str, value: str, alchemy
    ):
        valid = Validation()
        ps = PolarSystem()
        df_identity = ps.ReaderFromQueries(
            database, schema, table, "isIdentity", alchemy
        )
        if len(df_identity) > 0:
            query = f""" SET IDENTITY_INSERT {database}.{schema}.{table} {value} ; select 1  """
            pl.read_database(query, connection=alchemy, infer_schema_length=None)
            return True, valid.ValidateIdentityColumn(df_identity).replace(
                "[", ""
            ).replace("]", "")

        return False, ""

    def generateMergeTempsTables(
        database: str, schema: str, table: str, keys, cols, isIdentity=True
    ) -> str:
        values: str = ""
        insertvalues: str = ""
        # key2 = f'{keys[1].replace("[","").replace("]","")}'
        if isinstance(keys, list):
            key1 = f'{keys[0][0].replace("[","").replace("]","")}'
            key2 = f'{keys[0][1].replace("[","").replace("]","")}'
            if len(keys[0]) > 2:
                key3 = f'{keys[0][2].replace("[","").replace("]","")}'
                for v in cols.split(","):
                    if v != f"[{key1}]" and v != f"[{key2}]" and v != f"[{key2}]":
                        values += f"{v} = source.{v}, "

                for v in cols.split(","):
                    # if v != f"[{key1}]" and v!=f"[{key2}]"and v!=f"[{key2}]":
                    insertvalues += f"SOURCE.{v}, "

                values = values[:-2]
                insertvalues = insertvalues[:-2]
                sql = [
                    f";WITH pandastemp ({cols} ) "
                    f"AS(select  {cols} from {database}.pd.{table}) "
                    f"MERGE {database}.{schema}.{table} AS TARGET "
                    f"USING pandastemp AS SOURCE ON (TARGET.{key1} = SOURCE.{key1} and target.{key2} = source.{key2} and target.{key3} = source.{key3}) "
                    "WHEN  MATCHED THEN update SET "
                    f"{values} "
                    "WHEN NOT MATCHED BY TARGET THEN "
                    f"INSERT ({insertvalues.replace("SOURCE.","")})"
                    f"VALUES ({insertvalues}) ;"
                    "select 1 "
                ]

            else:
                for v in cols.split(","):
                    if v != f"[{key1}]" and v != f"[{key2}]":
                        values += f"{v} = source.{v}, "
                for v in cols.split(","):
                    # if v != f"[{key1}]" and v!=f"[{key2}]"and v!=f"[{key2}]":
                    insertvalues += f"SOURCE.{v}, "

                values = values[:-2]
                insertvalues = insertvalues[:-2]
                sql = [
                    f";WITH pandastemp ({cols} ) "
                    f"AS(select  {cols} from {database}.pd.{table}) "
                    f"MERGE {database}.{schema}.{table} AS TARGET "
                    f"USING pandastemp AS SOURCE ON (TARGET.{key1} = SOURCE.{key1} and target.{key2} = source.{key2}) "
                    "WHEN  MATCHED THEN update SET "
                    f"{values} "
                    "WHEN NOT MATCHED BY TARGET THEN "
                    f"INSERT ({insertvalues.replace("SOURCE.","")})"
                    f"VALUES ({insertvalues}) ;"
                    "select 1 "
                ]
        else:
            for v in cols.split(","):
                if v != f"[{keys.replace("[","").replace("]","")}]":
                    values += f"{v} = source.{v}, "
            for v in cols.split(","):
                if isIdentity:
                    if v != f"[{keys}]":
                        insertvalues += f"SOURCE.{v}, "
                else:
                    insertvalues += f"SOURCE.{v}, "

            values = values[:-2]
            insertvalues = insertvalues[:-2]
            sql = [
                f";WITH pandastemp ({cols} ) "
                f"AS(select  {cols} from {database}.pd.{table}) "
                f"MERGE {database}.{schema}.{table} AS TARGET "
                f"USING pandastemp AS SOURCE ON (TARGET.{keys} = SOURCE.{keys} ) "
                "WHEN  MATCHED THEN update SET "
                f"{values} "
                "WHEN NOT MATCHED BY TARGET THEN "
                f"INSERT ({insertvalues.replace("SOURCE.","")})"
                f"VALUES ({insertvalues}) ;"
                "select 1 "
            ]

        return sql[0]
