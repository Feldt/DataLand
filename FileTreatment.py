import polars as pl

from datetime import datetime
from Framework import Validations
from Framework.Connections.Connections import Connections
from Framework.Connections.Logic import PolarSystem
from Framework.Logger import Journal


def MigrateData(database: str, sourceServer, targetServer):

    log = Journal()
    conns = Connections()
    ps = PolarSystem()
    valid = Validations.Validation()

    log.InsertToLog("MigrateData_PL&PD", f"start time {datetime.now()}")

    # getting all tables from the database selected
    ## we ignore the tables that has created as backup in order to clean the data
    query = conns.QueryRetuns(database, queryType="tablesInformation")

    # creating the connections for source and targer servers in order to reuse the variables depending on server selected.
    alchemy_source = conns.connectAlchemy(sourceServer, database)
    alchemy_target = conns.connectAlchemy(targetServer, database)

    # reaging from databse the tables on source server and save them as _df type list to loop it
    df_tables = pl.read_database(
        query, connection=alchemy_source, infer_schema_length=None
    )

    log.InsertToLog("MigrateData_PL&PD", df_tables)
    counterTables = 1

    # processing tables
    for t in df_tables.to_series().to_list():
        log.InsertToLog("MigrateData_PL&PD", f"table number {counterTables}")
        # separete the values that came from the tables list query
        schema = t.split(".")[0]
        table = t.split(".")[1]

        # exeption schemas and tables section
        # we want to avoid some tables that are created for another pruporses
        if schema == "pd" or schema == "tr" or schema == "pg":
            continue

        if (
            table == "tblFiles"
            or table == "TestTable"
            or table == "idvalor"
            or table == "mytog_kpi"
        ):
            continue

        if table == "togexes_MonthDays":
            continue

        if (
            (table == "SurveySAssignationDetails" and schema == "GEN")
            or (table == "GobalParameters" and schema == "CONF")
            or (table == "Security" and schema == "CONF")
        ):
            continue

        log.InsertToLog("MigrateData_PL&PD", f"processing {table}")

        # verify and toggle the Idendity Insert [TABLE] to ON
        log.InsertToLog(
            "MigrateData_PL&PD", f"toggleIdentityInsert: validating identity"
        )
        idendity, idendityKey = ps.toggleIdentityInsert(
            database, schema, table, "ON", alchemy_target
        )
        log.InsertToLog("MigrateData_PL&PD", f"{idendity} ; {idendityKey}")

        # getting columns from table selected and validating if we have columns as date or Id candidate keys in order to process the table
        log.InsertToLog("MigrateData_PL&PD", "ValidateDatesAndKeys")
        mainCols, keys, dates = valid.ValidateDatesAndKeys(
            ps.ReaderFromQueries(database, schema, table, "GetColumns", alchemy_source)
        )
        # build the data depening if it has a catalog or transactional data
        if dates is not None:
            sql = f"select {mainCols}  from {database}.{schema}.{table} where {dates} > getdate() -5 "  # the 5 days can be ajusted here
        else:
            sql = f"select {mainCols}  from {database}.{schema}.{table}"

        # filling up the source and target DataFrame from SQL Server
        log.InsertToLog("MigrateData_PL&PD", f"filling up source and targer with {sql}")
        df_source = pl.read_database(
            query=sql, connection=alchemy_source, infer_schema_length=None
        )

        df_target = pl.read_database(
            query=sql, connection=alchemy_target, infer_schema_length=None
        )

        log.InsertToLog("MigrateData_PL&PD", "building surrogeateKey")
        # get theese name as id
        customIdentity = conns.getCustomIdentity(table)
        # create surrogate according this id
        if len(customIdentity) > 0:
            log.InsertToLog("MigrateData_PL&PD", f"custom keys w/ {customIdentity}")
            # need to locate the fields for surrogate
            # create surrogate based on this fields
            if len(customIdentity[0]) > 2:
                df_source = df_source.with_columns(
                    pl.concat_str(
                        pl.col(customIdentity[0][0]),
                        pl.col(customIdentity[0][1]),
                        pl.col(customIdentity[0][2]),
                    ).alias("surrogate")
                )
                df_target = df_target.with_columns(
                    pl.concat_str(
                        pl.col(customIdentity[0][0]),
                        pl.col(customIdentity[0][1]),
                        pl.col(customIdentity[0][2]),
                    ).alias("surrogate")
                )
            else:
                df_source = df_source.with_columns(
                    pl.concat_str(
                        pl.col(customIdentity[0][0]), pl.col(customIdentity[0][1])
                    ).alias("surrogate")
                )
                df_target = df_target.with_columns(
                    pl.concat_str(
                        pl.col(customIdentity[0][0]), pl.col(customIdentity[0][1])
                    ).alias("surrogate")
                )
            if idendity:
                df_source = df_source.drop(idendityKey)
                df_target = df_target.drop(idendityKey)
                log.InsertToLog("MigrateData_PL&PD", "insert identity not allowed")
                idendity, idendityKey = ps.toggleIdentityInsert(
                    database, schema, table, "OFF", alchemy_target
                )
            else:
                idendityKey = customIdentity[0][0].replace("[", "").replace("]", "")

        elif len(idendityKey) > 0:
            log.InsertToLog("MigrateData_PL&PD", f"identity built-in w/ {idendityKey}")
            df_source = df_source.with_columns(
                pl.concat_str(pl.col(idendityKey)).alias("surrogate")
            )
            df_target = df_target.with_columns(
                pl.concat_str(pl.col(idendityKey)).alias("surrogate")
            )
        elif len(keys) > 0:
            log.InsertToLog("MigrateData_PL&PD", f"auto w/ {keys}")
            df_source = df_source.with_columns(
                pl.concat_str(pl.col(keys[0].replace("[", "").replace("]", ""))).alias(
                    "surrogate"
                )
            )

            df_target = df_target.with_columns(
                pl.concat_str(pl.col(keys[0].replace("[", "").replace("]", ""))).alias(
                    "surrogate"
                )
            )
            idendityKey = keys[0].replace("[", "").replace("]", "")

        log.InsertToLog("MigrateData_PL&PD", "filtering anti to be inserted")

        # selecting the columns to be migrated after comparation
        cols = df_target.columns
        colsList = [col for col in cols if "_f" not in col and "surrogate" != col]

        # converting the PL.DataFrame to PD.DataFrame in order to ETL
        dfsource = df_source.to_pandas()
        dfdest = df_target.to_pandas()

        # join wth 'anti' the df1 and df2 and separete them using the surrogate key
        dfAux = dfsource.merge(dfdest, on="surrogate", how="left", suffixes=("", "_f"))

        # getting ready the insert df in order to modify the database
        pf_insert = dfAux[dfAux[f"{idendityKey}_f"].isnull()]
        pf_insert = pf_insert[colsList].copy()

        # we only insert data if there is new data to be inserted
        if len(pf_insert.index) > 0:
            log.InsertToLog(
                "MigrateData_PL&PD", f"inserting on {table}:  {datetime.now()}"
            )
            try:
                pf_insert.to_sql(
                    table,
                    con=alchemy_target,
                    schema="pd",
                    chunksize=1000,
                    method="multi",
                    if_exists="replace",
                    index=False,
                )
                if len(customIdentity) > 0:
                    mainCols, _, _ = valid.ValidateDatesAndKeys(
                        ps.ReaderFromQueries(
                            database, "pd", table, "GetColumns", alchemy_target
                        )
                    )
                    pl.read_database(
                        query=ps.generateMergeTempsTables(
                            database, schema, table, customIdentity, mainCols
                        ),
                        connection=alchemy_target,
                        infer_schema_length=None,
                    )
                elif len(idendityKey) > 0:
                    idendity, valkey = ps.toggleIdentityInsert(
                        database, schema, table, "OFF", alchemy_target
                    )
                    if idendity:
                        idendityKey = valkey
                    pl.read_database(
                        query=ps.generateMergeTempsTables(
                            database,
                            schema,
                            table,
                            idendityKey,
                            mainCols,
                            isIdentity=idendity,
                        ),
                        connection=alchemy_target,
                        infer_schema_length=None,
                    )
                else:
                    pl.read_database(
                        query=ps.generateMergeTempsTables(
                            database, schema, table, keys[0], mainCols, False
                        ),
                        connection=alchemy_target,
                        infer_schema_length=None,
                    )
                    idendityKey = keys[0]
            except Exception as exc:
                log.InsertToLog("insert", msg=exc)

        # separete the rows that need no be updated ##TODO: is this been used?
        # dfAux= dfsource.merge(dfdest,on="surrogate",how="left",suffixes=('_f',''))
        # pf_update = dfAux[dfAux[f'{idendityKey}'].notnull()]
        # pf_update = pf_update[colsList].copy()

        # Separate the data from the DataFrame with a condition, selecting rows that have the same ID but different data
        pf_update = dfdest.merge(dfsource, how="outer", indicator=True).query(
            "_merge == 'right_only'"
        )[dfdest.columns]
        pf_update = pf_update[colsList].copy()

        # we only follow the process to update in database if exists any change
        if len(pf_update.index) > 0:
            log.InsertToLog(
                "MigrateData_PL&PD", f"inserting on PD.{table} | {datetime.now()}"
            )
            try:
                pf_update.to_sql(
                    table,
                    con=alchemy_target,
                    schema="pd",
                    chunksize=1000,
                    method="multi",
                    if_exists="replace",
                    index=False,
                )
            except Exception as exc:
                log.InsertToLog("update", msg=exc)

            # generate SQL merge statement in order to merche the data inside the sql motor from external autogenerated sql
            log.InsertToLog("MigrateData_PL&PD", "get merge info")
            if len(customIdentity) > 0:
                mainCols, _, _ = valid.ValidateDatesAndKeys(
                    ps.ReaderFromQueries(
                        database, "pd", table, "GetColumns", alchemy_target
                    )
                )
                pl.read_database(
                    query=ps.generateMergeTempsTables(
                        database, schema, table, customIdentity, mainCols
                    ),
                    connection=alchemy_target,
                    infer_schema_length=None,
                )
            elif len(idendityKey):

                idendity, valkey = ps.toggleIdentityInsert(
                    database, schema, table, "OFF", alchemy_target
                )
                if idendity:
                    idendityKey = valkey
                pl.read_database(
                    query=ps.generateMergeTempsTables(
                        database,
                        schema,
                        table,
                        idendityKey,
                        mainCols,
                        isIdentity=idendity,
                    ),
                    connection=alchemy_target,
                    infer_schema_length=None,
                )
            else:
                pl.read_database(
                    query=ps.generateMergeTempsTables(
                        database, schema, table, keys[0], mainCols
                    ),
                    connection=alchemy_target,
                    infer_schema_length=None,
                )

            # Clean the database by removing the transaction data that were created as part of this process.
            pl.read_database(
                query=f"truncate TABLE pd.{table};select 1 ",
                connection=alchemy_target,
                infer_schema_length=None,
            )
        log.InsertToLog(
            "MigrateData_PL&PD", f"inserte on PD schema completed ~ {datetime.now()}"
        )

        if idendity:
            log.InsertToLog("MigrateData_PL&PD", "returning identity insert to off")
            ps.toggleIdentityInsert(database, schema, table, "OFF", alchemy_target)

        counterTables = counterTables + 1
        log.InsertToLog(
            "MigrateData_PL&PD", f"table finished {table} t: {datetime.now()}\n\n"
        )

    log.InsertToLog(
        "MigrateInfrastructure", f"Finishing 'DATA' migration for {database}"
    )
    # break

def MigrateInfrastructure(database: str, sourceServer: str, targetServer: str):
    log = Journal()
    ps = PolarSystem()
    sql = Connections()

    ServerCursor = sql.ConnectCursors(targetServer, database)

    log.InsertToLog("MigrateInfrastructure", "connecting to source")
    s_alchemy = sql.connectAlchemy(sourceServer, database)
    log.InsertToLog("MigrateInfrastructure", "connecting to targer")
    t_alchemy = sql.connectAlchemy(targetServer, database)
    log.InsertToLog("migrateInfrastructure", "Generating cursor for possibles changes")
    sql.ConnectCursors(targetServer, database)
    isConnected = False

    log.InsertToLog("MigrateInfrastructure", "geting information from source")
    # compare if we need new tables
    df_source = ps.ReaderFromQueries(database, "-", "-", "tablesInformation", s_alchemy)
    log.InsertToLog("MigrateInfrastructure", "geting information from target")
    df_target = ps.ReaderFromQueries(database, "-", "-", "tablesInformation", t_alchemy)
    log.InsertToLog("MigrateInfrastructure", "converting results")
    pf_source = df_source.to_pandas()
    pf_target = df_target.to_pandas()
    log.InsertToLog(
        "MigrateInfrastructure", "getting diferences on tables and locating new tables"
    )
    pf_update = pf_target.merge(pf_source, how="outer", indicator=True).query(
        "_merge == 'right_only'"
    )[pf_target.columns]

    # avoid extra processing when no data for migration
    if len(pf_update.index) > 0:

        query = f"""use [{database}];
                IF NOT EXISTS (SELECT SCHEMA_ID FROM sys.schemas WHERE [name] = 'pd')
                    EXEC('CREATE SCHEMA [pd] AUTHORIZATION [dbo]'); """
        ServerCursor.execute(query)
        isConnected = True

        for t in pf_update["tables"].to_list():
            schema = t.split(".")[0]
            table = t.split(".")[1]

            log.InsertToLog(
                "MigrateInfrastructure", f"wotking on '[{schema}].[{table}]'"
            )
            log.InsertToLog(
                "MigrateInfrastructure",
                "generating the objects to be inserted in order to create the new tables",
            )
            log.InsertToLog(
                "MigrateInfrastructure", "getting the changes with auto built-in"
            )
            df_result = ps.ReaderFromQueries(
                database, schema, table, "TableHEAD", s_alchemy
            )

            # avoid extra processing with no data for migrations
            if len(df_result) > 0:
                log.InsertToLog(
                    "MigrateInfrastructure", f"creating new tables on {targetServer}"
                )
                query = (
                    df_result.to_series()
                    .to_list()[0]
                    .replace("\n", "")
                    .replace(",);", ");")
                    .replace(",", ", ")
                )
                log.InsertToLog(
                    "MigrateInfrastructure",
                    f"executing auto generated queries to create the {schema}.{table} on {targetServer}",
                )
                ServerCursor.execute(query)
                isConnected = True
                # pd.read_sql(sql=query,con=t_alchemy)

    log.InsertToLog("MigrateInfrastructure", "Getting TABLES INFORMAATION")
    df_GeneralDatabaseInformation = pl.read_database(
        query=sql.QueryReturns(database, queryType="tablesInformation"),
        connection=s_alchemy,
        infer_schema_length=None,
    )

    for t in df_GeneralDatabaseInformation.to_series().to_list():

        schema = t.split(".")[0]
        table = t.split(".")[1]

        # compare if we need new fields
        log.InsertToLog(
            "MigrateInfrastructure",
            "comparing if there is new fields added to the tables",
        )
        df_source = ps.ReaderFromQueries(
            database, schema, table, "GetcolumsFromSchemaTable", s_alchemy
        )
        df_target = ps.ReaderFromQueries(
            database, schema, table, "GetcolumsFromSchemaTable", t_alchemy
        )
        log.InsertToLog("MigrateInfrastructure", "converting results")
        pf_source = df_source.to_pandas()
        pf_target = df_target.to_pandas()

        log.InsertToLog(
            "MigrateInfrastructure",
            "getting diferences on structure and locating new fields",
        )
        pf_update = pf_target.merge(pf_source, how="outer", indicator=True).query(
            "_merge == 'right_only'"
        )[pf_target.columns]

        if len(pf_update.index) > 0:
            log.InsertToLog(
                "MigrateInfrastructure",
                f"Generating objects to modify tables on {targetServer} ",
            )
            log.InsertToLog(
                "MigrateInfrastructure", "getting the changes with auto built-in"
            )
            df_s_result = ps.ReaderFromQueries(
                database, schema, table, "AddingNewFields", s_alchemy
            )
            df_t_result = ps.ReaderFromQueries(
                database, schema, table, "AddingNewFields", t_alchemy
            )

            pf_s_result = df_s_result.to_pandas()
            pf_t_result = df_t_result.to_pandas()
            df_result = pf_t_result.merge(
                pf_s_result, how="outer", indicator=True
            ).query("_merge == 'right_only'")[pf_t_result.columns]

            if len(df_result) > 0:
                alter_statement = df_result["alter_statement"].to_list()
                for query in alter_statement:
                    log.InsertToLog(
                        "MigrateInfrastructure", "creating new modifcations"
                    )
                    log.InsertToLog(
                        "MigrateInfrastructure",
                        f"executing auto generated queries to add fields on {schema}.{table} on {targetServer}",
                    )
                    # pd.read_sql(sql=query,con=t_alchemy)
                    ServerCursor.execute(query)
                    isConnected = True

        log.InsertToLog(
            "MigrateInfrastructure", f" [{schema}].[{table}] has been processed\n"
        )

        # compare if we need new store procedures
        # compare if we need new funcionts
    log.InsertToLog("MigrateInfrastructure", "loading all the programability section")
    df_GeneralDatabaseInformation = pl.read_database(
        query=sql.QueryReturns(database, queryType="GetAllSchemas"),
        connection=s_alchemy,
        infer_schema_length=None,
    )
    for schema in df_GeneralDatabaseInformation.to_series().to_list():

        log.InsertToLog("MigrateInfrastructure", f"working on : '{schema}'")
        log.InsertToLog("MigrateInfrastructure", "loading programability on source")
        df_source = ps.ReaderFromQueries(
            database, schema, "-", "GetProgramability", s_alchemy
        )
        log.InsertToLog("MigrateInfrastructure", "loading programability on target")
        df_target = ps.ReaderFromQueries(
            database, schema, "-", "GetProgramability", t_alchemy
        )

        log.InsertToLog("MigrateInfrastructure", "converting results")
        pf_source = df_source.to_pandas()
        pf_target = df_target.to_pandas()

        log.InsertToLog(
            "MigrateInfrastructure", "getting diferences on the programability section"
        )
        pf_update = pf_target.merge(pf_source, how="outer", indicator=True).query(
            "_merge == 'right_only'"
        )[pf_target.columns]

        # avoid extra processing working on non diferences
        if len(pf_update.index) > 0:
            log.InsertToLog(
                "MigrateInfrastructure",
                "getting diferences on the programability section",
            )
            log.InsertToLog(
                "MigrateInfrastructure", "getting the changes with auto built-in"
            )

            df_s_result = ps.ReaderFromQueries(
                database, schema, table, "AddingProgramability", s_alchemy
            )
            df_t_result = ps.ReaderFromQueries(
                database, schema, table, "AddingProgramability", t_alchemy
            )

            pf_s_result = df_s_result.to_pandas()
            pf_t_result = df_t_result.to_pandas()
            df_result = pf_t_result.merge(
                pf_s_result, how="outer", indicator=True
            ).query("_merge == 'right_only'")[pf_t_result.columns]

            # avoid extra processing working on non diferences to be aplied
            if len(df_result) > 0:
                alter_statement = df_result["alter_statement"].to_list()
                for query in alter_statement:
                    log.InsertToLog(
                        "MigrateInfrastructure", "spliting results to modify objects "
                    )
                    log.InsertToLog(
                        "MigrateInfrastructure", "modifying programability objects "
                    )
                    # pd.read_sql(sql=query,con=t_alchemy)
                    ServerCursor.execute(query)
                    isConnected = True

        log.InsertToLog("MigrateInfrastructure", f"finishing this schema : '{schema}'")

    if isConnected:
        ServerCursor.close()
    # compare if we need new stored table
    log.InsertToLog(
        "MigrateInfrastructure", f"Finishing 'structure' migration for {database}"
    )
