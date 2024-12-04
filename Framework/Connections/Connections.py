

import pymssql
import sqlalchemy

from Framework.Logger import Journal
from Framework.Connections.Servers import SQL


class Connections():
    def __init__(self):
        self
        
    def ConnectCursors(server,database="master"):
        log = Journal()
        sql = SQL()
        log.InsertToLog("cursor","setting up conenction to database")
        connected:bool = False
        try:
            global ServerConnection
            # sourceConn  = pymssql.connect(server)
            ServerConnection  = pymssql.connect(server=sql.ServerList(server),database=database) 
            connected = True
        except pymssql.InterfaceError as exc:
            log.InsertToLog("cursor","\x1b[0;31;48m"
                "ERROR: Could not connect to the source database. "
                "Make sure the server is running and check your settings."
                "\x1b[0m")
            log.InsertToLog("cursor", msg=exc)
            exit(1)
        log.InsertToLog("cursor", msg=f"Is connected? : {connected} ")
        log.InsertToLog("cursor", msg=f"Connection to {server} database established")   

        global ServerCursor
        ServerConnection.autocommit(True)
        ServerCursor = ServerConnection.cursor()

        log.InsertToLog("cursor", msg="Cursor established.")
        return connected
 
    def connectAlchemy (source: str,database: str = 'master'):
        try:
            log = Journal()            
            sql = SQL()
            engine = sqlalchemy.create_engine(rf"mssql+pymssql://{sql.ServerList(source)}/{database}")
            econn =engine.connect()
        except Exception as ex :
            log.InsertToLog("connectAlchemy",ex)
            exit(1)
        return econn
     
    def QueryReturns(database:str,schema ="dbo",table="-",queryType="ignore"):
        log = Journal()
        log.InsertToLog("QueryReturns",f"The opntion: {queryType} has been selected")
        if queryType=="ignore":
            return ""
        if queryType == 'isIdentity':            
            return  f''' SELECT 
                    QUOTENAME(c.name) 
                        FROM {database}.sys.objects AS o
                    JOIN {database}.sys.columns AS c
                        ON c.object_id = o.object_id
                            AND c.is_identity = 1
                    JOIN {database}.sys.types AS t
                        ON t.system_type_id = c.system_type_id
                    WHERE o.type = 'U' --; --User tables
                        and o.name = '{table}' 
                    and o.schema_id = SCHEMA_ID('{schema}')  '''
        if queryType=="GetColumns":
            return f''' SELECT QUOTENAME(syc.COLUMN_NAME)
                        FROM  {database}.INFORMATION_SCHEMA.COLUMNS  syc
                        WHERE syc.TABLE_NAME = '{table}'
                        AND syc.TABLE_SCHEMA = '{schema}'
    					AND syc.COLUMN_NAME NOT IN 
                            (SELECT name FROM {database}.sys.columns WHERE  is_computed = 1 and object_id = object_id('{table}'))
                        AND syc.COLUMN_NAME NOT IN 
                            ('RequiereAccionDePersonal','TimeStamp','longitude')'''
        if queryType == "TableHEAD":
            return f""" DECLARE @Sql NVARCHAR(MAX); SET @Sql = (SELECT 'CREATE TABLE {database}.{schema}.' + QUOTENAME('{table}') + '(' + CHAR(10) + STRING_AGG(COLUMN_DEFINITION, + CHAR(10)) + CHAR(10) + ');' FROM ( SELECT CONCAT(QUOTENAME(COLUMN_NAME) ,+ ' ' + UPPER(DATA_TYPE), + CASE WHEN DATA_TYPE IN ('DATE','DATETIME','INT') THEN  '' ELSE '(' + CASE WHEN CHARACTER_MAXIMUM_LENGTH = '-1' THEN 'MAX' ELSE CAST(CHARACTER_MAXIMUM_LENGTH as varchar) END + ')' END , CASE WHEN COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 1 THEN ' IDENTITY(' + CAST(IDENT_SEED(TABLE_SCHEMA + '.' + TABLE_NAME) AS VARCHAR) + ',' + CAST(IDENT_INCR(TABLE_SCHEMA + '.' + TABLE_NAME) AS VARCHAR) + ')' ELSE '' END ) + ' ' + CASE WHEN IS_NULLABLE = 'NO' THEN 'NOT NULL' ELSE 'NULL' END + ',' COLUMN_DEFINITION FROM {database}.INFORMATION_SCHEMA.COLUMNS WHERE  TABLE_NAME = '{table}' and TABLE_SCHEMA = '{schema}') AS Columns FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'); SELECT @Sql Result; """
        if queryType == 'tablesInformation':
            return f''' select concat(table_schema,'.',TABLE_NAME) tables 
            FROM {database}.INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE'
    	    and TABLE_NAME NOT LIKE '%[0-9]%'
    	    and TABLE_NAME NOT LIKE '%DELETE%'
    	    and TABLE_NAME NOT LIKE '%Log'
    	    and TABLE_NAME NOT LIKE '%Historico'
    	    and TABLE_NAME NOT LIKE '%TEMP'	
    	    and TABLE_NAME NOT LIKE '%copy'	
    	    and TABLE_NAME NOT LIKE '%backup' '''
        if queryType=='GetcolumsFromSchemaTable':
            return f''' SELECT QUOTENAME(syc.COLUMN_NAME)
                        FROM  {database}.INFORMATION_SCHEMA.COLUMNS  syc
                        WHERE syc.TABLE_NAME = '{table}'
                        AND syc.TABLE_SCHEMA = '{schema}' '''
        if queryType=='GetProgramability':
            return  f'''select STRING_AGG(cast(sc.text as varchar(max)),' ')
                        FROM   {database}.dbo.sysobjects  so , 
                        {database}.dbo.syscomments sc ,
                        {database}.INFORMATION_SCHEMA.ROUTINES r
                        WHERE  so.id = sc.id 
                            and so.name = r.SPECIFIC_NAME
                            and SPECIFIC_SCHEMA  = '{schema}'
                        group BY so.type, so.name, sc.number'''
        if queryType == 'AddingNewFields':
            return f''' 
                    SELECT 
                         CONCAT('ALTER TABLE ', QUOTENAME(syc.TABLE_SCHEMA), '.', QUOTENAME(syc.TABLE_NAME), ' ',
                            'ADD ', QUOTENAME(syc.COLUMN_NAME), ' ', syc.DATA_TYPE,
                            CASE 
                                WHEN syc.DATA_TYPE LIKE '%char%' THEN CONCAT('(', syc.CHARACTER_MAXIMUM_LENGTH, ')')
                                ELSE ''
                            END, 
                            ';') AS alter_statement
                    FROM dbOrion.INFORMATION_SCHEMA.COLUMNS syc
                    WHERE syc.TABLE_NAME = '{table}'
                    AND syc.TABLE_SCHEMA = '{schema}';'''
        if queryType == "AddingProgramability":
            return f''' 
                SELECT STRING_AGG(cast(replace(replace(replace(replace(sc.text,'  ',' '),'  ',' '),'CREATE FUNCTION','CREATE OR ALTER FUNCTION'),'CREATE PROCEDURE','CREATE OR ALTER PROCEDURE') as varchar(max)),' ')    alter_statement
                FROM    {database}.dbo.sysobjects  so , 
                        {database}.dbo.syscomments sc ,
                	    {database}.INFORMATION_SCHEMA.ROUTINES r
                WHERE  so.id = sc.id 
                AND so.name = r.SPECIFIC_NAME
                AND SPECIFIC_SCHEMA  = '{schema}'
                group BY so.type, so.name, sc.number '''
        if queryType=="GetAllSchemas":
            return f'''SELECT name FROM {database}.sys.schemas where schema_id>4 and name not like 'db_%' '''
    
    def getCustomIdentity(type:str) -> list:
        #This list represents the tables with custom keys that need to be considered when migrating the data to avoid unexpected errors.
        Files: dict ={
        "Table1": ['field1','field2' ],
        "table2":['candidateKey01','candidateKey02']
        }    
        item_list:str = [value for key, value in Files.items() if type in key]

        return item_list