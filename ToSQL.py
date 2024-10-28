# import pandas as pd
import snowflake.connector as sf
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
import os

load_dotenv()

snowflake_user = os.getenv("snowflake_user")
snowflake_password = os.getenv("snowflake_password")
snowflake_account = os.getenv("snowflake_account")
snowflake_warehouse = os.getenv("snowflake_warehouse")
snowflake_database = os.getenv("snowflake_database")
snowflake_schema = os.getenv("snowflake_schema")
snowflake_role = os.getenv("snowflake_role")
conn = sf.connect(
    user=snowflake_user,
    password=snowflake_password,
    account=snowflake_account,
    database=snowflake_database,
    schema=snowflake_schema,
    warehouse=snowflake_warehouse,
    role=snowflake_role
)

datesys = datetime.now().strftime("%Y-%m-%d")
today_date = datetime.today()
two_years_ago = today_date - relativedelta(years=2)
two_years_ago_date_str = two_years_ago.strftime("%Y-%m-%d")

connStr = os.getenv("connStrW")

mycursor = conn.cursor()


def print_connection_info(snow_conn):
    print("Snowflake connection info:")
    print(f"Account: {snow_conn.account}")
    print(f"User: {snow_conn.user}")
    print(f"Database: {snow_conn.database}")
    print(f"Schema: {snow_conn.schema}")
    print(f"Role: {snow_conn.role}")
    print(f"Warehouse: {snow_conn.warehouse}")
    print(f"Application: {snow_conn.application}")
    print(f"session_id: {snow_conn.session_id}")


def checkColumnExist(TableName):
    mycursor.execute(f"""
                        SELECT Column_name
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_NAME = '{TableName}'
                     """)
    column = [i[0] for i in mycursor.fetchall()]
    return column


def CreateColumn(TableName, ColumnName):
    add_colomn_sql = f"ALTER TABLE {TableName} ADD COLUMN IF NOT EXISTS {ColumnName} Varchar(4000)"
    mycursor.execute(add_colomn_sql)


def create_vessel_table(database_name="DEV_WSM_DB", schema_name="API"):
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {database_name}.{schema_name}.BLUET_API_SHIPS (
        ID VARCHAR(255),
        IMONUMBER VARCHAR(255),
        NAME VARCHAR(255),
        ALTERNATIVENAME VARCHAR(255),
        CURRENTVERSIONSTAMP VARCHAR(255),
        SHIPCLASSID VARCHAR(255),
        SHIPCLASSNAME VARCHAR(255),
        PORTOFREGISTRYUNLOC VARCHAR(255),
        ISHIDDEN VARCHAR(255),
        CALLSIGN VARCHAR(255),
        UPDATEDDATE VARCHAR(255),
        SHORTNAME VARCHAR(4000)
    );
    """
    mycursor.execute(create_sql)


def create_event_table(database_name="DEV_WSM_DB", schema_name="API"):
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {database_name}.{schema_name}.BLUET_API_EVENTS (
        ID VARCHAR(255),
        CUSTOMID VARCHAR(255),
        TIMESTAMP VARCHAR(255),
        EVENTTYPE VARCHAR(255),
        IMONUMBER VARCHAR(255),
        PORTUNLOC VARCHAR(255),
        PORTNAME VARCHAR(255),
        VOYAGENAME VARCHAR(255),
        UPDATEDDATE VARCHAR(255)
    );
    """
    mycursor.execute(create_sql)


def create_voyage_table(database_name="DEV_WSM_DB", schema_name="API"):
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {database_name}.{schema_name}.BLUET_API_VOYAGES (
        id VARCHAR(255),
        Name VARCHAR(255),
        imoNumber VARCHAR(255),
        UpdatedDate VARCHAR(255)
    );
    """
    mycursor.execute(create_sql)


def create_voyage_summary_table(database_name="DEV_WSM_DB", schema_name="API"):
    create_sql = f"""
    create or replace TABLE {database_name}.{schema_name}.BLUET_API_VOYAGESUMMARY (
        ID VARCHAR(255),
        VERSIONSTAMP VARCHAR(255),
        SHIPID VARCHAR(255),
        SHIPIMONUMBER VARCHAR(255),
        SHIPNAME VARCHAR(255),
        SHIPALTERNATIVENAME VARCHAR(255),
        SHIPCURRENTVERSIONSTAMP VARCHAR(255),
        SHIPSHIPCLASSID VARCHAR(255),
        SHIPPORTOFREGISTRYUNLOC VARCHAR(255),
        SHIPISHIDDEN VARCHAR(255),
        NAME VARCHAR(255),
        DURATION VARCHAR(255),
        SAILINGTIME VARCHAR(255),
        DISTANCESAILEDOVERGROUND VARCHAR(255),
        DISTANCESAILEDTHROUGHWATER VARCHAR(255),
        TOTALFOC VARCHAR(255),
        TOTALFOCME VARCHAR(255),
        TOTALFOCAE VARCHAR(255),
        TOTALFOCAB VARCHAR(255),
        TOTALCO2 VARCHAR(255),
        TOTALCO2ME VARCHAR(255),
        TOTALCO2AE VARCHAR(255),
        TOTALCO2AB VARCHAR(255),
        TOTALCO2IGS VARCHAR(255),
        TOTALCO2INC VARCHAR(255),
        TOTALFOCHFO VARCHAR(255),
        TOTALFOCHFOHS VARCHAR(255),
        TOTALFOCHFOLS VARCHAR(255),
        TOTALFOCHFOLLS VARCHAR(255),
        TOTALFOCLFO VARCHAR(255),
        TOTALFOCMDO VARCHAR(255),
        TOTALFOCMGO VARCHAR(255),
        TOTALFOCPROPANE VARCHAR(255),
        TOTALFOCBUTANE VARCHAR(255),
        TOTALFOCLNG VARCHAR(255),
        TOTALFOCMETHANOL VARCHAR(255),
        TOTALFOCETHANOL VARCHAR(255),
        EEOITEU VARCHAR(255),
        EEOIWEIGHT VARCHAR(255),
        TOTALCYLINDEROILCONSUMPTION VARCHAR(255),
        TOTALCIRCULATIONLUBOILMECONSUMPTION VARCHAR(255),
        TOTALCIRCULATIONLUBOILAECONSUMPTION VARCHAR(255),
        SOXEMISSIONSME VARCHAR(255),
        SOXEMISSIONSAE VARCHAR(255),
        SOXEMISSIONSAB VARCHAR(255),
        TOTALSOXEMISSIONS VARCHAR(255),
        NOXEMISSIONSME VARCHAR(255),
        NOXEMISSIONSAE VARCHAR(255),
        NOXEMISSIONSAB VARCHAR(255),
        TOTALNOXEMISSIONS VARCHAR(255),
        ENGINEDISTANCE VARCHAR(255),
        ESTIMATIONSCORE VARCHAR(255),
        COMPLETENESSSCORE VARCHAR(255),
        PLAUSIBILITYSCORE VARCHAR(255),
        TOTALFOCISO VARCHAR(255),
        TOTALFOCMEISO VARCHAR(255),
        TOTALFOCAEISO VARCHAR(255),
        TOTALFOCABISO VARCHAR(255),
        SHIPCALLSIGN VARCHAR(255),
        TOTALFOCIGS VARCHAR(255),
        TOTALFOCIGSISO VARCHAR(255),
        TOTALFRESHWATERCONSUMPTIONWASHING VARCHAR(255),
        TOTALFRESHWATERCONSUMPTIONDOMESTIC VARCHAR(255),
        TOTALFRESHWATERCONSUMPTIONUNDEF VARCHAR(255),
        BUNKERFRESHWATERBUNKERED VARCHAR(255),
        BUNKERFRESHWATERPRODUCED VARCHAR(255),
        TOTALFRESHWATERCONSUMPTIONBOILER VARCHAR(255),
        TOTALFOCINC VARCHAR(255),
        TOTALFOCINCISO VARCHAR(255),
        UPDATEDDATE VARCHAR(255),
        SHIPSHORTNAME VARCHAR(4000)
    );
    """
    mycursor.execute(create_sql)


def create_legsummary_table(database_name="DEV_WSM_DB", schema_name="API"):
    create_sql = f"""
    create or replace TABLE {database_name}.{schema_name}.BLUET_API_LEGSUMMARY (
        ID VARCHAR(255),
        REFERENCENUMBER VARCHAR(255),
        LEGEVENTID VARCHAR(255),
        VERSIONSTAMP VARCHAR(255),
        SHIPID VARCHAR(255),
        SHIPIMONUMBER VARCHAR(255),
        SHIPNAME VARCHAR(255),
        SHIPALTERNATIVENAME VARCHAR(255),
        SHIPCALLSIGN VARCHAR(255),
        SHIPCURRENTVERSIONSTAMP VARCHAR(255),
        SHIPSHIPCLASSID VARCHAR(255),
        SHIPPORTOFREGISTRYUNLOC VARCHAR(255),
        SHIPISHIDDEN VARCHAR(255),
        PORTOFORIGINID VARCHAR(255),
        PORTOFORIGINUNLOC VARCHAR(255),
        PORTOFORIGINNAME VARCHAR(255),
        PORTOFDESTINATIONID VARCHAR(255),
        PORTOFDESTINATIONUNLOC VARCHAR(255),
        PORTOFDESTINATIONNAME VARCHAR(255),
        DEPARTURETIME VARCHAR(255),
        ARRIVALTIME VARCHAR(255),
        CAPTAIN VARCHAR(255),
        CHIEFENG VARCHAR(255),
        DURATION VARCHAR(255),
        SAILINGTIME VARCHAR(255),
        DISTANCESAILEDOVERGROUND VARCHAR(255),
        DISTANCESAILEDTHROUGHWATER VARCHAR(255),
        CARGOWEIGHT VARCHAR(255),
        CARGOREEFERS VARCHAR(255),
        TOTALFOC VARCHAR(255),
        TOTALFOCME VARCHAR(255),
        TOTALFOCAE VARCHAR(255),
        TOTALFOCAB VARCHAR(255),
        TOTALCO2 VARCHAR(255),
        TOTALCO2ME VARCHAR(255),
        TOTALCO2AE VARCHAR(255),
        TOTALCO2AB VARCHAR(255),
        TOTALCO2IGS VARCHAR(255),
        TOTALCO2INC VARCHAR(255),
        TOTALFOCHFO VARCHAR(255),
        TOTALFOCHFOHS VARCHAR(255),
        TOTALFOCHFOLS VARCHAR(255),
        TOTALFOCHFOLLS VARCHAR(255),
        TOTALFOCLFO VARCHAR(255),
        TOTALFOCMDO VARCHAR(255),
        TOTALFOCMGO VARCHAR(255),
        TOTALFOCPROPANE VARCHAR(255),
        TOTALFOCBUTANE VARCHAR(255),
        TOTALFOCLNG VARCHAR(255),
        TOTALFOCMETHANOL VARCHAR(255),
        TOTALFOCETHANOL VARCHAR(255),
        EEOIWEIGHT VARCHAR(255),
        TOTALCYLINDEROILCONSUMPTION VARCHAR(255),
        TOTALCIRCULATIONLUBOILAECONSUMPTION VARCHAR(255),
        NOXEMISSIONSME VARCHAR(255),
        NOXEMISSIONSAE VARCHAR(255),
        NOXEMISSIONSAB VARCHAR(255),
        TOTALNOXEMISSIONS VARCHAR(255),
        ESTIMATIONSCORE VARCHAR(255),
        COMPLETENESSSCORE VARCHAR(255),
        PLAUSIBILITYSCORE VARCHAR(255),
        BALLASTWEIGHT VARCHAR(255),
        CARGOTEUFULL VARCHAR(255),
        CARGOTEUEMPTY VARCHAR(255),
        EEOITEU VARCHAR(255),
        SOXEMISSIONSME VARCHAR(255),
        SOXEMISSIONSAE VARCHAR(255),
        SOXEMISSIONSAB VARCHAR(255),
        TOTALSOXEMISSIONS VARCHAR(255),
        ENGINEDISTANCE VARCHAR(255),
        TOTALCIRCULATIONLUBOILMECONSUMPTION VARCHAR(255),
        TOTALFOCISO VARCHAR(255),
        TOTALFOCMEISO VARCHAR(255),
        UPDATEDDATE VARCHAR(255),
        SHIPSHORTNAME VARCHAR(4000),
        TOTALFOCAEISO VARCHAR(4000),
        TOTALFOCABISO VARCHAR(4000),
        TOTALFRESHWATERCONSUMPTIONDOMESTIC VARCHAR(4000),
        TOTALFRESHWATERCONSUMPTIONBOILER VARCHAR(4000),
        TOTALFRESHWATERCONSUMPTIONWASHING VARCHAR(4000),
        TOTALFRESHWATERCONSUMPTIONUNDEF VARCHAR(4000),
        BUNKERFRESHWATERPRODUCED VARCHAR(4000),
        VOYAGEID VARCHAR(4000),
        VOYAGENAME VARCHAR(4000)
    """
    mycursor.execute(create_sql)


def check_table_exists(table_name, database_name=None, schema_name=None):
    if database_name and not schema_name:
        raise ValueError(
            "Schema has to be provided when a database is provided"
        )
    location = ""
    if database_name:
        location = f"{database_name}."
    schema_filter = "CURRENT_SCHEMA()"
    if schema_name:
        schema_filter = f"'{schema_name}'"
    mycursor.execute(f"""
        SELECT 1
        FROM {location}INFORMATION_SCHEMA.TABLES
        WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = {schema_filter}
    """)
    result = mycursor.fetchone()
    return True if result else False


def get_all_columns(table_name, database_name=None, schema_name=None):
    if database_name and not schema_name:
        raise ValueError(
            "Schema has to be provided when a database is provided"
        )
    location = ""
    if database_name:
        location = f"{database_name}."
    schema_filter = "CURRENT_SCHEMA()"
    if schema_name:
        schema_filter = f"'{schema_name}'"
    mycursor.execute(f"""
        SELECT Column_name
        FROM {location}INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = {schema_filter}
    """)
    column = [i[0] for i in mycursor.fetchall()]
    return column


def create_table_with_column(table_name, initial_column_name="ID", database_name=None, schema_name=None):
    if database_name and not schema_name:
        raise ValueError(
            "Schema has to be provided when a database is provided"
        )
    table_identifier = table_name
    if schema_name:
        if not database_name:
            table_identifier = f"{schema_name}.{table_name}"
        else:
            table_identifier = f"{database_name}.{schema_name}.{table_name}"
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_identifier} (
        {initial_column_name} VARCHAR(255)
    );
    """
    mycursor.execute(create_sql)

def drop_table(table_name, database_name=None, schema_name=None):
    if database_name and not schema_name:
        raise ValueError(
            "Schema has to be provided when a database is provided"
        )
    table_identifier = table_name
    if schema_name:
        if not database_name:
            table_identifier = f"{schema_name}.{table_name}"
        else:
            table_identifier = f"{database_name}.{schema_name}.{table_name}"
    drop_sql = f"""
    DROP TABLE IF EXISTS {table_identifier};
    """
    mycursor.execute(drop_sql)
    return mycursor.fetchall()[0][0]


def upsert_data(data_df, table_name, on=None, delete_insert=True, database_name=None, schema_name=None,
                temp_table_name=None):
    """
    delete_insert:
        True: Delete matched rows of the source table, then insert(append) the whole source table to the target table.
                Used when then keys are not unique.
        False: Update the matched rows, insert otherwise. Used when the keys are unique values.
    """
    if not on:
        raise ValueError(
            "Parameter 'on' must be a string or a list of strings."
        )
    if database_name and not schema_name:
        raise ValueError(
            "Schema has to be provided when a database is provided"
        )
    key_cols = on
    if isinstance(on, str):
        key_cols = [on]

    temp_table = temp_table_name if temp_table_name else f"TEMP_TABLE_{datetime.now().strftime("%Y%m%d_%H%M%S")}"
    table_identifier = table_name
    temp_table_identifier = temp_table
    if schema_name:
        if not database_name:
            table_identifier = f"{schema_name}.{table_name}"
            temp_table_identifier = f"{schema_name}.{temp_table}"
        else:
            table_identifier = f"{database_name}.{schema_name}.{table_name}"
            temp_table_identifier = f"{database_name}.{schema_name}.{temp_table}"
    cur = conn.cursor()
    current_columns = get_all_columns(table_name, database_name=database_name)
    # print(f"{current_columns=}")
    for col in data_df.columns:
        if col not in current_columns:
            add_column_sql = f"ALTER TABLE {table_identifier} ADD COLUMN IF NOT EXISTS {col} Varchar(4000)"
            cur.execute(add_column_sql)

    # Upload data into temporary table

    drop_temp_table_sql = f"""
    DROP TABLE IF EXISTS {temp_table_identifier};
    """
    cur.execute(drop_temp_table_sql)
    success, nchunks, nrows, _ = write_pandas(conn, data_df, temp_table, database=database_name, schema=schema_name,
                                              table_type="temporary", auto_create_table=True)
    if success:
        print(f"Temporary table {temp_table_identifier} created. Merging it into {table_identifier}...")

    # Merge into
    match_keys_sql = " AND ".join([f"t.{col} = s.{col}" for col in key_cols])
    update_clause_set = ", ".join([f"t.{col} = s.{col}" for col in data_df.columns if col not in key_cols])
    insert_clause_col_list = ", ".join(data_df.columns)
    insert_clause_val_list = ", ".join([f"s.{col}" for col in data_df.columns])

    # rows_affected = 0
    if delete_insert:
        match_keys_list = ", ".join(key_cols)
        """
        BEGIN;
        -- Step 1: Delete existing rows in target_table that have keys present in source_table
        DELETE FROM target_table
        WHERE (key1, key2) IN (SELECT DISTINCT key1, key2 FROM source_table);

        -- Step 2: Insert new rows from source_table into target_table
        INSERT INTO target_table
        SELECT * FROM source_table;
        COMMIT;
        """
        delete_from_sql = f"""
        DELETE FROM {table_identifier}
        WHERE ({match_keys_list}) IN (SELECT DISTINCT {match_keys_list} FROM {temp_table_identifier});
        """
        insert_into_sql = f"""
        INSERT INTO {table_identifier} ({insert_clause_col_list})
        SELECT * FROM {temp_table_identifier};
        """
        try:
            cur.execute("BEGIN;")
            cur.execute(delete_from_sql)
            deleted_rows = cur.rowcount
            print(f"Number of rows deleted: {deleted_rows}")
            cur.execute(insert_into_sql)
            inserted_rows = cur.rowcount
            print(f"Number of rows inserted: {inserted_rows}")
            cur.execute("COMMIT;")
            rows_affected = deleted_rows + inserted_rows
        except Exception as e:
            print(f"An error occurred: {e}")
            # Attempt to rollback
            try:
                cur.execute("ROLLBACK;")
                print("Transaction rolled back due to an exception.")
            except Exception as rollback_error:
                print(f"Failed to rollback transaction: {rollback_error}")
            finally:
                raise
    else:

        """
        MERGE INTO target_table AS t
        USING source_table AS s
          ON t.key1 = s.key1 AND t.key2 = s.key2
        WHEN MATCHED THEN 
          UPDATE SET 
            t.value1 = s.value1,
            t.value2 = s.value2
        WHEN NOT MATCHED THEN 
          INSERT (key1, key2, value1, value2) 
          VALUES (s.key1, s.key2, s.value1, s.value2);
        """

        run_sql = f"""
        MERGE INTO {table_identifier} AS t
        USING {temp_table_identifier} AS s
          ON {match_keys_sql}
        WHEN MATCHED THEN 
          UPDATE SET {update_clause_set}
        WHEN NOT MATCHED THEN 
          INSERT ({insert_clause_col_list}) 
          VALUES ({insert_clause_val_list});
        """
        cur.execute(run_sql)
        rows_affected = cur.rowcount
        print(f"Number of rows affected: {rows_affected}")

    cur.execute(drop_temp_table_sql)
    delete_table_result = cur.fetchall()
    if delete_table_result:
        print(delete_table_result[0][0])
    return rows_affected


def insert_data(data_df, table_name, database_name=None, schema_name=None):
    if database_name and not schema_name:
        raise ValueError(
            "Schema has to be provided when a database is provided"
        )
    table_identifier = table_name
    if schema_name:
        if not database_name:
            table_identifier = f"{schema_name}.{table_name}"
        else:
            table_identifier = f"{database_name}.{schema_name}.{table_name}"
    # cur = conn.cursor()
    current_columns = get_all_columns(table_name, database_name=database_name)
    # print(f"{current_columns=}")
    for col in data_df.columns:
        if col not in current_columns:
            add_column_sql = f"ALTER TABLE {table_identifier} ADD COLUMN IF NOT EXISTS {col} Varchar(4000)"
            mycursor.execute(add_column_sql)
    success, nchunks, nrows, _ = write_pandas(conn, data_df, table_name, database=database_name, schema=schema_name)

    # print(f"{success=}, {nchunks=}, {nrows=}")


def insertVessels(Vessels):
    VesselList = checkColumnExist("BlueT_API_Ships")
    for ship in Vessels:
        head = [i for i in ship]
        head.append("UpdatedDate")
        line = ["'" + str(ship[i]) + "'" for i in ship]
        date = "'" + datesys + "'"
        print(line)
        line.append(date)
        for i in head:
            if i not in VesselList:
                CreateColumn("BlueT_API_Ships", i)
        Head = ",".join(head)
        Line = ",".join(line)
        # print(Head)
        # print(Line)
        mycursor.execute(f"""
                      INSERT INTO BlueT_API_Ships ({Head})
                      VALUES ({Line})
                     """)
    # mycursor.close()
    # conn.commit()
    # conn.close()


def insertReport(header, lines, vessels):
    ReportList = checkColumnExist("BlueT_API_Report")
    header = header + ",UpdatedDate"
    lines = lines + ",'" + datesys + "'"
    Header = header.split(",")
    ReportList = [i.lower() for i in ReportList]
    for i in Header:
        if i.lower() not in ReportList:
            CreateColumn("BlueT_API_Report", i)
    mycursor.execute(f"""
                      INSERT INTO BlueT_API_Report (IMONum,{header})
                      VALUES ('{vessels}',{lines})
                     """)

    # conn.commit()


def insertEngineReport(header, lines):
    ReportList = checkColumnExist("BlueT_API_ReportEngine")
    header = header + ",UpdatedDate"
    Header = header.split(",")
    lines = lines + ",'" + datesys + "'"
    ReportList = [i.lower() for i in ReportList]
    for i in Header:
        if i.lower() not in ReportList:
            CreateColumn("BlueT_API_ReportEngine", i)
    # print(header)
    # print(lines)
    mycursor.execute(f"""
            INSERT INTO BlueT_API_ReportEngine ({header})
            values ({lines})
            """
                     )
    # conn.commit()


def insertEvent(Header, Line):
    Header = Header + ",UpdatedDate"
    Line = Line + ",'" + datesys + "'"
    mycursor.execute(
        f"""
            INSERT INTO BlueT_API_Events ({Header}) VALUES ({Line})
            """)
    # mycursor.close()
    # conn.commit()
    # conn.close()


def insertVoyage(Voyages, IMO):
    Count = 0
    date = datesys
    for Voyage in Voyages:
        mycursor.execute("""
                    INSERT INTO BlueT_API_Voyages (id, name, imoNumber, UpdatedDate)
                    VALUES (?, ?, ?, ?)
                """, (Voyage["id"], Voyage["name"], IMO, date))
        Count += 1

    # mycursor.close()
    # conn.commit()
    return Count


def insertVoyageSummary(header, line):
    available = checkColumnExist("BlueT_API_VoyageSummary")
    header = header + ",UpdatedDate"
    Header = header.split(",")
    line = line + ",'" + datesys + "'"
    for i in Header:
        if i not in available:
            CreateColumn("BlueT_API_VoyageSummary", i)
    mycursor.execute(f"""
                      INSERT INTO BlueT_API_VoyageSummary ({header})
                      Values({line})
                     """
                     )

    # mycursor.close()
    # conn.commit()


def get_Vessel():
    date = datesys
    mycursor.execute(f"""
                      select distinct imoNumber from BlueT_API_Ships
                      where UpdatedDate='{date}'
                     """
                     )
    tem = [i[0] for i in mycursor.fetchall()]
    return tem


def get_VoyageID():
    date = datesys
    mycursor.execute(f"""
                          select distinct id from BlueT_API_Voyages
                          where UpdatedDate='{date}'
                         """
                     )
    tem = [i[0] for i in mycursor.fetchall()]
    return tem


def get_EventID():
    date = datesys
    mycursor.execute(f"""
                          select id from API.BlueT_API_Events 
                            where eventType='Departure'
                            and UpdatedDate='{date}'
                         """
                     )
    tem = [i[0] for i in mycursor.fetchall()]
    return tem


def insertLegSummary(header, line):
    LegList = checkColumnExist("BlueT_API_LegSummary")
    LegList = [i.lower() for i in LegList]
    header = header + ",UpdatedDate"
    Header = header.split(",")
    line = line + ",'" + datesys + "'"
    for i in Header:
        if i.lower() not in LegList:
            # print(LegList)
            # print(i.lower())
            CreateColumn("BlueT_API_LegSummary", i)
    mycursor.execute(f"""
                      INSERT INTO BlueT_API_LegSummary ({header})
                      Values({line})
                     """
                     )

    # mycursor.close()
    # conn.commit()
    # conn.close()


def TruncateFile(TableName):
    mycursor.execute(f"""
                          delete from {TableName}
                         """
                     )


def GetDataList(TableName):
    mycursor.execute(f"""
                      select * from {TableName}
                     """
                     )
    data = mycursor.fetchall()
    head = [i[0] for i in mycursor.description]
    return [head, data]


def closeConnection():
    mycursor.close()
    conn.close()


def AddColumn(TableName, Column):
    mycursor.execute(f"""
                      ALTER TABLE {TableName}
                        ADD {Column} varchar(255)
                     """
                     )
    # conn.commit()
