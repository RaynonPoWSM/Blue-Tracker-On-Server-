# import pandas as pd
import snowflake.connector as sf
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

snowflake_user = os.getenv("snowflake_user")
snowflake_password = os.getenv("snowflake_password")
snowflake_account = os.getenv("snowflake_account")
snowflake_warehouse = os.getenv("snowflake_warehouse")
snowflake_database = os.getenv("snowflake_database")
snowflake_schema = os.getenv("snowflake_schema")
conn = sf.connect(
    user=snowflake_user,
    password=snowflake_password,
    account=snowflake_account,
    database=snowflake_database,
    schema=snowflake_schema,
    warehouse=snowflake_warehouse
)

datesys = datetime.now().strftime("%Y-%m-%d")
connStr = os.getenv("connStrW")

mycursor = conn.cursor()


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

def create_report_table(database_name="DEV_WSM_DB", schema_name="API"):
    create_sql = f"""
    create or replace TABLE {database_name}.{schema_name}.BLUET_API_REPORT (
        ABHFOFOC VARCHAR(255),
        ABHFOHSFOC VARCHAR(255),
        ABHFOLLSFOC VARCHAR(255),
        ABHFOLSFOC VARCHAR(255),
        ABLFOFOC VARCHAR(255),
        ABMDOFOC VARCHAR(255),
        ABMDOHSFOC VARCHAR(255),
        ABMDOLLSFOC VARCHAR(255),
        ABMDOLSFOC VARCHAR(255),
        ABMGOFOC VARCHAR(255),
        ABMGOLLSFOC VARCHAR(255),
        ABMGOLSFOC VARCHAR(255),
        ABMETHANOLFOC VARCHAR(255),
        ABLNGFOC VARCHAR(255),
        ABBTLFOC VARCHAR(255),
        ABFAMEFOC VARCHAR(255),
        ABHVOFOC VARCHAR(255),
        ABBIOFUELBLENDFOC VARCHAR(255),
        AEHFOFOC VARCHAR(255),
        AEHFOHSFOC VARCHAR(255),
        AEHFOLLSFOC VARCHAR(255),
        AEHFOLSFOC VARCHAR(255),
        AELFOFOC VARCHAR(255),
        AEMDOFOC VARCHAR(255),
        AEMDOHSFOC VARCHAR(255),
        AEMDOLLSFOC VARCHAR(255),
        AEMDOLSFOC VARCHAR(255),
        AEMGOFOC VARCHAR(255),
        AEMGOLLSFOC VARCHAR(255),
        AEMGOLSFOC VARCHAR(255),
        AEMETHANOLFOC VARCHAR(255),
        AELNGFOC VARCHAR(255),
        AEBTLFOC VARCHAR(255),
        AEFAMEFOC VARCHAR(255),
        AEHVOFOC VARCHAR(255),
        AEBIOFUELBLENDFOC VARCHAR(255),
        AIRPRESS VARCHAR(255),
        AIRTEMP VARCHAR(255),
        AVERAGERELATIVEAEPOWER VARCHAR(255),
        AVERAGERELATIVEMEPOWER VARCHAR(255),
        AVERAGESHAFTRPM VARCHAR(255),
        AVERAGESLIP VARCHAR(255),
        AVERAGESLIPOVERGROUND VARCHAR(255),
        AVERAGESPEEDOVERGROUND VARCHAR(255),
        AVERAGESPEEDTHROUGHWATER VARCHAR(255),
        CAPTAIN VARCHAR(255),
        CARGOREEFERS VARCHAR(255),
        CHIEFENG VARCHAR(255),
        COMPLETENESSSCORE VARCHAR(255),
        COOLINGWATERTEMP VARCHAR(255),
        COURSEMADEGOOD VARCHAR(255),
        DAILYAEFOC VARCHAR(255),
        DAILYMEFOC VARCHAR(255),
        DAILYTOTALFOC VARCHAR(255),
        DRAFTAFT VARCHAR(255),
        DRAFTFWD VARCHAR(255),
        DRAFTMID VARCHAR(255),
        ENGINEDISTANCE VARCHAR(255),
        ENGINEROOMTEMP VARCHAR(255),
        ESTIMATIONSCORE VARCHAR(255),
        EVENTCUSTOMID VARCHAR(255),
        EVENTNAME VARCHAR(255),
        EVENTSHORTNAME VARCHAR(255),
        EVENTTYPE VARCHAR(255),
        GAINLOSSLUBOILCIRCULATIONAE VARCHAR(255),
        GAINLOSSLUBOILCIRCULATIONME VARCHAR(255),
        GAINLOSSLUBOILCYLINDER VARCHAR(255),
        GAINLOSSLUBOILCYLINDERHS VARCHAR(255),
        GAINLOSSLUBOILCYLINDERLS VARCHAR(255),
        HEADING VARCHAR(255),
        ID VARCHAR(255),
        ISSLOWSTEAMING VARCHAR(255),
        ISTCCUTOUT VARCHAR(255),
        MEANDRAFT VARCHAR(255),
        MEHFOFOC VARCHAR(255),
        MEHFOHSFOC VARCHAR(255),
        MEHFOLLSFOC VARCHAR(255),
        MEHFOLSFOC VARCHAR(255),
        MELFOFOC VARCHAR(255),
        MEMDOFOC VARCHAR(255),
        MEMDOHSFOC VARCHAR(255),
        MEMDOLLSFOC VARCHAR(255),
        MEMDOLSFOC VARCHAR(255),
        MEMGOFOC VARCHAR(255),
        MEMGOLLSFOC VARCHAR(255),
        MEMGOLSFOC VARCHAR(255),
        MEMETHANOLFOC VARCHAR(255),
        MELNGFOC VARCHAR(255),
        MEBTLFOC VARCHAR(255),
        MEFAMEFOC VARCHAR(255),
        MEHVOFOC VARCHAR(255),
        MEBIOFUELBLENDFOC VARCHAR(255),
        NOXEMISSIONSAE VARCHAR(255),
        NOXEMISSIONSME VARCHAR(255),
        PERIOD VARCHAR(255),
        PLAUSIBILITYCOUNTFINE VARCHAR(255),
        PLAUSIBILITYCOUNTMAJOR VARCHAR(255),
        PLAUSIBILITYCOUNTMINOR VARCHAR(255),
        PLAUSIBILITYCOUNTNOTSET VARCHAR(255),
        PLAUSIBILITYSCORE VARCHAR(255),
        POSLAT VARCHAR(255),
        POSLNG VARCHAR(255),
        REPORTID VARCHAR(255),
        ROBFUELHFO VARCHAR(255),
        ROBFUELHFOHS VARCHAR(255),
        ROBFUELMDO VARCHAR(255),
        ROBLUBOILCIRCULATIONAE VARCHAR(255),
        ROBLUBOILCIRCULATIONME VARCHAR(255),
        ROBLUBOILCYLINDER VARCHAR(255),
        ROBLUBOILCYLINDERHS VARCHAR(255),
        ROBLUBOILCYLINDERLS VARCHAR(255),
        SAILEDDISTANCEOVERGROUND VARCHAR(255),
        SAILEDDISTANCETHROUGHWATER VARCHAR(255),
        SAILINGTIME VARCHAR(255),
        SEASTATE VARCHAR(255),
        SEAWATERTEMP VARCHAR(255),
        STATE VARCHAR(255),
        SWELLDIRECTION VARCHAR(255),
        SWELLHEIGHT VARCHAR(255),
        TIMESTAMP VARCHAR(255),
        TOTALAVERAGEAEPOWER VARCHAR(255),
        TOTALAVERAGEELECTRICALPOWER VARCHAR(255),
        TOTALAVERAGEMEPOWER VARCHAR(255),
        TOTALAVERAGESHAFTPOWER VARCHAR(255),
        TOTALCO2AB VARCHAR(255),
        TOTALCO2IGS VARCHAR(255),
        TOTALCO2INC VARCHAR(255),
        TOTALCO2UNDEF VARCHAR(255),
        TOTALCYLINDERLSLUBOILCONSUMPTION VARCHAR(255),
        TOTALCYLINDEROILCONSUMPTION VARCHAR(255),
        TOTALFOC VARCHAR(255),
        TOTALFOCAE VARCHAR(255),
        TOTALFOCBUTANE VARCHAR(255),
        TOTALFOCETHANOL VARCHAR(255),
        TOTALFOCHFO VARCHAR(255),
        TOTALFOCHFOHS VARCHAR(255),
        TOTALFOCHFOLLS VARCHAR(255),
        TOTALFOCHFOLS VARCHAR(255),
        TOTALFOCLFO VARCHAR(255),
        TOTALFOCLNG VARCHAR(255),
        TOTALFOCMDO VARCHAR(255),
        TOTALFOCME VARCHAR(255),
        DAILYABFOC VARCHAR(255),
        NOXEMISSIONSAB VARCHAR(255),
        TOTALFOCAB VARCHAR(255),
        CALCULATEDPORTREEFERS VARCHAR(255),
        CARGOWEIGHT VARCHAR(255),
        MASTERSETA VARCHAR(255),
        DISTANCETOGO VARCHAR(255),
        TOTALCIRCULATIONLUBOILAECONSUMPTION VARCHAR(255),
        TOTALCO2ME VARCHAR(255),
        LEGEVENTID VARCHAR(255),
        TOTALCYLINDERHSLUBOILCONSUMPTION VARCHAR(255),
        GAINLOSSFUELHFO VARCHAR(255),
        GAINLOSSFUELHFOHS VARCHAR(255),
        MEHFOHSLCV VARCHAR(255),
        MEHFOHSSULPHURCONTENT VARCHAR(255),
        ROBFUELMGO VARCHAR(255),
        SOXEMISSIONSME VARCHAR(255),
        TOTALFOCISO VARCHAR(255),
        TOTALFOCMETHANOL VARCHAR(255),
        TOTALFOCMGO VARCHAR(255),
        TOTALFOCPROPANE VARCHAR(255),
        TOTALFOCUNDEF VARCHAR(255),
        TOTALCIRCULATIONLUBOILMECONSUMPTION VARCHAR(255),
        TOTALFRESHWATERCONSUMPTIONBOILER VARCHAR(255),
        TOTALFRESHWATERCONSUMPTIONDOMESTIC VARCHAR(255),
        TOTALCO2 VARCHAR(255),
        TOTALCO2AE VARCHAR(255),
        TOTALFRESHWATERCONSUMPTIONUNDEF VARCHAR(255),
        TOTALFRESHWATERCONSUMPTIONWASHING VARCHAR(255),
        BUNKERLUBOILCYLINDER VARCHAR(255),
        BUNKERLUBOILCYLINDERHS VARCHAR(255),
        TOTALGAINLOSSFUELOIL VARCHAR(255),
        BUNKERFUELMGO VARCHAR(255),
        TOTALGAINLOSSLUBOILCIRCULATION VARCHAR(255),
        TOTALGENERATEDAEENERGY VARCHAR(255),
        TOTALBUNKERFUELOIL VARCHAR(255),
        EEOIWEIGHT VARCHAR(255),
        TOTALGENERATEDELECTRICALENERGY VARCHAR(255),
        TOTALGENERATEDMEENERGY VARCHAR(255),
        DAILYMEFOCISO VARCHAR(255),
        DAILYTOTALFOCISO VARCHAR(255),
        TOTALGENERATEDSHAFTENERGY VARCHAR(255),
        TOTALNOXEMISSIONS VARCHAR(255),
        CARGOTEUEMPTY VARCHAR(255),
        CARGOTEUFULL VARCHAR(255),
        TOTALROBFRESHWATER VARCHAR(255),
        MEHFOSULPHURCONTENT VARCHAR(255),
        TOTALROBFUELOIL VARCHAR(255),
        TOTALROBLUBOILCIRCULATION VARCHAR(255),
        ROBFUELHFOLS VARCHAR(255),
        AER VARCHAR(255),
        TOTALRUNNINGHOURSME VARCHAR(255),
        TOTALSCOCME VARCHAR(255),
        CURRENTFACTOR VARCHAR(255),
        DOUGLASSEASCALE VARCHAR(255),
        TOTALSFOCAE VARCHAR(255),
        AVERAGERELATIVEAEGENERATORPOWER VARCHAR(255),
        TOTALSFOCAEISO VARCHAR(255),
        GAINLOSSFUELMGO VARCHAR(255),
        TOTALSFOCME VARCHAR(255),
        GAINLOSSFUELLFO VARCHAR(255),
        TOTALSFOCMEISO VARCHAR(255),
        TOTALSOXEMISSIONS VARCHAR(255),
        ABHFOLSSULPHURCONTENT VARCHAR(255),
        AEHFOLSSULPHURCONTENT VARCHAR(255),
        TRANSPORTEFFICIENCY VARCHAR(255),
        BALLASTWEIGHT VARCHAR(255),
        TRIM VARCHAR(255),
        DISPLACEMENT VARCHAR(255),
        UPDATEDDATE VARCHAR(255),
        SOXEMISSIONSAB VARCHAR(4000),
        VERSIONSTAMP VARCHAR(4000),
        SOXEMISSIONSAE VARCHAR(4000),
        VOYAGENAME VARCHAR(4000),
        EEOITEU VARCHAR(4000),
        WATERDEPTH VARCHAR(4000),
        WATERDEPTHBELOWKEEL VARCHAR(4000),
        MEHFOLSSULPHURCONTENT VARCHAR(4000),
        GAINLOSSFUELHFOLS VARCHAR(4000),
        WAVEDIRECTION VARCHAR(4000),
        WAVEHEIGHT VARCHAR(4000),
        BUNKERFUELHFO VARCHAR(4000),
        BUNKERFUELHFOLS VARCHAR(4000),
        WINDDIRREL VARCHAR(4000),
        WINDDIRTRUE VARCHAR(4000),
        ABHFOHSLCV VARCHAR(4000),
        ABHFOHSSULPHURCONTENT VARCHAR(4000),
        WINDFORCE VARCHAR(4000),
        AEHFOHSLCV VARCHAR(4000),
        WINDSPEEDREL VARCHAR(4000),
        WINDSPEEDRELKNOTS VARCHAR(4000),
        AEHFOHSSULPHURCONTENT VARCHAR(4000),
        AEMGOLCV VARCHAR(4000),
        WINDSPEEDTRUE VARCHAR(4000),
        WINDSPEEDTRUEKNOTS VARCHAR(4000),
        AEMGOSULPHURCONTENT VARCHAR(4000),
        DAILYABFOCISO VARCHAR(4000),
        TOTALFOCMEISO VARCHAR(4000),
        TOTALFOCABISO VARCHAR(4000),
        TOTALFOCAEISO VARCHAR(4000),
        DAILYAEFOCISO VARCHAR(4000),
        ROBFUELLFO VARCHAR(4000),
        BUNKERFUELHFOHS VARCHAR(4000),
        BUNKERFUELMDO VARCHAR(4000),
        IMONUM VARCHAR(4000),
        CURRENTCII VARCHAR(4000),
        CURRENTCIICORRECTED VARCHAR(4000),
        CIIREQUIRED VARCHAR(4000),
        CIIRATING VARCHAR(4000),
        CIIRATINGCORRECTED VARCHAR(4000),
        CIIATTAINED VARCHAR(4000),
        CIIATTAINEDCORRECTED VARCHAR(4000),
        CIIATTAINEDRATING VARCHAR(4000),
        CIIATTAINEDRATINGCORRECTED VARCHAR(4000),
        MONTHLYCII VARCHAR(4000),
        MONTHLYCIICORRECTED VARCHAR(4000),
        PLAUSIBILITYCOUNTACKNOWLEDGED VARCHAR(4000),
        ROBFRESHWATERDISTILLED VARCHAR(4000),
        ABMGOLLSLCV VARCHAR(4000),
        ABMGOLLSSULPHURCONTENT VARCHAR(4000),
        AEMGOLLSLCV VARCHAR(4000),
        AEMGOLLSSULPHURCONTENT VARCHAR(4000),
        MEMGOLLSLCV VARCHAR(4000),
        MEMGOLLSSULPHURCONTENT VARCHAR(4000),
        BUNKERFRESHWATERPRODUCED VARCHAR(4000),
        MONTHLYCIIRATING VARCHAR(4000),
        MONTHLYCIICORRECTEDRATING VARCHAR(4000),
        ROBFUELMGOLS VARCHAR(4000),
        ROBFUELMGOLLS VARCHAR(4000),
        BDNNUMBERS VARCHAR(4000),
        BDNNUMBERSFUEL VARCHAR(4000),
        BDNNUMBERSLUBOIL VARCHAR(4000),
        TOTALBUNKERFOSSILFUELAMOUNT VARCHAR(4000)
    );
    """
    mycursor.execute(create_sql)

def create_report_engine_table(database_name="DEV_WSM_DB", schema_name="API"):
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {database_name}.{schema_name}.BLUET_API_REPORTENGINE (
        NAME VARCHAR(255),
        AVERAGESHAFTRPM VARCHAR(255),
        SHAFTREVOLUTIONS VARCHAR(255),
        GENERATOREFFICIENCY VARCHAR(255),
        AVERAGESHAFTPOWER VARCHAR(255),
        GENERATEDSHAFTENERGY VARCHAR(255),
        RELATIVEPOWER VARCHAR(255),
        SLIPTHROUGHWATER VARCHAR(255),
        SLIPOVERGROUND VARCHAR(255),
        ENGINEDISTANCE VARCHAR(255),
        LUBOILCONSUMPTIONS VARCHAR(255),
        AVERAGEPOWER VARCHAR(255),
        GENERATEDENERGY VARCHAR(255),
        AVERAGENOXVALUE VARCHAR(255),
        CONSUMPTIONS VARCHAR(255),
        NO VARCHAR(255),
        RUNNINGHOURS VARCHAR(255),
        GENERATEDGENERATORENERGY VARCHAR(255),
        AVERAGEGENERATORPOWER VARCHAR(255),
        LOAD VARCHAR(255),
        AGGREGATE VARCHAR(255),
        KIND VARCHAR(255),
        AMOUNT VARCHAR(255),
        TYPESULPHURESTIMATED VARCHAR(255),
        LUBOIL VARCHAR(255),
        FRESHWATER VARCHAR(255),
        AMOUNTISO VARCHAR(255),
        TYPELCV VARCHAR(255),
        TYPESULPHUR VARCHAR(255),
        AMOUNTCO2 VARCHAR(255),
        TYPECO2FACTOR VARCHAR(255),
        RELATIVEGENERATORPOWER VARCHAR(255),
        TYPEGRADE VARCHAR(255),
        TYPEDENSITY VARCHAR(255),
        ENGINE VARCHAR(255),
        IMONUMBER VARCHAR(255),
        RUNNINGHOURSSHAFTGENERATOR VARCHAR(255),
        ID VARCHAR(255),
        TYPETBN VARCHAR(255),
        DENSITY VARCHAR(255),
        VOLUME VARCHAR(255),
        UPDATEDDATE VARCHAR(255),
        PURPOSE VARCHAR(4000),
        FRESHWATERKIND VARCHAR(4000),
        SFOC VARCHAR(4000),
        SFOCISO VARCHAR(4000),
        TOTALFOC VARCHAR(4000),
        TOTALFOCISO VARCHAR(4000)
    );
    """
    mycursor.execute(create_sql)


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
