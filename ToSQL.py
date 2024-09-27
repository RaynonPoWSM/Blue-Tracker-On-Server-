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
    CREATE TABLE If NOT EXISTS {database_name}.{schema_name}.BlueT_API_Ships (
        id VARCHAR(10),
        imoNumber VARCHAR(10),
        name VARCHAR(20),
        alternativeName VARCHAR(20),
        currentVersionStamp INT,
        shipClassId VARCHAR(5),
        shipClassName VARCHAR(50),
        portOfRegistryUnloc VARCHAR(10),
        isHidden VARCHAR(10),
        callSign VARCHAR(10),
        UpdatedDate TIMESTAMP,
        shortName VARCHAR(5)
    );
    """
    mycursor.execute(create_sql)


def create_event_table(database_name="DEV_WSM_DB", schema_name="API"):
    create_sql = f"""
    create or replace TABLE {database_name}.{schema_name}.BLUET_API_EVENTS (
        ID VARCHAR(10),
        CUSTOMID VARCHAR(50),
        TIMESTAMP VARCHAR(4000),
        EVENTTYPE VARCHAR(100),
        IMONUMBER VARCHAR(10),
        PORTUNLOC VARCHAR(10),
        PORTNAME VARCHAR(50),
        VOYAGENAME VARCHAR(20),
        UPDATEDDATE VARCHAR(255)
    );
    """
    mycursor.execute(create_sql)


def create_voyage_table(database_name="DEV_WSM_DB", schema_name="API"):
    create_sql = f"""
    create or replace TABLE {database_name}.{schema_name}.BLUET_API_VOYAGES (
        ID VARCHAR(255),
        NAME VARCHAR(255),
        IMONUMBER VARCHAR(255),
        UPDATEDDATE VARCHAR(255)
    );
    """
    mycursor.execute(create_sql)


def create_voyage_summary_table(database_name="DEV_WSM_DB", schema_name="API"):
    create_sql = f"""
    create or replace TABLE {database_name}.{schema_name}.BLUET_API_VOYAGESUMMARY (
        ID VARCHAR(10),
        VERSIONSTAMP NUMBER(38,0),
        SHIPID VARCHAR(10),
        SHIPIMONUMBER VARCHAR(10),
        SHIPNAME VARCHAR(20),
        SHIPALTERNATIVENAME VARCHAR(20),
        SHIPCURRENTVERSIONSTAMP NUMBER(38,0),
        SHIPSHIPCLASSID VARCHAR(5),
        SHIPPORTOFREGISTRYUNLOC VARCHAR(10),
        SHIPISHIDDEN VARCHAR(10),
        NAME VARCHAR(20),
        DURATION NUMBER(9,4),
        SAILINGTIME NUMBER(9,4),
        DISTANCESAILEDOVERGROUND NUMBER(9,4),
        DISTANCESAILEDTHROUGHWATER NUMBER(9,4),
        TOTALFOC NUMBER(9,4),
        TOTALFOCME NUMBER(9,4),
        TOTALFOCAE NUMBER(9,4),
        TOTALFOCAB NUMBER(9,4),
        TOTALCO2 NUMBER(9,4),
        TOTALCO2ME NUMBER(9,4),
        TOTALCO2AE NUMBER(9,4),
        TOTALCO2AB NUMBER(9,4),
        TOTALCO2IGS NUMBER(9,4),
        TOTALCO2INC NUMBER(9,4),
        TOTALFOCHFO NUMBER(9,4),
        TOTALFOCHFOHS NUMBER(9,4),
        TOTALFOCHFOLS NUMBER(9,4),
        TOTALFOCHFOLLS NUMBER(9,4),
        TOTALFOCLFO NUMBER(9,4),
        TOTALFOCMDO NUMBER(9,4),
        TOTALFOCMGO NUMBER(9,4),
        TOTALFOCPROPANE NUMBER(9,4),
        TOTALFOCBUTANE NUMBER(9,4),
        TOTALFOCLNG NUMBER(9,4),
        TOTALFOCMETHANOL NUMBER(9,4),
        TOTALFOCETHANOL NUMBER(9,4),
        EEOITEU NUMBER(9,4),
        EEOIWEIGHT NUMBER(9,4),
        TOTALCYLINDEROILCONSUMPTION NUMBER(12,4),
        TOTALCIRCULATIONLUBOILMECONSUMPTION NUMBER(12,4),
        TOTALCIRCULATIONLUBOILAECONSUMPTION NUMBER(12,4),
        SOXEMISSIONSME NUMBER(9,4),
        SOXEMISSIONSAE NUMBER(9,4),
        SOXEMISSIONSAB NUMBER(9,4),
        TOTALSOXEMISSIONS NUMBER(9,4),
        NOXEMISSIONSME NUMBER(9,4),
        NOXEMISSIONSAE NUMBER(9,4),
        NOXEMISSIONSAB NUMBER(9,4),
        TOTALNOXEMISSIONS NUMBER(9,4),
        ENGINEDISTANCE NUMBER(12,4),
        ESTIMATIONSCORE NUMBER(12,4),
        COMPLETENESSSCORE NUMBER(9,4),
        PLAUSIBILITYSCORE NUMBER(9,4),
        TOTALFOCISO NUMBER(9,4),
        TOTALFOCMEISO NUMBER(9,4),
        TOTALFOCAEISO NUMBER(9,4),
        TOTALFOCABISO NUMBER(9,4),
        SHIPCALLSIGN VARCHAR(10),
        TOTALFOCIGS NUMBER(9,4),
        TOTALFOCIGSISO NUMBER(9,4),
        TOTALFRESHWATERCONSUMPTIONWASHING NUMBER(9,4),
        TOTALFRESHWATERCONSUMPTIONDOMESTIC NUMBER(9,4),
        TOTALFRESHWATERCONSUMPTIONUNDEF NUMBER(9,4),
        BUNKERFRESHWATERBUNKERED NUMBER(9,4),
        BUNKERFRESHWATERPRODUCED NUMBER(9,4),
        TOTALFRESHWATERCONSUMPTIONBOILER NUMBER(9,4),
        TOTALFOCINC NUMBER(9,4),
        TOTALFOCINCISO NUMBER(9,4),
        UPDATEDDATE DATE,
        SHIPSHORTNAME VARCHAR(10)
    );
    """
    mycursor.execute(create_sql)


def create_legsummary_table(database_name="DEV_WSM_DB", schema_name="API"):
    create_sql = f"""
    create or replace TABLE {database_name}.{schema_name}.BLUET_API_LEGSUMMARY (
        ID VARCHAR(10),
        REFERENCENUMBER NUMBER(38,0),
        LEGEVENTID VARCHAR(10),
        VERSIONSTAMP VARCHAR(10),
        SHIPID VARCHAR(10),
        SHIPIMONUMBER VARCHAR(10),
        SHIPNAME VARCHAR(50),
        SHIPALTERNATIVENAME VARCHAR(50),
        SHIPCALLSIGN VARCHAR(10),
        SHIPCURRENTVERSIONSTAMP VARCHAR(10),
        SHIPSHIPCLASSID VARCHAR(10),
        SHIPPORTOFREGISTRYUNLOC VARCHAR(10),
        SHIPISHIDDEN VARCHAR(10),
        PORTOFORIGINID VARCHAR(10),
        PORTOFORIGINUNLOC VARCHAR(10),
        PORTOFORIGINNAME VARCHAR(50),
        PORTOFDESTINATIONID VARCHAR(10),
        PORTOFDESTINATIONUNLOC VARCHAR(10),
        PORTOFDESTINATIONNAME VARCHAR(50),
        DEPARTURETIME TIMESTAMP_NTZ(9),
        ARRIVALTIME TIMESTAMP_NTZ(9),
        CAPTAIN VARCHAR(50),
        CHIEFENG VARCHAR(50),
        DURATION NUMBER(9,4),
        SAILINGTIME NUMBER(9,4),
        DISTANCESAILEDOVERGROUND NUMBER(9,4),
        DISTANCESAILEDTHROUGHWATER NUMBER(9,4),
        CARGOWEIGHT NUMBER(12,4),
        CARGOREEFERS NUMBER(12,4),
        TOTALFOC NUMBER(9,4),
        TOTALFOCME NUMBER(9,4),
        TOTALFOCAE NUMBER(9,4),
        TOTALFOCAB NUMBER(9,4),
        TOTALCO2 NUMBER(9,4),
        TOTALCO2ME NUMBER(9,4),
        TOTALCO2AE NUMBER(9,4),
        TOTALCO2AB NUMBER(9,4),
        TOTALCO2IGS NUMBER(9,4),
        TOTALCO2INC NUMBER(9,4),
        TOTALFOCHFO NUMBER(9,4),
        TOTALFOCHFOHS NUMBER(9,4),
        TOTALFOCHFOLS NUMBER(9,4),
        TOTALFOCHFOLLS NUMBER(9,4),
        TOTALFOCLFO NUMBER(9,4),
        TOTALFOCMDO NUMBER(9,4),
        TOTALFOCMGO NUMBER(9,4),
        TOTALFOCPROPANE NUMBER(9,4),
        TOTALFOCBUTANE NUMBER(9,4),
        TOTALFOCLNG NUMBER(9,4),
        TOTALFOCMETHANOL NUMBER(9,4),
        TOTALFOCETHANOL NUMBER(9,4),
        EEOIWEIGHT NUMBER(9,4),
        TOTALCYLINDEROILCONSUMPTION NUMBER(12,4),
        TOTALCIRCULATIONLUBOILAECONSUMPTION NUMBER(12,4),
        NOXEMISSIONSME NUMBER(9,4),
        NOXEMISSIONSAE NUMBER(9,4),
        NOXEMISSIONSAB NUMBER(9,4),
        TOTALNOXEMISSIONS NUMBER(9,4),
        ESTIMATIONSCORE NUMBER(9,4),
        COMPLETENESSSCORE NUMBER(9,4),
        PLAUSIBILITYSCORE NUMBER(9,4),
        BALLASTWEIGHT NUMBER(9,4),
        CARGOTEUFULL NUMBER(9,4),
        CARGOTEUEMPTY NUMBER(9,4),
        EEOITEU NUMBER(9,4),
        SOXEMISSIONSME NUMBER(9,4),
        SOXEMISSIONSAE NUMBER(9,4),
        SOXEMISSIONSAB NUMBER(9,4),
        TOTALSOXEMISSIONS NUMBER(9,4),
        ENGINEDISTANCE NUMBER(9,4),
        TOTALCIRCULATIONLUBOILMECONSUMPTION NUMBER(9,4),
        TOTALFOCISO NUMBER(9,4),
        TOTALFOCMEISO NUMBER(9,4),
        UPDATEDDATE TIMESTAMP_NTZ(9),
        SHIPSHORTNAME VARCHAR(10),
        TOTALFOCAEISO NUMBER(9,4),
        TOTALFOCABISO NUMBER(9,4),
        TOTALFRESHWATERCONSUMPTIONDOMESTIC NUMBER(9,4),
        TOTALFRESHWATERCONSUMPTIONBOILER NUMBER(9,4),
        TOTALFRESHWATERCONSUMPTIONWASHING NUMBER(9,4),
        TOTALFRESHWATERCONSUMPTIONUNDEF NUMBER(9,4),
        BUNKERFRESHWATERPRODUCED NUMBER(9,4),
        VOYAGEID VARCHAR(10),
        VOYAGENAME VARCHAR(50)
    );
    """
    mycursor.execute(create_sql)

def create_report_table(database_name="DEV_WSM_DB", schema_name="API"):
    create_sql = f"""
    create or replace TABLE {database_name}.{schema_name}.BLUET_API_REPORT (
        ABHFOFOC NUMBER(9,4),
        ABHFOHSFOC NUMBER(9,4),
        ABHFOLLSFOC NUMBER(9,4),
        ABHFOLSFOC NUMBER(9,4),
        ABLFOFOC NUMBER(9,4),
        ABMDOFOC NUMBER(9,4),
        ABMDOHSFOC NUMBER(9,4),
        ABMDOLLSFOC NUMBER(9,4),
        ABMDOLSFOC NUMBER(9,4),
        ABMGOFOC NUMBER(9,4),
        ABMGOLLSFOC NUMBER(9,4),
        ABMGOLSFOC NUMBER(9,4),
        ABMETHANOLFOC NUMBER(9,4),
        ABLNGFOC NUMBER(9,4),
        ABBTLFOC NUMBER(9,4),
        ABFAMEFOC NUMBER(9,4),
        ABHVOFOC NUMBER(9,4),
        ABBIOFUELBLENDFOC NUMBER(9,4),
        AEHFOFOC NUMBER(9,4),
        AEHFOHSFOC NUMBER(9,4),
        AEHFOLLSFOC NUMBER(9,4),
        AEHFOLSFOC NUMBER(9,4),
        AELFOFOC NUMBER(9,4),
        AEMDOFOC NUMBER(9,4),
        AEMDOHSFOC NUMBER(9,4),
        AEMDOLLSFOC NUMBER(9,4),
        AEMDOLSFOC NUMBER(9,4),
        AEMGOFOC NUMBER(9,4),
        AEMGOLLSFOC NUMBER(9,4),
        AEMGOLSFOC NUMBER(9,4),
        AEMETHANOLFOC NUMBER(9,4),
        AELNGFOC NUMBER(9,4),
        AEBTLFOC NUMBER(9,4),
        AEFAMEFOC NUMBER(9,4),
        AEHVOFOC NUMBER(9,4),
        AEBIOFUELBLENDFOC NUMBER(9,4),
        AIRPRESS NUMBER(9,4),
        AIRTEMP NUMBER(9,4),
        AVERAGERELATIVEAEPOWER NUMBER(9,4),
        AVERAGERELATIVEMEPOWER NUMBER(9,4),
        AVERAGESHAFTRPM NUMBER(12,4),
        AVERAGESLIP NUMBER(12,4),
        AVERAGESLIPOVERGROUND NUMBER(12,4),
        AVERAGESPEEDOVERGROUND NUMBER(12,4),
        AVERAGESPEEDTHROUGHWATER NUMBER(12,4),
        CAPTAIN VARCHAR(50),
        CARGOREEFERS NUMBER(12,4),
        CHIEFENG VARCHAR(50),
        COMPLETENESSSCORE NUMBER(9,4),
        COOLINGWATERTEMP NUMBER(9,4),
        COURSEMADEGOOD NUMBER(9,4),
        DAILYAEFOC NUMBER(9,4),
        DAILYMEFOC NUMBER(9,4),
        DAILYTOTALFOC NUMBER(9,4),
        DRAFTAFT NUMBER(9,4),
        DRAFTFWD NUMBER(9,4),
        DRAFTMID NUMBER(9,4),
        ENGINEDISTANCE NUMBER(12,4),
        ENGINEROOMTEMP NUMBER(12,4),
        ESTIMATIONSCORE NUMBER(9,4),
        EVENTCUSTOMID VARCHAR(50),
        EVENTNAME VARCHAR(50),
        EVENTSHORTNAME VARCHAR(10),
        EVENTTYPE VARCHAR(20),
        GAINLOSSLUBOILCIRCULATIONAE NUMBER(9,4),
        GAINLOSSLUBOILCIRCULATIONME NUMBER(9,4),
        GAINLOSSLUBOILCYLINDER NUMBER(9,4),
        GAINLOSSLUBOILCYLINDERHS NUMBER(9,4),
        GAINLOSSLUBOILCYLINDERLS NUMBER(9,4),
        HEADING NUMBER(9,4),
        ID VARCHAR(10),
        ISSLOWSTEAMING VARCHAR(10),
        ISTCCUTOUT VARCHAR(10),
        MEANDRAFT NUMBER(9,4),
        MEHFOFOC NUMBER(9,4),
        MEHFOHSFOC NUMBER(9,4),
        MEHFOLLSFOC NUMBER(9,4),
        MEHFOLSFOC NUMBER(9,4),
        MELFOFOC NUMBER(9,4),
        MEMDOFOC NUMBER(9,4),
        MEMDOHSFOC NUMBER(9,4),
        MEMDOLLSFOC NUMBER(9,4),
        MEMDOLSFOC NUMBER(9,4),
        MEMGOFOC NUMBER(9,4),
        MEMGOLLSFOC NUMBER(9,4),
        MEMGOLSFOC NUMBER(9,4),
        MEMETHANOLFOC NUMBER(9,4),
        MELNGFOC NUMBER(9,4),
        MEBTLFOC NUMBER(9,4),
        MEFAMEFOC NUMBER(9,4),
        MEHVOFOC NUMBER(9,4),
        MEBIOFUELBLENDFOC NUMBER(9,4),
        NOXEMISSIONSAE NUMBER(9,4),
        NOXEMISSIONSME NUMBER(9,4),
        PERIOD NUMBER(9,4),
        PLAUSIBILITYCOUNTFINE NUMBER(9,4),
        PLAUSIBILITYCOUNTMAJOR NUMBER(9,4),
        PLAUSIBILITYCOUNTMINOR NUMBER(9,4),
        PLAUSIBILITYCOUNTNOTSET NUMBER(9,4),
        PLAUSIBILITYSCORE NUMBER(9,4),
        POSLAT NUMBER(9,4),
        POSLNG NUMBER(9,4),
        REPORTID VARCHAR(10),
        ROBFUELHFO NUMBER(12,4),
        ROBFUELHFOHS NUMBER(12,4),
        ROBFUELMDO NUMBER(12,4),
        ROBLUBOILCIRCULATIONAE NUMBER(12,4),
        ROBLUBOILCIRCULATIONME NUMBER(12,4),
        ROBLUBOILCYLINDER NUMBER(12,4),
        ROBLUBOILCYLINDERHS NUMBER(12,4),
        ROBLUBOILCYLINDERLS NUMBER(12,4),
        SAILEDDISTANCEOVERGROUND NUMBER(9,4),
        SAILEDDISTANCETHROUGHWATER NUMBER(9,4),
        SAILINGTIME NUMBER(9,4),
        SEASTATE NUMBER(9,4),
        SEAWATERTEMP NUMBER(9,4),
        STATE VARCHAR(20),
        SWELLDIRECTION NUMBER(9,4),
        SWELLHEIGHT NUMBER(9,4),
        TIMESTAMP TIMESTAMP_NTZ(9),
        TOTALAVERAGEAEPOWER NUMBER(9,4),
        TOTALAVERAGEELECTRICALPOWER NUMBER(9,4),
        TOTALAVERAGEMEPOWER NUMBER(9,4),
        TOTALAVERAGESHAFTPOWER NUMBER(9,4),
        TOTALCO2AB NUMBER(9,4),
        TOTALCO2IGS NUMBER(9,4),
        TOTALCO2INC NUMBER(9,4),
        TOTALCO2UNDEF NUMBER(9,4),
        TOTALCYLINDERLSLUBOILCONSUMPTION NUMBER(9,4),
        TOTALCYLINDEROILCONSUMPTION NUMBER(9,4),
        TOTALFOC NUMBER(9,4),
        TOTALFOCAE NUMBER(9,4),
        TOTALFOCBUTANE NUMBER(9,4),
        TOTALFOCETHANOL NUMBER(9,4),
        TOTALFOCHFO NUMBER(9,4),
        TOTALFOCHFOHS NUMBER(9,4),
        TOTALFOCHFOLLS NUMBER(9,4),
        TOTALFOCHFOLS NUMBER(9,4),
        TOTALFOCLFO NUMBER(9,4),
        TOTALFOCLNG NUMBER(9,4),
        TOTALFOCMDO NUMBER(9,4),
        TOTALFOCME NUMBER(9,4),
        DAILYABFOC NUMBER(9,4),
        NOXEMISSIONSAB NUMBER(9,4),
        TOTALFOCAB NUMBER(9,4),
        CALCULATEDPORTREEFERS NUMBER(9,4),
        CARGOWEIGHT NUMBER(12,4),
        MASTERSETA TIMESTAMP_NTZ(9),
        DISTANCETOGO NUMBER(9,4),
        TOTALCIRCULATIONLUBOILAECONSUMPTION NUMBER(9,4),
        TOTALCO2ME NUMBER(9,4),
        LEGEVENTID VARCHAR(10),
        TOTALCYLINDERHSLUBOILCONSUMPTION NUMBER(9,4),
        GAINLOSSFUELHFO NUMBER(9,4),
        GAINLOSSFUELHFOHS NUMBER(9,4),
        MEHFOHSLCV NUMBER(9,4),
        MEHFOHSSULPHURCONTENT NUMBER(9,4),
        ROBFUELMGO NUMBER(12,4),
        SOXEMISSIONSME NUMBER(9,4),
        TOTALFOCISO NUMBER(9,4),
        TOTALFOCMETHANOL NUMBER(9,4),
        TOTALFOCMGO NUMBER(9,4),
        TOTALFOCPROPANE NUMBER(9,4),
        TOTALFOCUNDEF NUMBER(9,4),
        TOTALCIRCULATIONLUBOILMECONSUMPTION NUMBER(9,4),
        TOTALFRESHWATERCONSUMPTIONBOILER NUMBER(9,4),
        TOTALFRESHWATERCONSUMPTIONDOMESTIC NUMBER(9,4),
        TOTALCO2 NUMBER(9,4),
        TOTALCO2AE NUMBER(9,4),
        TOTALFRESHWATERCONSUMPTIONUNDEF NUMBER(9,4),
        TOTALFRESHWATERCONSUMPTIONWASHING NUMBER(9,4),
        BUNKERLUBOILCYLINDER NUMBER(9,4),
        BUNKERLUBOILCYLINDERHS NUMBER(9,4),
        TOTALGAINLOSSFUELOIL NUMBER(12,4),
        BUNKERFUELMGO NUMBER(9,4),
        TOTALGAINLOSSLUBOILCIRCULATION NUMBER(12,4),
        TOTALGENERATEDAEENERGY NUMBER(12,4),
        TOTALBUNKERFUELOIL NUMBER(9,4),
        EEOIWEIGHT NUMBER(9,4),
        TOTALGENERATEDELECTRICALENERGY NUMBER(12,4),
        TOTALGENERATEDMEENERGY NUMBER(12,4),
        DAILYMEFOCISO NUMBER(9,4),
        DAILYTOTALFOCISO NUMBER(9,4),
        TOTALGENERATEDSHAFTENERGY NUMBER(12,4),
        TOTALNOXEMISSIONS NUMBER(12,4),
        CARGOTEUEMPTY NUMBER(12,4),
        CARGOTEUFULL NUMBER(12,4),
        TOTALROBFRESHWATER NUMBER(9,4),
        MEHFOSULPHURCONTENT NUMBER(9,4),
        TOTALROBFUELOIL NUMBER(9,4),
        TOTALROBLUBOILCIRCULATION NUMBER(9,4),
        ROBFUELHFOLS NUMBER(12,4),
        AER NUMBER(9,4),
        TOTALRUNNINGHOURSME NUMBER(9,4),
        TOTALSCOCME NUMBER(12,4),
        CURRENTFACTOR NUMBER(9,4),
        DOUGLASSEASCALE NUMBER(9,4),
        TOTALSFOCAE NUMBER(12,4),
        AVERAGERELATIVEAEGENERATORPOWER NUMBER(9,4),
        TOTALSFOCAEISO NUMBER(12,4),
        GAINLOSSFUELMGO NUMBER(9,4),
        TOTALSFOCME NUMBER(12,4),
        GAINLOSSFUELLFO NUMBER(9,4),
        TOTALSFOCMEISO NUMBER(12,4),
        TOTALSOXEMISSIONS NUMBER(9,4),
        ABHFOLSSULPHURCONTENT NUMBER(9,4),
        AEHFOLSSULPHURCONTENT NUMBER(9,4),
        TRANSPORTEFFICIENCY NUMBER(9,4),
        BALLASTWEIGHT NUMBER(9,4),
        TRIM NUMBER(9,4),
        DISPLACEMENT NUMBER(9,4),
        UPDATEDDATE VARCHAR(20),
        SOXEMISSIONSAB NUMBER(9,4),
        VERSIONSTAMP VARCHAR(10),
        SOXEMISSIONSAE NUMBER(9,4),
        VOYAGENAME VARCHAR(20),
        EEOITEU NUMBER(9,4),
        WATERDEPTH NUMBER(9,4),
        WATERDEPTHBELOWKEEL NUMBER(9,4),
        MEHFOLSSULPHURCONTENT NUMBER(9,4),
        GAINLOSSFUELHFOLS NUMBER(9,4),
        WAVEDIRECTION NUMBER(9,4),
        WAVEHEIGHT NUMBER(9,4),
        BUNKERFUELHFO NUMBER(9,4),
        BUNKERFUELHFOLS NUMBER(9,4),
        WINDDIRREL NUMBER(9,4),
        WINDDIRTRUE NUMBER(9,4),
        ABHFOHSLCV NUMBER(9,4),
        ABHFOHSSULPHURCONTENT NUMBER(9,4),
        WINDFORCE NUMBER(9,4),
        AEHFOHSLCV NUMBER(9,4),
        WINDSPEEDREL NUMBER(9,4),
        WINDSPEEDRELKNOTS NUMBER(9,4),
        AEHFOHSSULPHURCONTENT NUMBER(9,4),
        AEMGOLCV NUMBER(9,4),
        WINDSPEEDTRUE NUMBER(9,4),
        WINDSPEEDTRUEKNOTS NUMBER(9,4),
        AEMGOSULPHURCONTENT NUMBER(9,4),
        DAILYABFOCISO NUMBER(9,4),
        TOTALFOCMEISO NUMBER(9,4),
        TOTALFOCABISO NUMBER(9,4),
        TOTALFOCAEISO NUMBER(9,4),
        DAILYAEFOCISO NUMBER(9,4),
        ROBFUELLFO NUMBER(12,4),
        BUNKERFUELHFOHS NUMBER(9,4),
        BUNKERFUELMDO NUMBER(9,4),
        IMONUM VARCHAR(10),
        CURRENTCII NUMBER(9,4),
        CURRENTCIICORRECTED NUMBER(9,4),
        CIIREQUIRED NUMBER(9,4),
        CIIRATING VARCHAR(10),
        CIIRATINGCORRECTED VARCHAR(10),
        CIIATTAINED NUMBER(9,4),
        CIIATTAINEDCORRECTED NUMBER(9,4),
        CIIATTAINEDRATING VARCHAR(10),
        CIIATTAINEDRATINGCORRECTED VARCHAR(10),
        MONTHLYCII NUMBER(9,4),
        MONTHLYCIICORRECTED NUMBER(9,4),
        PLAUSIBILITYCOUNTACKNOWLEDGED NUMBER(9,4),
        ROBFRESHWATERDISTILLED NUMBER(12,4),
        ABMGOLLSLCV NUMBER(9,4),
        ABMGOLLSSULPHURCONTENT NUMBER(9,4),
        AEMGOLLSLCV NUMBER(9,4),
        AEMGOLLSSULPHURCONTENT NUMBER(9,4),
        MEMGOLLSLCV NUMBER(9,4),
        MEMGOLLSSULPHURCONTENT NUMBER(9,4),
        BUNKERFRESHWATERPRODUCED NUMBER(9,4),
        MONTHLYCIIRATING VARCHAR(10),
        MONTHLYCIICORRECTEDRATING VARCHAR(10),
        ROBFUELMGOLS NUMBER(12,4),
        ROBFUELMGOLLS NUMBER(12,4),
        BDNNUMBERS VARCHAR(20),
        BDNNUMBERSFUEL VARCHAR(10),
        BDNNUMBERSLUBOIL VARCHAR(20),
        TOTALBUNKERFOSSILFUELAMOUNT NUMBER(9,4)
    );
    """
    mycursor.execute(create_sql)

def create_report_engine_table(database_name="DEV_WSM_DB", schema_name="API"):
    create_sql = f"""
    create or replace TABLE {database_name}.{schema_name}.BLUET_API_REPORTENGINE (
        NAME VARCHAR(10),
        AVERAGESHAFTRPM NUMBER(12,4),
        SHAFTREVOLUTIONS NUMBER(14,4),
        GENERATOREFFICIENCY NUMBER(12,4),
        AVERAGESHAFTPOWER VARCHAR(20),
        GENERATEDSHAFTENERGY NUMBER(12,4),
        RELATIVEPOWER NUMBER(9,4),
        SLIPTHROUGHWATER NUMBER(12,4),
        SLIPOVERGROUND NUMBER(12,4),
        ENGINEDISTANCE NUMBER(12,4),
        LUBOILCONSUMPTIONS VARCHAR(100),
        AVERAGEPOWER NUMBER(9,4),
        GENERATEDENERGY NUMBER(12,4),
        AVERAGENOXVALUE NUMBER(9,4),
        CONSUMPTIONS VARCHAR(10),
        NO VARCHAR(10),
        RUNNINGHOURS NUMBER(9,4),
        GENERATEDGENERATORENERGY NUMBER(12,4),
        AVERAGEGENERATORPOWER NUMBER(9,4),
        LOAD NUMBER(9,4),
        "AGGREGATE" VARCHAR(20),
        KIND VARCHAR(20),
        AMOUNT NUMBER(9,4),
        TYPESULPHURESTIMATED VARCHAR(10),
        LUBOIL NUMBER(9,4),
        FRESHWATER NUMBER(9,4),
        AMOUNTISO NUMBER(9,4),
        TYPELCV VARCHAR(10),
        TYPESULPHUR VARCHAR(10),
        AMOUNTCO2 NUMBER(9,4),
        TYPECO2FACTOR VARCHAR(10),
        RELATIVEGENERATORPOWER NUMBER(9,4),
        TYPEGRADE VARCHAR(10),
        TYPEDENSITY VARCHAR(10),
        ENGINE VARCHAR(50),
        IMONUMBER VARCHAR(10),
        RUNNINGHOURSSHAFTGENERATOR NUMBER(9,4),
        ID VARCHAR(10),
        TYPETBN NUMBER(9,4),
        DENSITY NUMBER(9,4),
        VOLUME NUMBER(9,4),
        UPDATEDDATE TIMESTAMP_NTZ(9),
        PURPOSE VARCHAR(10),
        FRESHWATERKIND VARCHAR(10),
        SFOC NUMBER(12,4),
        SFOCISO NUMBER(12,4),
        TOTALFOC NUMBER(9,4),
        TOTALFOCISO NUMBER(9,4)
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
                    VALUES (%s, %s, %s, %s)
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
