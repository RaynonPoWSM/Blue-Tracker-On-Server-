import FromAPI as api
import ToSQL as sql
import pandas as pd
# import numpy as np
import logging
from datetime import date
# from dotenv import load_dotenv, dotenv_values
import os

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    filename=os.path.join("log", "RunGetReport.log"),
    datefmt='%Y-%m-%d %H:%M:%S')
today = date.today()


def flat(json_data, parent_key=''):
    flattened = {}
    for key, value in json_data.items():
        new_key = f"{parent_key}{key}" if parent_key else key
        if isinstance(value, dict):
            flattened.update(flat(value, new_key))
        else:
            flattened[new_key] = value
    return flattened


def generate_report_dataframes(vessel_imo, start=None, end=None, page_size=None):
    params = {}
    if start:
        params["start"] = start
    if end:
        params["end"] = end
    if page_size:
        params["PageSize"] = page_size

    engine_data_cur_vessel = []
    main_report_data_cur_vessel = []
    logging.info(f"Requesting report info of vessel {vessel_imo}")
    initial_response = api.get_reports(vessel_imo, params=params)
    num_pages = initial_response["pageCount"]
    logging.info(f"Number of reports to proceed: {initial_response['totalCount']}. Total {num_pages} pages.")
    for p in range(num_pages):
        new_param = params.copy()
        new_param["Page"] = p
        logging.info(f"Requesting for report info for vessel {vessel_imo}. Page {p}...")
        ship_reports_response = api.get_reports(vessel_imo, params=new_param)

        logging.info(f"Parsing report info for vessel {vessel_imo}. Page {p}...")
        ship_reports = ship_reports_response["items"]
        for report_item in ship_reports:
            report_id = report_item["reportId"]
            item_id = report_item["id"]

            # print(f"report ID = {report_id}")
            # Ignore all the keys containing "ship":
            report_item_no_ship = {k: v for k, v in report_item.items() if 'ship' not in k}

            # logging.info(f"Report ID: {report_id}")
            # get the aggregationDetails info then remove it.
            aggr = report_item_no_ship.pop("aggregationDetails",
                                           None)  # get the aggregationDetails info then remove it.
            if aggr:
                # noinspection PyTypeChecker
                aggr_flattened = flat(aggr)  # a dict of "str": [list]  # type: ignore
                for engine_key, engine_data in aggr_flattened.items():
                    # print(f"{engine_key=}")
                    # dd = json.dumps(engine_data, indent=4)
                    # print(dd)
                    # print("\n\n")
                    for engine_item in engine_data:
                        # Each will contribute 1 row
                        cur_row_dict = {
                            "ENGINE": str(engine_key),
                            "REPORTID": str(report_id),
                            "ID": str(item_id),
                        }
                        for k, v in engine_item.items():
                            if isinstance(v, list):
                                cur_row_dict[k] = ",".join([str(i) for i in v])
                            elif isinstance(v, dict):
                                for sub_k, sub_v in v.items():
                                    cur_row_dict[k + sub_k] = str(sub_v)
                            else:
                                cur_row_dict[k] = str(v)
                        engine_data_cur_vessel.append(cur_row_dict)  # Store into list

            # Proceed with the main report:
            main_report_info = flat(report_item_no_ship)
            main_report_info_str = {k: str(v) for k, v in main_report_info.items()}
            main_report_data_cur_vessel.append(main_report_info_str)

    # Generate engine report df
    engine_data_cur_vessel_df = pd.DataFrame(engine_data_cur_vessel)
    engine_data_cur_vessel_df["UPDATEDATE"] = sql.datesys
    engine_data_cur_vessel_df.columns = engine_data_cur_vessel_df.columns.str.upper()  # Convert column names to upper cases

    # Generate main report df
    main_report_data_cur_vessel_df = pd.DataFrame(main_report_data_cur_vessel)
    main_report_data_cur_vessel_df["UPDATEDATE"] = sql.datesys
    main_report_data_cur_vessel_df.columns = main_report_data_cur_vessel_df.columns.str.upper()  # Convert column names to upper cases

    return main_report_data_cur_vessel_df, engine_data_cur_vessel_df


# Empty string/list is stored literally as empty string ''
# Missing values are NaN, handled by pandas automatically
def ingest_report_summaries(vessel_imo, report_engine_table_name, main_report_table_name,
                            database_name=None, schema_name=None, first_time=False
                            ):
    """
    Ingest the data into Snowflake
    Empty string/list is stored literally as empty string ''.
    Missing values are NaN, handled by pandas automatically.
    :param vessel_imo:
    :param report_engine_table_name:
    :param main_report_table_name:
    :param database_name:
    :param schema_name:
    :param first_time: If this is the first time ingesting the table.
    :return: length of the 2 dataframes
    """
    # Get the pd DataFrames
    if first_time:
        main_report, engine_report = generate_report_dataframes(vessel_imo, page_size=100)
    else:
        main_report, engine_report = generate_report_dataframes(vessel_imo, start=sql.two_years_ago_date_str,
                                                                page_size=100)

    # Append IMO number to the dataframe
    main_report["IMONUMBER"] = str(vessel_imo)
    engine_report["IMONUMBER"] = str(vessel_imo)
    logging.info(f"Pushing data to Snowflake. Main report: {len(main_report)} rows. Engine report: {len(engine_report)} rows.")
    if first_time:
        sql.insert_data(engine_report, report_engine_table_name, schema_name=schema_name, database_name=database_name)
        sql.insert_data(main_report, main_report_table_name, schema_name=schema_name, database_name=database_name)
    else:
        # (conn, data_df, table_name, on=None, database_name=None, schema_name=None, temp_table_name=None)
        sql.upsert_data(main_report, main_report_table_name,
                        on=["IMONUMBER", "REPORTID"],
                        schema_name=schema_name, database_name=database_name)
        sql.upsert_data(engine_report, report_engine_table_name,
                        on=["IMONUMBER", "REPORTID"],
                        schema_name=schema_name, database_name=database_name)
    return len(main_report), len(engine_report)


report_table_name = "TEST_REPORT"
engine_table_name = "TEST_ENGINE"
total_main_report_count = 0
total_engine_report_count = 0
try:
    # sql.TruncateFile("BlueT_API_Report")
    # sql.TruncateFile("BlueT_API_ReportEngine")
    logging.info(date.today())
    logging.info("Run api")

    Vessels_all = api.get_vessels()

    num_page = Vessels_all['pageCount']
    num_vessels = Vessels_all['totalCount']
    logging.info(f"Number of vessels to proceed: {num_vessels}")

    all_vessel_imo = []
    for pg in range(num_page):
        resp_js = api.get_vessels({"Page": pg})
        all_vessel_imo += [vessel_info["imoNumber"] for vessel_info in resp_js['items']]

    first_time_to_ingest = False
    main_report_table_exists = sql.check_table_exists(report_table_name)
    if not main_report_table_exists:
        first_time_to_ingest = True
        # If main report does not exist, then also drop the engine table
        drop_table_result = sql.drop_table(engine_table_name)
        logging.info(drop_table_result)
        logging.info("Report tables have been initialized")

    # Create table if not exists
    sql.create_table_with_column(engine_table_name, initial_column_name="REPORTID")
    sql.create_table_with_column(report_table_name, initial_column_name="REPORTID")

    for idx, vessel in enumerate(all_vessel_imo):
        logging.info(f"Processing vessel {idx} out of {num_vessels} vessels.")
        logging.info(f"Vessel IMO = {vessel}")
        report_count, engine_count = ingest_report_summaries(vessel,
                                                             report_engine_table_name=engine_table_name,
                                                             main_report_table_name=report_table_name,
                                                             first_time=first_time_to_ingest
                                                             )
        total_main_report_count += report_count
        total_engine_report_count += engine_count
        logging.info(f"======== Vessel {vessel} done ========")

finally:
    print("complete")
    logging.info(f"ReportCount :{total_main_report_count}")
    logging.info(f"EngReportCount : {total_engine_report_count}")
    sql.closeConnection()
