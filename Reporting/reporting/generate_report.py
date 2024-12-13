import datetime
import json
import os
from enum import Enum
from pathlib import Path
import time
from requests import request
import pandas as pd
from external.report_auth import auth
import src.drive_download as drive
from src.utils import utils
import external.connectors as connectors
import composition.compose as cp


# Reorting helper class
class reporting_conf:
    def __init__(self):
        self.url = "https://nativetouch.api.beeswax.com/rest"
        self.ms_path = "MasterSheets"
        self.reports_path = "temp/Reporting"
        self.master_spredsheet_id = "1ftTgmEuLkWQz8cMiiwcIZLLrU7WLVRhO8t5Ub0ISDN4"
        os.makedirs(self.ms_path, exist_ok=1)
        os.makedirs(self.reports_path, exist_ok=1)
        self.logger = None
        self.celtra_date_offset = 7

    def set_pands_options(
        self,
        max_columns="display.max_columns",
        max_colwidth="display.max_colwidth",
        max_rows="display.max_rows",
    ):
        print("Setting Pandas Options")
        # pd.set_option(max_columns, None)
        # pd.set_option(max_colwidth, None)
        # pd.set_option(max_rows, None)

    def set_logger(self, DEBUG=False):
        self.logger = logger("debug" if DEBUG else "info")

    def fd_drop(self):
        return [
            "SFID",
            "Placement_Name",
            "Ad_Size",
            "Primary_KPI",
            "Primary_Benchmark",
            "Secondary_KPI",
            "Secondary_Benchmark",
            "Rate",
            "Rate_Type",
            "Daily_Target",
            "Target_Total",
            "AM",
        ]

    def mapping_rules(self):
        return {
            "Cross Device Banners": "Banners",
            "DOOH": "DOOH",
            #'Foot Traffic Attribution': 'NTFTA',
            "Rich Media Interstitial": "RM Interstitials",
            "Interactive Video": "Interactive Videos",
            "Mobile Persona - Banner - Smartphone": "Banners",
            "Vertical Video": "Videos",
            "Retargeting - Banners": "Banners",
            "Rich Media CTE": "CTEs",
            "Rich Media Shoppable": "Shoppable",
            "Rich Media Video": "RM Interstitials",
            "Static Interstitial": "Static Interstitial",
            "Standard Interstitial": "Static Interstitial",
            "Skinned Video": "Videos",
            "Standard Banner": "Banners",
            "Standard Banner CPC": "Banners",
            "Standard Banner In-App": "Banners",
            "Standard Banner Web": "Banners",
            "Standard Banners In-App/Web": "Banners",
            "Standard Video": "Videos",
            "Synced Video": "Videos",
            "CTV": "CTV",
            "Connveted tv": "CTV",
        }


# Simple Logger
class logger:
    def __init__(self, mode):
        if mode == "debug":
            self.mode = LogLevel.DEBUG
        elif mode == "info":
            self.mode = LogLevel.INFO
        elif mode == "error":
            self.mode = LogLevel.ERROR

    def log(self, message, level=2):
        if self.mode.value <= level.value:
            print(message)


# Log Levels for the logger
class LogLevel(Enum):
    DEBUG = 3
    INFO = 2
    ERROR = 1


# Report Task
class report_task:
    def __init__(self, task_name, task_session, **kwargs):
        self.task_name = task_name
        self.task_session = task_session
        self.task_data = None
        self.kwargs = kwargs

    def run(self):
        if self.task_name == "beeswax":
            self.beeswax()
        if self.task_name == "aws":
            self.AWS()
        if self.task_name == "celtra":
            self.celtra()
        print(self.task_data)

    def beeswax(self):
        con = connectors.bw_conector(
            self.kwargs["url"],
            self.kwargs["io"],
            f'{self.kwargs["path"]}/BW',
            self.kwargs["reach"],
        )
        con.get_data()

    def AWS(self):
        con = connectors.athena_connector(f'{self.kwargs["path"]}/AWS')
        con.get_data()

    def celtra(self):
        con = connectors.celtra_connector(f'{self.kwargs["path"]}/celtra', self.kwargs["offset"])
        con.get_data()


# Reporting Core Exuctuion Path
class pd_processor:

    def __init__(self, path):
        self.o_path = path

    def daily(self, daily):
        df = pd.read_csv(daily)
        return df

    def exposure(self, exposure):
        df = pd.read_csv(exposure)
        return df

    def mapping(self, mapping, daily):
        df = pd.read_csv(mapping)
        df = df.rename(
            columns={
                "Line Item ID": "line_item_id",
                "Line Item Alternative ID": "line_item_alternative_id",
                "Creative ID": "creative_id",
                "Creative Alternative ID": "creative_alternative_id",
                "Creative Name": "creative_name",
            }
        )
        df_daily = pd.read_csv(daily)
        df_daily = df_daily.rename(
            columns={"Line Item ID": "line_item_id", "Line Item Name": "line_item_name"}
        )
        df_daily = df_daily[["line_item_id", "line_item_name"]]
        df = pd.merge(df, df_daily, how="left", on="line_item_id")
        df = df.drop_duplicates(subset=["line_item_id"])
        df.drop("creative_alternative_id", axis=1, inplace=True)
        return df

    def geo(self, geo):
        df = pd.read_csv(geo)
        df = df.rename(
            columns={
                "Line Item Alternative ID": "line_item_alternative_id",
                "Day": "date",
                "Country": "geo_country",
                "Region/State": "geo_metro",
                "City": "geo_city",
                "Impressions": "impressions",
                "Clicks": "clicks",
                "Conversions": "conversions",
                "Measurable Impressions": "measureable",
                "Viewable Impressions": "viewability",
                "Video Completes": "video_completes",
            }
        )
        return df

    def app(self, app):
        df = pd.read_csv(app)
        df["Clicks"] = 0
        df["Conversions"] = 0
        df["Measurable Impressions"] = 0
        df["Viewable Impressions"] = 0
        df["Video Completes"] = 0
        # Add more columns as needed
        df = df.rename(
            columns={
                "Line Item Alternative ID": "line_item_alternative_id",
                "Day": "date",
                "App Name": "app/domain",
                "Impressions": "impressions",
                "Clicks": "clicks",
                "Conversions": "conversions",
                "Measurable Impressions": "measureable",
                "Viewable Impressions": "viewability",
                "Video Completes": "video_completes",
            }
        )
        return df

    def domain(self, domain):
        df = pd.read_csv(domain)
        df["Clicks"] = 0
        df["Conversions"] = 0
        df["Measurable Impressions"] = 0
        df["Viewable Impressions"] = 0
        df["Video Completes"] = 0
        # Add more columns as needed
        df = df.rename(
            columns={
                "Line Item Alternative ID": "line_item_alternative_id",
                "Day": "date",
                "Domain": "app/domain",
                "Impressions": "impressions",
                "Clicks": "clicks",
                "Conversions": "conversions",
                "Measurable Impressions": "measureable",
                "Viewable Impressions": "viewability",
                "Video Completes": "video_completes",
            }
        )
        return df

    def weekly_reach(self, weekly_reach):
        df = pd.read_csv(weekly_reach)
        df = df.rename(columns={"Line Item Alternative ID": "line_item_alternative_id"})
        return pd.read_csv(weekly_reach)

    def li_reach(self, li_reach):
        df = pd.read_csv(li_reach)
        df = df.rename(columns={"Line Item Alternative ID": "line_item_alternative_id"})
        return df

    def BW_daily(self, daily, mapping, d2):
        df = self.daily(daily)
        print(df.columns)
        df_mapping = self.mapping(mapping, d2)
        df = pd.merge(
            df, df_mapping, how="left", on="line_item_id", suffixes=("", "_remove")
        )
        df.drop([i for i in df.columns if "remove" in i], axis=1, inplace=True)
        if not os.path.exists(self.o_path):
            os.makedirs(self.o_path)
        df.to_csv(f"{self.o_path}/BW_daily.csv", index=False)

    def CL_Exposure(self, exposure, events, Pixel):
        df = self.exposure(exposure)
        #print(df.columns)
        #df_mapping = self.mapping(mapping, d2)
        #df = pd.merge(
        #    df, df_mapping, how="left", on="line_item_id", suffixes=("", "_remove")
        #)
        #df.drop([i for i in df.columns if "remove" in i], axis=1, inplace=True)
        #if not os.path.exists(self.o_path):
        #    os.makedirs(self.o_path)
        #df.to_csv(f"{self.o_path}/BW_daily.csv", index=False)

    def BW_daily_domain(self, app, domain, daily):
        df_daily = self.daily(daily)
        df_merged = pd.concat([self.app(app), self.domain(domain)], ignore_index=True)
        df_merged = pd.merge(
            df_daily,
            df_merged,
            how="left",
            on=["line_item_alternative_id", "date"],
            suffixes=("", "_remove"),
        )
        df_merged.drop(
            [i for i in df_merged.columns if "remove" in i], axis=1, inplace=True
        )
        if not os.path.exists(self.o_path):
            os.makedirs(self.o_path)
        df_merged.to_csv(f"{self.o_path}/BW_daily_domain.csv", index=False)

    def BW_reach(self, weekly_reach, li_reach):
        df = pd.read_csv(weekly_reach)
        df_li = pd.read_csv(li_reach)
        if not os.path.exists(self.o_path):
            os.makedirs(self.o_path)
        df.to_csv(f"{self.o_path}/BW_weekly_reach.csv", index=False)
        df_li.to_csv(f"{self.o_path}/BW_li_reach.csv", index=False)

    def SF_MS(self, ms):
        df = pd.read_csv(ms)
        if not os.path.exists(self.o_path):
            os.makedirs(self.o_path)
        df.to_csv(f"{self.o_path}/SF_Report.csv", index=False)


class reporting:
    # Todo Remove Debug
    def __init__(
        self, args, auto_grouping=None, Logging=None, ai_engagement_categorization=False
    ):
        # init reporting config
        self.rc = reporting_conf()
        if args['celtra_date']:
            given_date = datetime.datetime.strptime(args['celtra_date'], "%Y-%m-%d")
            today = datetime.datetime.today()
            self.rc.celtra_date_offset  = (today - given_date).days
            print(self.rc.celtra_date_offset)
        self.rc.set_pands_options()
        self.rc.set_logger(Logging)

        # set the reporting parameters
        print(args)
        self.io_number = str(args["io_number"])
        self.auto_grouping = auto_grouping
        self.g_folder_id = drive.Folder_ID_Chek(str(args["g_folder_id"]))

        self.rc.logger.log("Reporting Initiated:", LogLevel.DEBUG)

        if args["master_spredsheet_id"] == None:
            drive.File_ID_Check(str(args["master_spredsheet_id"]))

        self.rc.logger.log("Mastersheet Set:", LogLevel.DEBUG)

        # set AI engagement categorization
        self.ai_engagement_categorization = ai_engagement_categorization

        # Authenticate and set the proxy
        services = [
            self.get_service("Beeswax"),
            self.get_service("AWS"),
            self.get_service("SF"),
            self.get_service("Celtra"),
        ]
        print(services)
        # TODO: Add the fix path
        a_obj = auth("/Users/ian/Documents/Projects/nt-adops/src/config")
        self.sessions = a_obj.authenticate(services)
        self.generate_mastersheet("MasterSheet", io_number=self.io_number, path=self.rc.reports_path)
        # self.celtra_token = self.auth.getToken("celtra")
        # self.openai = self.auth.getSession("openai")
        # self.dv_s = self.auth.getSession("google", ["doubleclickbidmanager", "v2"])
        # self.bs_token = self.auth.getSession("broadsign")

    def configure_reporting(self):
        # Load Mastersheet
        ms = pd.read_csv(f"{self.rc.reports_path}/MasterSheetMasterSheet.csv")
        items = []
        for i in ms.values:
            try:
                print(i)
                item = json.loads(i[2])
            except json.JSONDecodeError:
                corrected_string = i[2].replace("\'", '"').replace("None", "NaN")
                print(corrected_string)
                item = json.loads(corrected_string)
            items.append(item)
        cols = items[0].keys()
        items_df = pd.DataFrame(items, columns=cols)
        items_df.rename(
            columns={
                "IO__c": "IO",
                "Start_Date__c": "Start_Date",
                "End_Date__c": "End_Date",
                "Rate_Type__c": "Rate_Type",
                "Total_Spend__c": "Total_Spend",
                "Volume__c": "Volume",
                "Primary_KPI__c": "Primary_KPI",
                "Secondary_KPI__c": "Secondary_KPI",
                'Primary_Benchmark__c': 'Primary_Benchmark',
                'Secondary_Benchmark__c': 'Secondary_Benchmark',
                'Rate__c': 'Rate',
                'Daily_Target__c': 'Daily_Target',
                'Target_Total__c': 'Target_Total',
                'Account_Manager__c': 'AM',
                'Ad_Size__c': 'Ad_Size',
                'Additional_Targetting_New__c': 'Targetting',
                'Placement_Name__c': 'Placement_Name',
                'Campaign_Name__c': 'Campaign_Name',
                'Advertiser__c': 'Advertiser',
                'Agency__c': 'Agency',
                'ContractLineIdentifier__c': 'SFID',
            },
            inplace=True,
        )
        items_df.drop(columns=["attributes", "Contract_Line_Item__r"], inplace=True)

        filtered_data = items_df[
            (items_df["IO"] == self.io_number) & (items_df["Rate_Type"] != "Flat Rate")
        ].copy()
        filtered_data.fillna("n/a", inplace=True)
        filtered_data.to_csv(f"{self.rc.reports_path}/filtered_data.csv")
        self.filtered_data = filtered_data.copy()
        self.rc.logger.log("Data Filtered:", LogLevel.DEBUG)

        filtered_data["Start_Date"] = filtered_data["Start_Date"].min()
        filtered_data["End_Date"] = filtered_data["End_Date"].max()
        filtered_data["Total_Spend"] = filtered_data["Total_Spend"].sum()
        filtered_data["Volume"] = filtered_data["Volume"].sum()
        filtered_data = filtered_data.reset_index(drop=True)
        print(filtered_data.columns)
        self.campaign = filtered_data.drop(columns=self.rc.fd_drop()).loc[0].to_dict()
        self.campaign["Start_End_Dates"] = (
            f'{(datetime.datetime.strptime(self.campaign["Start_Date"], "%Y-%m-%d")).strftime("%Y-%m-%d")} - {datetime.datetime.strptime(self.campaign["End_Date"], "%Y-%m-%d").strftime("%Y-%m-%d")}'
        )
        self.rc.logger.log("Campaign Built:", LogLevel.DEBUG)

    def run_reporting(self, services=["Beeswax", "AWS", "SF", "Celtra"]):
        self.build_report(None)
        self.campaign_kpis = (
            self.filtered_data["Primary_KPI"].unique().tolist()
            + self.filtered_data["Secondary_KPI"].unique().tolist()
        )
        self.campaign_kpis = (
            ["n/a"] if len(self.campaign_kpis) == 0 else self.campaign_kpis
        )
        self.run_tasks(services)

    def process_data(self, source, r_path=None):
        proc = pd_processor(f"{self.rc.reports_path}/Raw")
        path = self.rc.reports_path
        if r_path:
            path = r_path
        if source == "BW":
            files = {}
            files = {
                x.split(".")[0]: f"{path}/BW/{x}" for x in os.listdir(f"{path}/BW")
            }
            proc.BW_daily(f"{path}/AWS/BW_Daily.csv", files["mapping"], files["daily"])
            #proc.BW_daily_domain(files["app"], files["domain"],files["daily"])
            #proc.BW_reach(files["weekly_lineitem_reach"],files["li_reach"])
        if source == "SF":
            proc.SF_MS(f"{self.rc.reports_path}/filtered_data.csv")
        if source == "CL":
            proc.CL_Exposure(f"{self.rc.reports_path}/celtra/celtra_exposure.csv", f'{self.rc.reports_path}/celtra/celtra_event.csv', f'{self.rc.reports_path}/AWS/Pixel.csv')

    def build_final_report(self):
        compose = cp.compose(f"{self.rc.reports_path}/Raw")
        compose.build_report()

    def build_report(self, existing_report, delete_old=False):
        Spreadsheet_Name = f"Native touch Weekly Report {self.campaign['Campaign_Name']}:{self.io_number}"
        r_type = self.report_type(existing_report, Spreadsheet_Name, delete_old)

        if r_type == "existing_report" or r_type == "existing_report_update":
            self.rc.logger.log("Existing Report Found:", LogLevel.DEBUG)
            self.spreadsheet_id = existing_report[0]["id"]
            existing_spreadsheet_name = existing_report[0]["name"]
            if r_type == "existing_report_update":
                drive.Delete_Drive_File(self.spreadsheet_id, existing_spreadsheet_name)
                self.spreadsheet_id = drive.Copy_spreadsheet(
                    self.rc.master_spredsheet_id, self.g_folder_id, Spreadsheet_Name
                )
        else:
            self.rc.logger.log("New Report:", LogLevel.DEBUG)
            self.spreadsheet_id = "THIS IS A TEST"

        self.existing_report = r_type == "existing_report"

    def config_mastersheet(self):
        file = max(
            [f for f in os.scandir(self.rc.reports_path)],
            key=lambda x: x.stat().st_mtime,
        ).name
        return self.build_mastersheet(file)

    def build_mastersheet(self, file):
        # print("SF/" + latest_edited_file)
        ms = pd.read_csv(self.rc.reports_path + "/" + file)
        print(ms.columns)
        ms.Start_Date = ms.Start_Date.astype("datetime64[ns]")
        ms.End_Date = ms.End_Date.astype("datetime64[ns]")
        ms = ms[ms.IO == self.io_number.upper()]
        ms.reset_index(drop=True, inplace=True)
        return ms

    def report_type(self, report, spreadsheet_name, delete_old):
        if report and len(report) > 1:
            raise Exception(
                f"Multiple files named {spreadsheet_name} found, Please only keep one. (keep any other files with the same name in a folder named DNU )"
            )
        elif report and len(report) == 1 and not delete_old:
            return "existing_report"
        elif report and len(report) == 1 and delete_old:
            return "existing_report_update"
        else:
            return "new_report"

    def run_tasks(self, services):
        print("Running Tasks")
        print(self.sessions)
        tasks = [
            report_task(x["source"], x["session"], **x["args"]) for x in self.sessions
        ]
        print(tasks)
        for task in tasks:
            print(task.task_name)
            if task.task_name not in services:
                continue
            else:
                print(f"Running {task.task_name} Task")
                task.run()

    def get_service(self, service):
        if service == "Beeswax":
            return {
                "source": "beeswax",
                "session": None,
                "args": {
                    "url": self.rc.url,
                    "io": self.io_number,
                    "path": self.rc.reports_path,
                    "ai": self.ai_engagement_categorization,
                    "reach": True,
                },
            }
        if service == "AWS":
            return {
                "source": "aws",
                "session": None,
                "args": {"path": self.rc.reports_path},
            }
        if service == "SF":
            return {
                "source": "salesforce",
                "session": None,
                "args": {"path": self.rc.reports_path},
            }
        if service == "Celtra":
            return {
                "source": "celtra",
                "session": None,
                "args": {"path": self.rc.reports_path, "offset": self.rc.celtra_date_offset},
            }
        else:
            return None

    def generate_mastersheet(self, timestamp=None, io_number=None, path=None):
        token = next(session["session"] for session in self.sessions if session["source"] == "salesforce")
        start_time = time.time()
        base_url = "https://nativetouch.my.salesforce.com/services/data/v51.0/query?"
        # query_string=f"q=SELECT+Master_Order__c.IO__c,Master_Order__c.Agency__c,Master_Order__c.Advertiser__c,Master_Order__c.Campaign_Name__c,Master_Order__c.Contract_Line_Item__r.Contract__r.Name,Master_Order__c.ContractLineIdentifier__c,Master_Order__c.Placement_Name__c,Master_Order__c.Start_Date__c,Master_Order__c.End_Date__c,Master_Order__c.Ad_Size__c,Master_Order__c.Additional_Targetting_New__c,Master_Order__c.Primary_KPI__c,Master_Order__c.Primary_Benchmark__c,Master_Order__c.Secondary_KPI__c,Master_Order__c.Secondary_Benchmark__c,Master_Order__c.Rate__c,Master_Order__c.Volume__c,Master_Order__c.Rate_Type__c,Master_Order__c.Daily_Target__c,Master_Order__c.Target_Total__c,Master_Order__c.Total_Spend__c,Master_Order__c.Account_Manager__c+FROM+Master_Order__c+WHERE+Status__c='Active'+AND+Contract_Status__c='Activated'+AND+IsDeleted=FALSE+AND+Archived_MO__c=FALSE+AND+End_Date__c+>+{self.last_of_3_months_prior()}"
        query_dict = {
            "select": [
                "Master_Order__c.IO__c",
                "Master_Order__c.Agency__c",
                "Master_Order__c.Advertiser__c",
                "Master_Order__c.Campaign_Name__c",
                "Master_Order__c.Contract_Line_Item__r.Contract__r.Name",
                "Master_Order__c.ContractLineIdentifier__c",
                "Master_Order__c.Placement_Name__c",
                "Master_Order__c.Start_Date__c",
                "Master_Order__c.End_Date__c",
                "Master_Order__c.Ad_Size__c",
                "Master_Order__c.Primary_KPI__c",
                "Master_Order__c.Primary_Benchmark__c",
                "Master_Order__c.Secondary_KPI__c",
                "Master_Order__c.Secondary_Benchmark__c",
                "Master_Order__c.Rate__c",
                "Master_Order__c.Volume__c",
                "Master_Order__c.Rate_Type__c",
                "Master_Order__c.Daily_Target__c",
                "Master_Order__c.Target_Total__c",
                "Master_Order__c.Total_Spend__c",
                "Master_Order__c.Account_Manager__c",
            ],
            "from": "Master_Order__c",
            "where": [
                "Status__c='Active'",
                "Archived_MO__c=false",
                "IsDeleted=false",
                "Product_Type__c='Media'",
                "End_Date__c+>+"
                + f"{(datetime.datetime.today()).strftime('%Y-%m-%d')}",
            ],
        }
        if io_number is not None:
            IO_Filter = f"+AND+Master_Order__c.IO__c='{io_number}'"
        else:
            IO_Filter = ""

        select_clause = ",".join(query_dict["select"])
        where_clause = "+AND+".join(query_dict["where"])
        query_string = f"q=SELECT+{select_clause}+FROM+{query_dict['from']}+WHERE+{where_clause}{IO_Filter}"
        payload = {}
        headers = {
            "Authorization": "Bearer " + token,
            "Content-Type": "application/json",
        }

        response_SF = request(
            "GET", base_url + query_string, headers=headers, data=payload
        )
        df_data = json.loads(response_SF.content)
        SF = pd.DataFrame(df_data)

        try:
            Next_Page = df_data["nextRecordsUrl"]
            while Next_Page.startswith("/services/data/"):
                # print(str(len(SF)) + " / " + str(Total_Size) + "Lines Added")
                url = (
                    "https://nativetouch.my.salesforce.com" + df_data["nextRecordsUrl"]
                )
                response_SF = request("GET", url, headers=headers, data=payload)
                df_data = json.loads(response_SF.content)
                SF_TEMP = pd.DataFrame(df_data)
                SF = pd.concat([SF, SF_TEMP], ignore_index=True, sort=False)

                df_data = json.loads(response_SF.content)
                if df_data["done"]:
                    # print(str(len(SF)) + " / " + str(Total_Size) + "Lines Added")
                    break
        except:
            pass

        self.save_sheet(timestamp, SF, custom_path=path)
        end_time = time.time()
        elapsed_time = end_time - start_time
        if elapsed_time > 60:
            minutes = int(elapsed_time // 60)
            seconds = int(elapsed_time % 60)
            elapsed_time = f"{round(minutes,0)}m:{round(seconds,0)}s"
        else:
            elapsed_time = f"{round(elapsed_time, 3)}s"

        print(f" | {elapsed_time}")

    def save_sheet(self, timestamp, dataframe, is_billing=None, custom_path=None):
        output_dir = "MasterSheets"
        if not custom_path in [None, False]:
            output_dir = custom_path
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        if timestamp is None:
            timestamp = str(pd.Timestamp.now())
        file_path = (
            f"{output_dir}/MasterSheet{timestamp}.csv"
            if not is_billing
            else f"{timestamp}.csv"
        )
        dataframe.to_csv(file_path, index=False)
        print(
            "Master sheet Updated" if not is_billing else "Billing sheet ready", end=""
        )
