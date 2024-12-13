import base64
from datetime import datetime, timedelta
from duckdb import df
import pandas as pd
import json
import time
import httplib2
from pyathena import connect
import csv
import os
import requests


class conf:

    def __init__(self, conf_path):
        # read config json file
        self.creds = self.read_config_file(f"{conf_path}/conf.json")

    def read_config_file(self, path):
        with open(path) as file:
            return json.load(file)


class conector:
    def __init__(self, type):
        self.type = type
        self.conf = conf(".")

    def get_data(self):
        raise NotImplementedError()


class bw_conector(conector):

    def __init__(self, url, io_number, storage_path, reach=True):
        super().__init__("BW")
        self.url = url
        self.io_number = io_number
        self.storage_path = storage_path
        self.reach = reach

    def get_data(self):
        report_url = f"{self.url}/v2/reporting/run-query"
        callback_url = f"{self.url}/v2/reporting/async-results"
        queries = self.BW_get_queries(self.io_number)

        print("\nGetting BW Data...")
        for name, query in queries.items():
            print(f"Getting {name} report...")
            task, cookies = self.BW_run_query(report_url, query)
            self.BW_get_report_async(
                callback_url, cookies, task, self.storage_path, name
            )

    # BW functions
    def BW_run_query(self, url, query):
        h = httplib2.Http(timeout=60)
        cookies = self.BW_auth()
        resp, data = h.request(
            url,
            "POST",
            headers={
                "Content-Type": "application/json; charset=UTF-8",
                "Cookie": cookies,
            },
            body=json.dumps(query),
        )
        task_id = None
        if "status" in resp and resp["status"] == "200":
            dataobj = json.loads(data)
            if "task_id" in dataobj:
                task_id = dataobj["task_id"]
        return task_id, cookies

    def BW_auth(self):
        api_secrets = self.conf.creds["API"]["BEESWAX"]
        h = httplib2.Http()
        body = {
            "email": api_secrets["API_USER"],
            "password": api_secrets["API_USER_PASSWORD"],
            "keep_logged_in": api_secrets["API_KEEP_LOGGED_IN"],
        }
        resp, data = h.request(
            f"{api_secrets['API_BASE_URL']}/authenticate",
            "POST",
            headers={"Content-Type": "application/json; charset=UTF-8"},
            body=json.dumps(body),
        )
        cookies = None
        if "status" in resp and resp["status"] == "200":
            dataobj = json.loads(data)
            if "success" in dataobj and dataobj["success"]:
                cookies = resp["set-cookie"]

                # httplib2 doesn't manage cookies well
                # so we are removing data that should not appear in cookie when using it
                cookies = cookies.replace(" Path=/; ", " ")
                cookies = cookies.replace(" SameSite=None; ", " ")
                cookies = cookies.replace(" Secure, ", " ")
        return cookies

    def BW_get_report_async(self, url, cookies, task_id, storage_path, name):
        continue_query = True
        itteration = 0
        report = None
        while continue_query:
            report, continue_query = self.BW_poll_report(url, task_id, cookies)
            time.sleep(5)
            print(f"Polling {name} report...")
            if itteration > 20:
                print("Error: Timeout")
                continue_query = False
        if report:
            df = pd.DataFrame(report)
            try:
                df.to_csv(f"{storage_path}/{name}.csv", index=False)
            except OSError as e:
                os.makedirs(storage_path, exist_ok=True)
                df.to_csv(f"{storage_path}/{name}.csv", index=False)

    def BW_poll_report(self, url, task_id, cookies):
        h = httplib2.Http(timeout=60)
        resp, data = h.request(
            f"{url}/{task_id}",
            "GET",
            headers={
                "Content-Type": "application/json; charset=UTF-8",
                "Cookie": cookies,
            },
        )
        if resp is None:
            return None, True

        if "status" in resp:
            if resp["status"] == "200":
                return json.loads(data), False
            elif resp["status"] == "204":
                return None, True

    def BW_get_queries(self, io_number):
        return {
            "creative": {
                "fields": [
                    "creative_name",
                    "creative_id",
                    "impression",
                    "clicks",
                    "measurable_impressions",
                    "viewable_impressions",
                    "video_completes",
                    "conversion_count",
                ],
                "view": "performance_agg",
                "filters": {
                    "account_id_filter": "2",
                    "campaign_alternative_id": io_number,
                    "bid_day": "NOT NULL",
                },
                "result_format": "json",
            },
            "campaign_agg": {
                "fields": [
                    "impression",
                    "clicks",
                    "measurable_impressions",
                    "viewable_impressions",
                    "video_completes",
                    "conversion_count",
                    "reach",
                    "revenue",
                ],
                "view": "performance_agg",
                "filters": {
                    "account_id_filter": "2",
                    "campaign_alternative_id": io_number,
                    "bid_day": "NOT NULL",
                },
                "result_format": "json",
            },
            "mapping": {
                "fields": [
                    "line_item_alternative_id",
                    "line_item_id",
                    "creative_alternative_id",
                    "creative_id",
                    "creative_name",
                ],
                "view": "performance_agg",
                "filters": {
                    "account_id_filter": "2",
                    "campaign_alternative_id": io_number,
                    "bid_day": "NOT NULL",
                },
                "result_format": "json",
            },
            "geo": {
                "fields": [
                    "line_item_alternative_id",
                    "country_name",
                    "region_name",
                    "city_name",
                    "bid_day",
                    "impression",
                    "clicks",
                    "measurable_impressions",
                    "viewable_impressions",
                    "video_completes",
                    "conversion_count",
                ],
                "view": "geo_agg",
                "filters": {
                    "account_id_filter": "2",
                    "campaign_alternative_id": io_number,
                    "bid_day": "NOT NULL",
                },
                "result_format": "json",
            },
            "li_reach": {
                "fields": ["line_item_alternative_id", "reach"],
                "view": "performance_agg",
                "filters": {
                    "account_id_filter": "2",
                    "campaign_alternative_id": io_number,
                    "bid_day": "NOT NULL",
                },
                "result_format": "json",
            },
            "daily": {
                "fields": [
                    "line_item_name",
                    "line_item_alternative_id",
                    "line_item_id",
                    "creative_name",
                    "creative_id",
                    "bid_day",
                    "impression",
                    "clicks",
                    "measurable_impressions",
                    "viewable_impressions",
                    "video_completes",
                    "conversion_count",
                ],
                "view": "performance_agg",
                "filters": {
                    "account_id_filter": "2",
                    "campaign_alternative_id": io_number,
                    "bid_day": "NOT NULL",
                },
                "result_format": "json",
            },
            "domain": {
                "fields": [
                    "line_item_alternative_id",
                    "bid_day",
                    "domain",
                    "impression",
                ],
                "view": "domain_agg",
                "filters": {
                    "account_id_filter": "2",
                    "campaign_alternative_id": io_number,
                    "bid_day": "NOT NULL",
                },
                "result_format": "json",
            },
            "app": {
                "fields": [
                    "line_item_alternative_id",
                    "bid_day",
                    "app_name",
                    "impression",
                ],
                "view": "app_agg",
                "filters": {
                    "account_id_filter": "2",
                    "campaign_alternative_id": io_number,
                    "bid_day": "NOT NULL",
                },
                "result_format": "json",
            },
            "weekly_lineitem_reach": {
                "fields": ["line_item_alternative_id", "bid_week", "reach"],
                "view": "performance_agg",
                "filters": {
                    "account_id_filter": "2",
                    "bid_day": "NOT NULL",
                    "campaign_alternative_id": io_number,
                },
                "result_format": "json",
                "query_timezone": "Pacific/Kiritimati",
            },
        }


class athena_connector(conector):

    def __init__(self, path):
        super().__init__("Athena")
        self.s3_path = "s3://aws-athena-query-results-290318218202-us-east-1"
        self.path = path
        if not os.path.exists(path):
            os.makedirs(path)
        self.region_name = "us-east-1"
        self.connect = connect

    def get_data(self):
        try:
            temp_data = self.read_csv_file("./temp/Reporting/BW/daily.csv")
            ids = set([f"'{row['Line Item ID']}'" for row in temp_data])
            id_list = ",".join(ids)
            start_dates = [row["Day"] for row in temp_data]
            farthest_start_date = min(start_dates)
            farthest_start_date = f"'{farthest_start_date.split(' ')[0]}'"
            # Use the 'farthest_start_date' in the query
            #Get Raw Data
            query = self.build_query(farthest_start_date, id_list)
            data = self.execute_athena_query(query, fetchdata=True)
            df_data = pd.DataFrame(data, columns=["campaign_id", "creative_id", "line_item_id", "geo_city", "geo_country", "geo_metro", "date", "clicks", "measureable", "viewability", "conversions", "video_completes", "impresions", "apps", "domains"])
            df_data.to_csv(f"{self.path}/BW_daily.csv", index=False)
            id = df_data["campaign_id"].unique().tolist()[0]
            #Get Pixel Data
            query = self.build_pixel_query(farthest_start_date, datetime.today().strftime("%Y-%m-%d"),id)
            data = self.execute_athena_query(query, fetchdata=True)
            df_data = pd.DataFrame(data, columns=["total_event_occurance", "creative_id", "main_event", "event_type", "event_label", "date", "placement_name"])
            df_data.to_csv(f"{self.path}/nt_pixel_exposure.csv", index=False)
        except Exception as e:
            print(f"Error: {e}")
            return None

    def read_csv_file(self, file_path):
        data = []
        with open(file_path, "r") as file:
            reader = csv.DictReader(file)
            for row in reader:
                data.append(row)
        return data

    def execute_athena_query(self, query, fetchdata=False):
        data, conn, cursor = None, None, None

        if query is not None:
            try:
                conn = self.connect(
                    s3_staging_dir=self.s3_path, region_name=self.region_name
                )
                cursor = conn.cursor()
                res = cursor.execute(query)
                if fetchdata:
                    data = res.fetchall()
            except Exception as err:
                print(f"Error: {err}")
                raise err
            finally:
                if cursor:
                    cursor.close()
                if conn:
                    conn.close()
            return data
        else:
            return None

    def build_query(self, date, line_items):
        return f"""
        select campaign_id as campaign_id,creative_id as creative_id,line_item_id as line_item_id,geo_city as geo_city,geo_country as geo_country,geo_metro as geo_metro,CAST(y || '-' || m || '-' || d as date) as "date",
            sum(cast(clicks as double)) as clicks,
            sum(case when cast(is_measurable as double) < 0 then 0 else cast(is_measurable as double) end) as measureable,
            sum(case when cast(in_view as double) < 0 then 0 else cast(in_view as double) end) as viewability,
            sum(cast(conversions as double)) as conversions,
            sum(cast(video_completes as double)) as video_completes,
            count() as impresions,
            app_name as apps,
            domain as domains
        from
            beeswax.wins_daily
        where
            CAST(y || '-' || m || '-' || d as date) > date({date})
            and line_item_id in ({line_items})
        group by
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            14,
            15
        """

    def build_pixel_query(self, date_start,date_end,campagin_id):
        return f"""
            select
            count(1) as total_event_occurance
            , creative_id
            , main_event
            , event_type
            , event_label
            , date
            , placement_name
            from
            (select
            params['creative_id'] as creative_id
            , params['event'] as main_event
            , params['event_type'] as event_type
            , params['event_value'] as event_value
            , params['event_label'] as event_label
            , params['auction_id'] as auction_id
            , from_unixtime(try_cast(params['timestamp'] as bigint) / 1000) as timestamp
            , CAST(y || '-' || m || '-' || d as date) as date
            , cast(time as timestamp) as received_time
            , params['campaign_id'] as campaign_id
            , params['timestamp'] as timestamp_in_ms
            , params['line_item_alt_id'] as sfid
            , params['line_item_name'] as placement_name
            from
            (with
            dataset as (
                select
                time
                , forwarded_for
                , split(path, '?') [1] as path
                , split(url_extract_query(path), '&') as params
                , y
                , m
                , d
                from
                tracking.pixel
                where
                CAST(y || '-' || m || '-' || d as timestamp) + interval '5' hour >= (cast(date({date_start}) as timestamp) + interval '5' hour) and CAST(y || '-' || m || '-' || d as timestamp) + interval '5' hour < (cast(cast(date('{date_end}') as timestamp) + interval '5' hour as timestamp) + interval '1' day)
            )
            , dataset_kv as (
                select
                time
                , path
                , forwarded_for
                , try(transform(params, x->split(x, '=') [1])) as key
                , try(transform(params, x->split(x, '=') [2])) as value
                , y
                , m
                , d
                from
                dataset
            )
            , dataset_map as (
                select
                time
                , path
                , forwarded_for
                , map(key, value) as params
                , y
                , m
                , d
                from
                dataset_kv
            )
            , dataset_campaign as (
                select
                *
                , params['campaign_id'] as campaign
                from
                dataset_map
            )
            select
            *
            from
            dataset_campaign
            where
            path in (
                '/pixel'
                , '/pixel/'
            )
            and 1=1) as tracking_pixel
            where
            params['creative_id'] is not null
            and params['campaign_id'] in ('{campagin_id}')
            and CAST(y || '-' || m || '-' || d as timestamp) + interval '5' hour >= (cast(date({date_start}) as timestamp) + interval '5' hour) and CAST(y || '-' || m || '-' || d as timestamp) + interval '5' hour < (cast(cast(date('{date_end}') as timestamp) + interval '5' hour as timestamp) + interval '1' day)) as pixel_select_event
            where
            1=1
            and main_event is not null
            group by
            2
            , 3
            , 4
            , 5
            , 6
            , 7
            order by
            creative_id
            , date
        """

class celtra_connector(conector):

    def __init__(self, path, lookback_days=None):
        super().__init__("Celtra")
        self.c_token = ""
        self.storage_path = path
        self.celtra_token = self.celtra()
        self.lookback_days = lookback_days if lookback_days else 7
        self.custom_dimentions = None
        self.custom_metrics = None
        self.custom_filter = None

    def celtra(self):
        api_secrets = self.conf.creds["API"]["CELTRA"]
        # Get secret values
        C_API_SECRET_KEY = api_secrets["API_SECRET_KEY"]
        C_API_APP_ID = api_secrets["API_APP_ID"]

        c_token = str(
            base64.b64encode(f"{C_API_APP_ID}:{C_API_SECRET_KEY}".encode()).decode()
        )

        return c_token

    def get_data(self):
        base_url = "https://hub.celtra.com/api/analytics"
        format = "format=csv"
        human = "human=1"
        header = {"Authorization": f"Basic {str(self.celtra_token)}"}

        # campaign_id = Mapping_df.loc[Mapping_df["Campaign name"].str.contains(io_number), "Campaign ID"].values[0]

        Event_metrics = "metrics=customEventOccurs"
        Event_dimensions = "dimensions=accountDate,creativeId,externalCreativeId,externalLineItemId,label"
        Event_metrics = self.custom_metrics if self.custom_metrics else Event_metrics
        Event_dimensions = (
            self.custom_dimentions if self.custom_dimentions else Event_dimensions
        )

        Exposure_metrics = "metrics=viewableTime,timeOnScreen,sessionsWithExpandAttempt"
        Exposure_dimensions = (
            "dimensions=accountDate,creativeId,externalCreativeId,externalLineItemId"
        )

        filters = "filters.accountId=ba748c8c"
        if self.custom_filter:
            self.custom_filter = [
                f for f in self.custom_filter if f != "filters.accountId=ba748c8c"
            ]
        else:
            self.custom_filter = None

        filters = (
            f'{filters}&{"&".join(self.custom_filter)}'
            if self.custom_filter
            else f"{filters}"
        )
        if self.lookback_days:
            lookback_date = (
                datetime.now() - timedelta(days=self.lookback_days)
            ).strftime("%Y-%m-%d")
            filters = f"{filters}&filters.utcDate.gte={lookback_date}"

        Exposure_query_list = [
            Exposure_metrics,
            Exposure_dimensions,
            filters,
            format,
            human,
        ]
        Event_query_list = [Event_metrics, Event_dimensions, filters, format, human]
        Exposure_response = requests.get(
            url=f"{base_url}?{'&'.join(Exposure_query_list)}", headers=header
        )
        Event_response = requests.get(
            url=f"{base_url}?{'&'.join(Event_query_list)}", headers=header
        )

        if (
            Event_response.status_code == 200
        ):  # Exposure_response.status_code == 200 and
            print("Celtra Exposure report Downloaded")
            if len(Event_response.content) > 0:
                with open(f"{self.storage_path}/celtra_event.csv", "wb") as f:
                    f.write(Event_response.content)
                print("Event report Downloaded")
                with open(f"{self.storage_path}/celtra_exposure.csv", "wb") as f:
                    f.write(Exposure_response.content)
                print("Exposure results Downloaded")

        else:
            print(f"Celtra Exposure report Error: {Exposure_response.text}")
            print(f"Celtra Event report Error: {Event_response.text}")

        # Create a dataframe from Event_response response
        Event_df = pd.read_csv(f"{self.storage_path}/celtra_event.csv")
        Exposure_df = pd.read_csv(f"{self.storage_path}/celtra_exposure.csv")
        return Event_df, Exposure_df
