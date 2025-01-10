# -*- coding: utf-8 -*-
"""
Created on Tue Nov 26 10:04:53 2024

@author: Espen.Nordsveen
"""

# from cog_client import client


def handle(client):

    import math
    import os
    import uuid

    from datetime import datetime, time, timedelta, timezone
    from zoneinfo import ZoneInfo

    import pandas as pd
    import requests

    # from cog_client import client
    from cognite.client.data_classes import Event

    # type: ignore
    # from dotenv import load_dotenv
    # load_dotenv()
    # CLIENT_ID = os.getenv("CLIENT_ID")
    # CLIENT_SECRET = os.getenv("CLIENT_SECRET")

    CLIENT_ID = os.getenv("sharepoint_client_id")
    CLIENT_SECRET = os.getenv("sharepoint_client_secret")

    # %%
    class DrainData:
        """
        A class to represent drain rounds in Skive. The class fetches data from Sharepoint lists in the Viridor domain

        ...

        Attributes
        ----------
        site_name : str
            the name of the sharepoint site to fetch data from

        Methods
        -------
        get_access_token(cls):
            Class method. Returns the access_token

        get_site_id(cls, site_name)
            Class method. Returns the site id for the given site_name


        """

        access_token = None

        # OAuth 2.0 token endpoint
        token_url = "https://login.microsoftonline.com/92bce3bb-abfb-484b-b074-32e1a37f3631/oauth2/v2.0/token"

        # Request payload
        payload = {
            "grant_type": "client_credentials",  # Use "authorization_code" if using user-delegated permissions
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "scope": "https://graph.microsoft.com/.default",  # Use ".default" for application permissions
        }

        # Class method to obtain the access token
        @classmethod
        def get_access_token(cls):
            """
            Obtains the access token from sharepoint with the given CLIENT_ID and CLIENT_SECRET for the app registration in Azure,
            set up for sharepoint access to Viridor domain. CLIENT_ID and CLIENT_SECRET is stored in a .env file. Obtains a token
            valid globally inside the class instance

            Returns
            -------
            str
                DESCRIPTION.

            """
            if cls.access_token is None:
                # Make the POST request
                response = requests.post(cls.token_url, data=cls.payload)

                # Handle the response
                if response.status_code == 200:
                    cls.access_token = response.json().get("access_token")

                    # print("Access Token:", access_token)
                else:
                    print(f"Failed to obtain token: {response.status_code}, {response.text}")
            return cls.access_token

        @classmethod
        def get_site_id(cls, site_name):

            access_token = cls.get_access_token()

            hostname = "viridor.sharepoint.com"

            # API endpoint
            url = f"https://graph.microsoft.com/v1.0/sites/{hostname}:/sites/{site_name}"

            # Make the request
            headers = {
                "Authorization": f"Bearer {access_token}",
            }

            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                site_id = response.json().get("id")
                # print("Site ID:", site_id)
                if not site_id:
                    raise Exception("Site ID not found in the response.")
                return site_id
            else:
                raise Exception(f"Failed to retrieve site ID: {response.status_code}, {response.text}")

        def __init__(self, site_name):
            """
            Construct all the necessary attributes for the DrainData object

            Parameters
            ----------
            site_name : str
                name of the sharepoint site to fetch data from

            Returns
            -------
            None.

            """
            self.access_token = DrainData.get_access_token()
            self.site_id = DrainData.get_site_id(site_name)
            self.site_name = site_name
            print(f"Initialized DrainData with Site name {site_name} and Site ID: {self.site_id}")

        def get_lists_id(self):
            url = f"http://graph.microsoft.com/v1.0/sites/{self.site_id}/lists"
            headers = {"Authorization": f"Bearer {self.access_token}"}
            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                lists = response.json()
                print("Lists fetched successfully")
                # print(json.dumps(lists, indent=4))
            else:
                raise Exception(f"Error: {response.status_code}, {response.text}")

            return lists

        def get_list_data(self, lists, list_name):
            """


            Parameters
            ----------
            lists : DICT
                Structured dict containing all lists for the given site name.
            list_name : STRING
                The name of the list to fetch data from.

            Returns
            -------
            LIST
                Returns list of data from the given list name

            """
            headers = {"Authorization": f"Bearer {self.access_token}"}

            list_dict = {item.get("name"): item.get("id") for item in lists.get("value")}
            list_id = list_dict.get(list_name)
            # print(list_id)
            # site_id = "your-sharepoint-site-id"
            url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/lists/{list_id}/items?expand=fields"
            response_list = requests.get(url, headers=headers)
            # print(response_list)
            if response_list.status_code == 200:
                response_json = response_list.json()
                # print(json.dumps(response_json, indent=4))
            else:
                print(f"Failed to fetch lists: {response_list.status_code}, {response_list.text}")

            return response_json.get("value")

        def create_12h_drain_df(self, list_12hours):
            """


            Parameters
            ----------
            list_12hours : list
                List data for 12 hours drain, fetched using get_list_data()

            Returns
            -------
            drain_df : DataFrame
                Returns a dataframe of all drain volumes from the 12 hours drain round with valve tag number as the column header name.

            """

            data = []
            for entry in list_12hours:
                data.append(
                    {
                        "Date": entry.get("fields").get("field_1"),
                        "P03_QJB_QM005": entry.get("fields").get("field_2"),
                        "P04_QJB_QM006": entry.get("fields").get("field_3"),
                        "P02_QJB_QM006": entry.get("fields").get("field_4"),
                        "P01_QJB_QM005": entry.get("fields").get("field_5"),
                        "P10_QJB_QM149": entry.get("fields").get("field_6"),
                        "P10_QEB_QM033": entry.get("fields").get("field_7"),
                    }
                )

            drain_df = pd.DataFrame(data)
            drain_df["Date"] = pd.to_datetime(drain_df["Date"])
            drain_df.set_index("Date", inplace=True)
            drain_df.fillna(0, inplace=True)
            # drain_df["Sum"] = drain_df.sum(axis=1)
            return drain_df

        def create_24h_drain_df(self, list_24hours, list_name):
            """


            Parameters
            ----------
            list_24hours : list
                List data for 24 hours drain, fetched using get_list_data()
            list_name : str
                DESCRIPTION.

            Returns
            -------
            drain_df : TYPE
                DESCRIPTION.

            """
            # list_dict = {item.get("name"): item.get("id") for item in lists.get("value")}
            # list_id = list_dict.get(list_name)

            # headers = {"Authorization": f"Bearer {self.access_token}"}

            # url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/lists/{list_id}/items?expand=fields"
            # response_list = requests.get(url, headers=headers)

            # if response_list.status_code == 200:
            #     response_json = response_list.json()
            #     # print(json.dumps(response_json, indent=4))
            # else:
            #     print(f"Failed to fetch lists: {response_list.status_code}, {response_list.text}")

            data = []
            for entry in list_24hours:
                data.append(
                    {
                        "Date": entry.get("fields").get("field_1"),
                        "P10_QJB_QM150": entry.get("fields").get("field_2"),
                        "P10_EKG_QM039": entry.get("fields").get("field_3"),
                        "P10_QJB_QM148": entry.get("fields").get("field_4"),
                        "P10_EGG_QM139": entry.get("fields").get("field_5"),
                        "P10_EKG_QM031": entry.get("fields").get("field_6"),
                        "P10_GAD_QM007": entry.get("fields").get("field_7"),
                        "P10_GNK_QM729": entry.get("fields").get("field_8"),
                        # "P10_EKG_GP001": entry.get("fields").get("field_7"),
                    }
                )

            drain_df = pd.DataFrame(data)
            drain_df["Date"] = pd.to_datetime(drain_df["Date"])
            drain_df.set_index("Date", inplace=True)
            drain_df.fillna(0, inplace=True)
            # drain_df["Sum"] = drain_df.sum(axis=1)
            return drain_df

        def single_drain_dp_12h_total(self, drain_list, year, month, day):
            """


            Parameters
            ----------
            drain_list : LIST
                List of drain data for 12h days drain round.
            year : datetime.year
                Input year of interest.
            month : datetime.month
                Input month of interest.
            day : datetime.day
                Input day of interest.

            Returns
            -------
            Returns the total sum of drain volumes for the given date

            """

            drain_sum = 0
            for item in drain_list:
                date = datetime.strptime(item.get("fields").get("field_1"), "%Y-%m-%dT%H:%M:%SZ")
                # print(datetime.strptime(item.get("fields").get("field_1"), "%Y-%m-%dT%H:%M:%SZ"))
                # print(date.day, date.month, date.year, ":", day, month, year)
                if date.day == day and date.month == month and date.year == year:
                    print(item.get("eTag"))
                    drain_sum += (
                        item.get("fields").get("field_2")
                        + item.get("fields").get("field_3")
                        + item.get("fields").get("field_4")
                        + item.get("fields").get("field_5")
                        + item.get("fields").get("field_6")
                        + item.get("fields").get("field_7")
                    )

            return drain_sum

        def single_drain_dp_24h_total(self, drain_list, year, month, day):
            """


            Parameters
            ----------
            drain_list : LIST
                List of drain data for 24 hours drain round.
            year : datetime.year
                Input year of interest.
            month : datetime.month
                Input month of interest.
            day : datetime.day
                Input day of interest.

            Returns
            -------
            Returns the total sum of drain volumes for the given date

            """

            drain_sum = 0
            for item in drain_list:
                date = datetime.strptime(item.get("fields").get("field_1"), "%Y-%m-%dT%H:%M:%SZ")
                # print(datetime.strptime(item.get("fields").get("field_1"), "%Y-%m-%dT%H:%M:%SZ"))
                if date.day == day and date.month == month and date.year == year:
                    print(item.get("eTag"))
                    drain_sum += (
                        item.get("fields").get("field_2")
                        + item.get("fields").get("field_3")
                        + item.get("fields").get("field_4")
                        + item.get("fields").get("field_5")
                        + item.get("fields").get("field_6")
                        + item.get("fields").get("field_7")
                        + item.get("fields").get("field_8")
                    )

            return drain_sum

        def single_drain_dp_7d_total(self, drain_list, year, month, day):
            """


            Parameters
            ----------
            drain_list : LIST
                List of drain data for 7 days drain round.
            year : datetime.year
                Input year of interest.
            month : datetime.month
                Input month of interest.
            day : datetime.day
                Input day of interest.

            Returns
            -------
            Returns the total sum of drain volumes for the given date

            """
            drain_sum = 0
            for item in drain_list:
                date = datetime.strptime(item.get("fields").get("field_1"), "%Y-%m-%dT%H:%M:%SZ")
                # print(datetime.strptime(item.get("fields").get("field_1"), "%Y-%m-%dT%H:%M:%SZ"))
                if date.day == day and date.month == month and date.year == year:
                    drain_sum += (
                        item.get("fields").get("field_2")
                        + item.get("fields").get("field_3")
                        + item.get("fields").get("field_4")
                        + item.get("fields").get("field_5")
                        + item.get("fields").get("field_6")
                        + item.get("fields").get("field_7")
                        + item.get("fields").get("field_8")
                        + item.get("fields").get("field_9")
                        + item.get("fields").get("field_10")
                        + item.get("fields").get("field_11")
                        + item.get("fields").get("field_12")
                        + item.get("fields").get("field_13")
                        + item.get("fields").get("field_14")
                        + item.get("fields").get("field_15")
                        + item.get("fields").get("field_16")
                        + item.get("fields").get("field_17")
                        + item.get("fields").get("field_18")
                        + item.get("fields").get("field_19")
                        + item.get("fields").get("field_20")
                        + item.get("fields").get("field_21")
                        + item.get("fields").get("field_22")
                        + item.get("fields").get("field_23")
                        + item.get("fields").get("field_24")
                        + item.get("fields").get("field_25")
                        + item.get("fields").get("field_26")
                        + item.get("fields").get("field_27")
                        + item.get("fields").get("field_28")
                        + item.get("fields").get("field_29")
                        + item.get("fields").get("field_30")
                        + item.get("fields").get("field_31")
                        + item.get("fields").get("field_32")
                    )

            return drain_sum

    data = {
        "prodhours1": ["2s=L1_PRODUCTION_HOURS:Q_TT"],
        "prodhours2": ["2s=L2_PRODUCTION_HOURS:Q_TT"],
        "prodhours3": ["2s=L3_PRODUCTION_HOURS:Q_TT"],
        "prodhours4": ["2s=L4_PRODUCTION_HOURS:Q_TT"],
        "plastfeed1": ["2s=P01_EAC_BW002_DFLC01:Q_TT"],
        "plastfeed2": ["2s=P02_EAC_BW002_DFLC01:Q_TT"],
        "plastfeed3": ["2s=P03_EAC_BW002_DFLC01:Q_TT"],
        "plastfeed4": ["2s=P04_EAC_BW002_DFLC01:Q_TT"],
        "CM101": ["2s=P01_EGG_BF103_DFLC01:Q_TT"],
        "CM201": ["2s=P02_EGG_BF203_DFLC01:Q_TT"],
        "CM301": ["2s=P03_EGG_BF303_DFLC01:Q_TT"],
        "CM401": ["2s=P04_EGG_BF403_DFLC01:Q_TT"],
        "CM101_wax": ["2s=P01_EGG_BF103_DFLC02:Q_TT"],
        "CM201_wax": ["2s=P02_EGG_BF203_DFLC02:Q_TT"],
        "CM301_wax": ["2s=P03_EGG_BF303_DFLC02:Q_TT"],
        "CM401_wax": ["2s=P04_EGG_BF403_DFLC02:Q_TT"],
        "CM922_line1": ["2s=P01_EGG_BF104_DFLC01:Q_TT"],
        "CM922_line2": ["2s=P02_EGG_BF204_DFLC01:Q_TT"],
        "CM972_line3": ["2s=P03_EGG_BF304_DFLC01:Q_TT"],
        "CM972_line4": ["2s=P04_EGG_BF404_DFLC01:Q_TT"],
        "flashtank1": ["2s=P01_EGG_BF102_DFLC01:Q_TT"],
        "flashtank2": ["2s=P02_EGG_BF202_DFLC01:Q_TT"],
        "flashtank3": ["2s=P03_EGG_BF302_DFLC01:Q_TT"],
        "flashtank4": ["2s=P04_EGG_BF402_DFLC01:Q_TT"],
        "separator1_water": ["2s=P11_GNK_BF925_DFLC01:Q_TT"],
        "separator2_water": ["2s=P12_GNK_BF975_DFLC01:Q_TT"],
        "separator1_oil": ["2s=P11_EGG_BF924_DFLC01:Q_TT"],
        "separator2_oil": ["2s=P12_EGG_BF974_DFLC01:Q_TT"],
        "wax_ht_mod1": ["2s=P11_EGG_BF925_DFLC01:Q_TT"],
        "wax_ht_mod2": ["2s=P12_EGG_BF975_DFLC01:Q_TT"],
        "NCGline1": ["2s=L1_BURNER_NCG_FLOW:Q_TT"],
        "NCGline2": ["2s=L2_BURNER_NCG_FLOW:Q_TT"],
        "NCGline3": ["2s=L3_BURNER_NCG_FLOW:Q_TT"],
        "NCGline4": ["2s=L4_BURNER_NCG_FLOW:Q_TT"],
        "NGline1": ["2s=L1_BURNER_NG_FLOW:Q_TT"],
        "NGline2": ["2s=L2_BURNER_NG_FLOW:Q_TT"],
        "NGline3": ["2s=L3_BURNER_NG_FLOW:Q_TT"],
        "NGline4": ["2s=L4_BURNER_NG_FLOW:Q_TT"],
        "NCGtoflare": ["2s=P10_EKG_BF001_DFLC01:Q_TT"],
        "NGtoflare": ["2s=P10_QJD_BF601_DFLC01:Q_TT"],
        "NCGdrain": ["2s=P10_EKG_BF002_DFLC01:Q_TT"],
        "plastic_bales_weight": ["plasticbale_weight"],
        "sulzer_feed": ["2s=P10_SUZ_BF001_DFLC01:Q_TT"],
        "sulzer_ep04": ["2s=P10_SUZ_BF003_DFLC01:Q_TT"],
        "sulzer_ep08": ["2s=P10_SUZ_BF016_DFLC01:Q_TT"],
        "sulzer_ep05": ["2s=P10_SUZ_BF004_DFLC01:Q_TT"],
        "sulzer_ep06": ["2s=P10_SUZ_BF005_DFLC01:Q_TT"],
    }

    def lastDayCounterValue(extId, yesterday_end):

        dps_first = (
            client.time_series.data.retrieve_latest(external_id=extId, before=yesterday_end - timedelta(hours=24))
            .to_pandas()
            .iloc[0, 0]
        )
        dps_last = (
            client.time_series.data.retrieve_latest(external_id=extId, before=yesterday_end).to_pandas().iloc[0, 0]
        )

        return dps_last - dps_first

    def accumulatedWeeklyNumbers(extId, yesterday_end):

        first_day_week = yesterday_end - datetime.timedelta(days=yesterday_end.weekday())

        dps_first = (
            client.time_series.data.retrieve_latest(
                external_id=extId, before=first_day_week - datetime.timedelta(hours=24)
            )
            .to_pandas()
            .iloc[0, 0]
        )
        dps_last = (
            client.time_series.data.retrieve_latest(external_id=extId, before=yesterday_end).to_pandas().iloc[0, 0]
        )

        return dps_last - dps_first

    def backfillDatapoints(extId, year, month, day):
        d = datetime.datetime(year, month, day)
        dt = d
        starttime = dt.replace(hour=23, minute=59, second=59)
        dk_tz = ZoneInfo("UTC")
        time_now_DKlocal = starttime.replace(tzinfo=dk_tz)

        dps_first = (
            client.time_series.data.retrieve_latest(
                external_id=extId, before=time_now_DKlocal - datetime.timedelta(hours=24)
            )
            .to_pandas()
            .iloc[0, 0]
        )
        dps_last = (
            client.time_series.data.retrieve_latest(external_id=extId, before=time_now_DKlocal).to_pandas().iloc[0, 0]
        )

        return dps_last - dps_first

    today = datetime.now(timezone.utc).date()
    yesterday = today - timedelta(days=1)
    t = time(hour=23, minute=59, second=59)
    lastday_end = datetime.combine(today, t) - timedelta(hours=24)
    lastday_end_tz = ZoneInfo("Europe/Oslo")
    yesterday_end = lastday_end.replace(tzinfo=lastday_end_tz)

    week = yesterday.isocalendar()[1]

    dk_tz = ZoneInfo("Europe/Oslo")

    # %% Manual drain
    drains = DrainData("S-Skive470")

    lists = drains.get_lists_id()
    list_12h = drains.get_list_data(lists, "DrainLog_12hr")
    list_24h = drains.get_list_data(lists, "DrainLog_24hr")
    list_7d = drains.get_list_data(lists, "DrainLog_7d")
    total_drain_12h = drains.single_drain_dp_12h_total(
        list_12h, yesterday_end.year, yesterday_end.month, yesterday_end.day
    )
    total_drain_24h = drains.single_drain_dp_24h_total(
        list_24h, yesterday_end.year, yesterday_end.month, yesterday_end.day
    )
    total_drain_7d = drains.single_drain_dp_7d_total(
        list_7d, yesterday_end.year, yesterday_end.month, yesterday_end.day
    )

    manual_drain = total_drain_12h + total_drain_24h + total_drain_7d

    # %% Plastic feed
    line1_feed = lastDayCounterValue(data["plastfeed1"], yesterday_end)
    line2_feed = lastDayCounterValue(data["plastfeed2"], yesterday_end)
    line3_feed = lastDayCounterValue(data["plastfeed3"], yesterday_end)
    line4_feed = lastDayCounterValue(data["plastfeed4"], yesterday_end)

    # %% Plastic feed to shredder
    today_nonutc = datetime.now(timezone.utc).date()
    t = time(hour=23, minute=59, second=59)
    lastday_end = datetime.combine(today_nonutc, t) - timedelta(hours=24)
    dk_tz = ZoneInfo("UTC")
    yesterday_end_utc = lastday_end.replace(tzinfo=dk_tz)

    # Daily
    df_bale_test = client.time_series.data.retrieve_dataframe(
        external_id=data["plastic_bales_weight"],
        start=yesterday_end_utc - timedelta(hours=24),
        end=yesterday_end_utc,
    )

    plastic_to_shredder_daily = df_bale_test["plasticbale_weight"].sum()

    # %% Operating hours
    line1 = lastDayCounterValue(data["prodhours1"], yesterday_end)
    line2 = lastDayCounterValue(data["prodhours2"], yesterday_end)
    line3 = lastDayCounterValue(data["prodhours3"], yesterday_end)
    line4 = lastDayCounterValue(data["prodhours4"], yesterday_end)

    # %% NCG to burner
    ncg_line1 = lastDayCounterValue(data["NCGline1"], yesterday_end)
    ncg_line2 = lastDayCounterValue(data["NCGline2"], yesterday_end)
    ncg_line3 = lastDayCounterValue(data["NCGline3"], yesterday_end)
    ncg_line4 = lastDayCounterValue(data["NCGline4"], yesterday_end)
    ncg_flare = lastDayCounterValue(data["NCGtoflare"], yesterday_end)
    ncg_drain = lastDayCounterValue(data["NCGdrain"], yesterday_end)

    # %% NG to burner
    ng_line1 = lastDayCounterValue(data["NGline1"], yesterday_end)
    ng_line2 = lastDayCounterValue(data["NGline2"], yesterday_end)
    ng_line3 = lastDayCounterValue(data["NGline3"], yesterday_end)
    ng_line4 = lastDayCounterValue(data["NGline4"], yesterday_end)
    ng_flare = lastDayCounterValue(data["NGtoflare"], yesterday_end)

    # %% CMx01 to holding tank
    cm101_dp = lastDayCounterValue(data["CM101"], yesterday_end)
    cm201_dp = lastDayCounterValue(data["CM201"], yesterday_end)
    cm301_dp = lastDayCounterValue(data["CM301"], yesterday_end)
    cm401_dp = lastDayCounterValue(data["CM401"], yesterday_end)

    # %% Oil from waxtank
    cm922_dp = lastDayCounterValue(data["wax_ht_mod1"], yesterday_end)
    cm972_dp = lastDayCounterValue(data["wax_ht_mod2"], yesterday_end)

    # %% CMx01 to wax tank
    cm101_wax = lastDayCounterValue(data["CM101_wax"], yesterday_end)
    cm201_wax = lastDayCounterValue(data["CM201_wax"], yesterday_end)
    cm301_wax = lastDayCounterValue(data["CM301_wax"], yesterday_end)
    cm401_wax = lastDayCounterValue(data["CM401_wax"], yesterday_end)

    # %% Wax reinjection
    cm922_line1 = lastDayCounterValue(data["CM922_line1"], yesterday_end)
    cm922_line2 = lastDayCounterValue(data["CM922_line2"], yesterday_end)
    cm972_line3 = lastDayCounterValue(data["CM972_line3"], yesterday_end)
    cm972_line4 = lastDayCounterValue(data["CM972_line4"], yesterday_end)

    # %% Flash tank
    flashtank1_dp = lastDayCounterValue(data["flashtank1"], yesterday_end)
    flashtank2_dp = lastDayCounterValue(data["flashtank2"], yesterday_end)
    flashtank3_dp = lastDayCounterValue(data["flashtank3"], yesterday_end)
    flashtank4_dp = lastDayCounterValue(data["flashtank4"], yesterday_end)

    # %% Separators
    sepoil1_dp = lastDayCounterValue(data["separator1_oil"], yesterday_end)
    sepoil2_dp = lastDayCounterValue(data["separator2_oil"], yesterday_end)
    sepwater1_dp = lastDayCounterValue(data["separator1_water"], yesterday_end)
    sepwater2_dp = lastDayCounterValue(data["separator2_water"], yesterday_end)

    # %% Sulzer
    sulzer_feed_column = lastDayCounterValue(data["sulzer_feed"], yesterday_end)
    sulzer_ep04_column = lastDayCounterValue(data["sulzer_ep04"], yesterday_end)
    sulzer_ep08_column = lastDayCounterValue(data["sulzer_ep08"], yesterday_end)
    sulzer_ep05_column = lastDayCounterValue(data["sulzer_ep05"], yesterday_end)
    sulzer_ep06_column = lastDayCounterValue(data["sulzer_ep06"], yesterday_end)

    gas_to_flare_calc = (
        sulzer_feed_column - sulzer_ep04_column - sulzer_ep05_column - sulzer_ep06_column - sulzer_ep08_column
    )

    # %% Oil to holding tank total
    total_oil_HT = (
        flashtank1_dp
        + flashtank2_dp
        + flashtank3_dp
        + flashtank4_dp
        + cm101_dp
        + cm201_dp
        + cm301_dp
        + cm401_dp
        + sepoil1_dp
        + sepoil2_dp
        + cm922_dp
        + cm972_dp
        + ncg_drain
    )

    # %% Ratio calculations
    manual_drain_ratio = manual_drain * 0.8 / (plastic_to_shredder_daily - sepwater1_dp - sepwater2_dp)
    ncg_drain_ratio = ncg_drain / (plastic_to_shredder_daily - sepwater1_dp - sepwater2_dp)
    ncg_total_ratio = (ncg_flare + ncg_line1 + ncg_line2 + ncg_line3 + ncg_line4) / (
        plastic_to_shredder_daily - sepwater1_dp - sepwater2_dp
    )
    oil_to_ht_ratio = total_oil_HT / (plastic_to_shredder_daily - sepwater1_dp - sepwater2_dp)
    ash_ratio = 1 - manual_drain_ratio - ncg_drain_ratio - ncg_total_ratio - oil_to_ht_ratio

    # %% Ash calculation
    ash = (plastic_to_shredder_daily - sepwater1_dp - sepwater2_dp) * ash_ratio
    # %% One event for all
    data_event = Event(
        external_id="skive_daily_production_numbers" + str(uuid.uuid4()),
        data_set_id=6509329789873439,
        start_time=int(yesterday_end.timestamp() * 1000),
        type="Process",
        subtype="raw",
        description="Calculation of last days production data, based on counter data",
        metadata=(
            {
                "Day": lastday_end.weekday(),
                "Week": week,
                "Operating_hours_line1": math.floor(line1 * 10) / 10,
                "Plastic_infeed_line1": math.floor(line1_feed),
                "NG_to_burner_line1": math.floor(ng_line1),
                "NCG_to_burner_line1": math.floor(ncg_line1),
                "Oil_from_flash_tank_line1": math.floor(flashtank1_dp),
                "Oil_from_CMx01_to_holding_tank_line1": math.floor(cm101_dp),
                "Oil_from_CMx01_to_CM9x2_line1": math.floor(cm101_wax),
                "Oil_reinjected_to_reactor_line1": math.floor(cm922_line1),
                "Operating_hours_line2": math.floor(line2 * 10) / 10,
                "Plastic_infeed_line2": math.floor(line2_feed),
                "NG_to_burner_line2": math.floor(ng_line2),
                "NCG_to_burner_line2": math.floor(ncg_line2),
                "Oil_from_flash_tank_line2": math.floor(flashtank2_dp),
                "Oil_from_CMx01_to_holding_tank_line2": math.floor(cm201_dp),
                "Oil_from_CMx01_to_CM9x2_line2": math.floor(cm201_wax),
                "Oil_reinjected_to_reactor_line2": math.floor(cm922_line2),
                "Operating_hours_line3": math.floor(line3 * 10) / 10,
                "Plastic_infeed_line3": math.floor(line3_feed),
                "NG_to_burner_line3": math.floor(ng_line3),
                "NCG_to_burner_line3": math.floor(ncg_line3),
                "Oil_from_flash_tank_line3": math.floor(flashtank3_dp),
                "Oil_from_CMx01_to_holding_tank_line3": math.floor(cm301_dp),
                "Oil_from_CMx01_to_CM9x2_line3": math.floor(cm301_wax),
                "Oil_reinjected_to_reactor_line3": math.floor(cm972_line3),
                "Operating_hours_line4": math.floor(line4 * 10) / 10,
                "Plastic_infeed_line4": math.floor(line4_feed),
                "NG_to_burner_line4": math.floor(ng_line4),
                "NCG_to_burner_line4": math.floor(ncg_line4),
                "Oil_from_flash_tank_line4": math.floor(flashtank4_dp),
                "Oil_from_CMx01_to_holding_tank_line4": math.floor(cm401_dp),
                "Oil_from_CMx01_to_CM9x2_line4": math.floor(cm401_wax),
                "Oil_reinjected_to_reactor_line4": math.floor(cm972_line4),
                "Oil_from_separator_1": math.floor(sepoil1_dp),
                "Water_from_separator_1": math.floor(sepwater1_dp),
                "Oil_from_separator_2": math.floor(sepoil2_dp),
                "Water_from_separator_2": math.floor(sepwater2_dp),
                "Oil_from_waxtank_module1": math.floor(cm922_dp),
                "Oil_from_waxtank_module2": math.floor(cm972_dp),
                "Manual_drain": manual_drain,
                "Plastic_feed_to_shredder": math.floor(plastic_to_shredder_daily),
                "Drain_NCG_buffer_tank": math.floor(ncg_drain),
                "NG_to_flare": math.floor(ng_flare),
                "NCG_to_flare": math.floor(ncg_flare),
                "NCG_total": math.floor(ncg_flare + ncg_line1 + ncg_line2 + ncg_line3 + ncg_line4),
                "Ash": math.floor(ash),
                "Oil_to_holding_tank_total": math.floor(total_oil_HT),
                "Feed_to_column": math.floor(sulzer_feed_column),
                "Gas_to_flare": math.floor(gas_to_flare_calc),
                "Light_fraction_from_EP04": math.floor(sulzer_ep04_column),
                "Light_fraction_from_EP08": math.floor(sulzer_ep08_column),
                "Middle_fraction_EP05": math.floor(sulzer_ep05_column),
                "Heavy_fraction_EP06": math.floor(sulzer_ep06_column),
            }
        ),
    )
    client.events.create(data_event)


# function_xid = "Skive_weekly_report_KPIs"
# function_name = "Skive weekly report KPIs"

# try:
#     client.functions.delete(external_id = function_xid)
#     print("Function deleted")
# except Exception:
#     print("No function to delete")

# func = client.functions.create(
#     external_id = function_xid,
#     name = function_name,
#     function_handle = handle)

# print("Function created")
