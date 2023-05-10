from flask_app import application
from utils.aws import (
    get_secret,
    download_from_s3_return_df,
    upload_str_to_s3,
    send_message,
)
from utils.stats_api import get_odata
import json
import time
import pandas as pd

END_POINT = "Covid-19Indicators"
STATS_API_KEY = json.loads(get_secret("stats_api_key"))["Primary_key"]


def stats_api_refresh():
    """
    pulls all stats data via Stats API and uploads to S3
    in the future, might use feather format to save space and time
    """
    try:
        meta_df = download_from_s3_return_df("meta.csv", "nz-stats/stats-api/")
        count = 0
        df = pd.DataFrame()
        for resource_id in meta_df["ResourceID"].unique():
            application.logger.info("Loading " + resource_id)
            temp_df = get_odata(
                END_POINT, f"$filter=(ResourceID eq '{resource_id}')", STATS_API_KEY
            )
            if len(temp_df) > 0:
                temp_df["Period"] = pd.to_datetime(temp_df["Period"], errors="coerce")
                # print(resource_id, len(temp_df), temp_df['Period'].min(), '-', temp_df['Period'].max())
                count += len(temp_df)
                if resource_id in ["CPTRD2", "CPTRD4", "CPTRD1", "CPTRD5"]:
                    for label in temp_df["Label1"].unique():
                        tmp = temp_df[temp_df["Label1"] == label].copy()
                        tmp.sort_values("Period", inplace=True)
                        tmp["Year"] = tmp["Period"].apply(lambda x: x.year)
                        temp_ser = tmp.groupby("Year")["Value"].shift(1)
                        temp_ser.fillna(0, inplace=True)
                        tmp["Value"] = tmp["Value"] - temp_ser
                        tmp.drop("Year", axis=1, inplace=True)
                        df = pd.concat([df, tmp])
                else:
                    df = pd.concat([df, temp_df])
            time.sleep(3)
        application.logger.info(f"{count} data downloaded via Stats API")
        upload_str_to_s3(df.to_csv(index=False), "nz-stats/stats-api/api-data.csv")
        application.logger.info("New api-data.csv file has been uploaded to S3")
        # send_message("INFO", 'Cache file successfully refreshed.\n\nstats_api_service')
    except Exception as e:
        application.logger.error("stats_api_refresh error:\n" + str(e))
        send_message("ERROR", str(e) + "\n\nstats_api_service")
