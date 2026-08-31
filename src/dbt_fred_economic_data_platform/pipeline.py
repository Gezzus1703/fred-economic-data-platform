import os
import subprocess
import time
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path

import boto3
import pandas as pd
import requests
from dotenv import load_dotenv

from dbt_fred_economic_data_platform.config import (
    FRED_API_URL,
    FRED_REQUEST_LIMIT,
    SERIES_IDS,
)


def download_series(series_id, api_key):
    all_data = []
    offset = 0

    while True:
        params = {
            "series_id": series_id,
            "api_key": api_key,
            "file_type": "json",
            "limit": FRED_REQUEST_LIMIT,
            "offset": offset,
        }

        response = requests.get(
            FRED_API_URL,
            params=params,
            timeout=30,
        )
        response.raise_for_status()

        response_data = response.json()
        observations = response_data["observations"]

        for observation in observations:
            observation["series_id"] = series_id

        all_data.extend(observations)
        offset += len(observations)

        if offset >= response_data["count"]:
            break

    return all_data


def upload_to_s3(df, bucket_name, run_time):
    parquet_buffer = BytesIO()

    df.to_parquet(
        parquet_buffer,
        engine="pyarrow",
        compression="snappy",
        index=False,
    )

    parquet_buffer.seek(0)

    file_name = (
        f"fred_observations_{run_time:%Y%m%dT%H%M%SZ}.parquet"
    )

    s3_key = (
        f"raw/fred/observations/"
        f"ingestion_date={run_time:%Y-%m-%d}/"
        f"{file_name}"
    )

    s3_client = boto3.client("s3")

    s3_client.upload_fileobj(
        parquet_buffer,
        bucket_name,
        s3_key,
    )

    return s3_key


def main():
    load_dotenv()

    api_key = os.getenv("FRED_API_KEY")
    bucket_name = os.getenv("S3_BUCKET_NAME")

    if not api_key:
        raise ValueError("FRED_API_KEY was not found in .env")

    if not bucket_name:
        raise ValueError("S3_BUCKET_NAME was not found in .env")

    all_data = []

    for series_id in SERIES_IDS:
        series_data = download_series(
            series_id,
            api_key,
        )

        all_data.extend(series_data)

        print(
            f"Downloaded {len(series_data)} rows "
            f"for {series_id}."
        )

    run_time = datetime.now(UTC)

    df = pd.DataFrame(all_data)

    df.rename(
        columns={"date": "observation_date"},
        inplace=True,
    )

    df["ingested_at"] = run_time

    print(f"Total rows: {len(df)}")

    s3_key = upload_to_s3(
        df,
        bucket_name,
        run_time,
    )

    print(
        f"Uploaded to "
        f"s3://{bucket_name}/{s3_key}"
    )

    print("Waiting 30 seconds for Snowpipe...")
    time.sleep(30)

    repository_root = (
        Path(__file__).resolve().parents[2]
    )

    dbt_project_dir = (
        repository_root / "fred_dbt"
    )

    subprocess.run(
        [
            "uv",
            "run",
            "dbt",
            "build",
            "--project-dir",
            str(dbt_project_dir),
            "--select",
            "+fact_economic_observations",
        ],
        cwd=repository_root,
        check=True,
    )

    print("Pipeline completed successfully.")


if __name__ == "__main__":
    main()
