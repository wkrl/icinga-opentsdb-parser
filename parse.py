import requests
import pandas as pd
import dask.dataframe as dd
import glob
import json
import argparse


KEYS = ["DATATYPE", "TIMET", "HOSTNAME", "SERVICEDESC", "SERVICEPERFDATA",
       "SERVICECHECKCOMMAND", "HOSTSTATE", "HOSTSTATETYPE",
       "SERVICESTATE", "SERVICESTATETYPE", "SERVICEOUTPUT"]
SERVICE_BLACKLIST = ["tcp_alive_6443", "tcp_alive_6444", "https_ords", "ris_apex"]
ICINGA_EXIT_CODES = {"OK": 0, "WARNING": 1, "CRITICAL": 2, "UNKNOWN": 3}

def parse_data(data, directory):
    for filename in list(glob.glob(directory+"*")):
        with open(filename) as file:
            for line in file:
                if "SERVICEOUTPUT" not in line:
                    data["SERVICEOUTPUT"].append("NO OUTPUT")

                parsed = line.split("\t")
                for item in parsed:
                    key, value = item.split("::")
                    data[key].append(value)
    return data

def drop_columns(df):
    return df[["TIMET", "HOSTNAME", "SERVICEDESC", "SERVICESTATE", "SERVICESTATETYPE", "SERVICEOUTPUT"]]

def create_dataframe(d):
    df_pd = pd.DataFrame(data=d)
    df_dd = dd.from_pandas(df_pd, npartitions=5)
    df_dd = drop_columns(df_dd)
    return df_dd

def cleanup_service_output(service, df):
    if service == "load":
        df["SERVICEOUTPUT"] = df["SERVICEOUTPUT"].replace(".*load average.*", "CPU USAGE", regex=True)

    if "docker" or "kubelet" or "haproxy" or "keepalived" in service:
        df["SERVICEOUTPUT"] = df["SERVICEOUTPUT"].replace("[A-Z]+: service ", "", regex=True)

    if service == "disk_root":
        df["SERVICEOUTPUT"] = df["SERVICEOUTPUT"].replace("DISK [A-Z]+ - free space.*", "DISK USAGE", regex=True)

    if service == "ris_ceph_health":
        df["SERVICEOUTPUT"] = df["SERVICEOUTPUT"].replace("\);.*", "", regex=True)
        df["SERVICEOUTPUT"] = df["SERVICEOUTPUT"].replace(".*\(", "", regex=True)

    if service == "https":
        df["SERVICEOUTPUT"] = df["SERVICEOUTPUT"].replace("HTTP [A-Z]+: ", "", regex=True)
        df["SERVICEOUTPUT"] = df["SERVICEOUTPUT"].replace(" [A-Z][A-Z]+ - .*", "", regex=True)
        df["SERVICEOUTPUT"] = df["SERVICEOUTPUT"].replace(".* Socket timeout .*", "SOCKET TIMEOUT", regex=True)
    return df

def cleanup_df(service, df):
    df["SERVICESTATE"] = df["SERVICESTATE"].replace(ICINGA_EXIT_CODES)
    df["SERVICESTATETYPE"] = df["SERVICESTATETYPE"].str.rstrip()
    df["SERVICEOUTPUT"] = df["SERVICEOUTPUT"].str.rstrip()
    df["SERVICEOUTPUT"] = df["SERVICEOUTPUT"].replace("is ", "", regex=True)
    df["SERVICEOUTPUT"] = df["SERVICEOUTPUT"].replace("Remote command execution failed.*", "REMOTE EXECUTION FAILED", regex=True)
    df["SERVICEOUTPUT"] = df["SERVICEOUTPUT"].replace(".* Plugin timed out .*", "PLUGIN TIMEOUT", regex=True)
    df = cleanup_service_output(service, df)

    df["SERVICEOUTPUT"] = df["SERVICEOUTPUT"].str.upper()
    df = df.drop(columns=["SERVICEDESC"])
    return df

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", type=str, dest="directory", help="file directory of icinga output")
    args = vars(parser.parse_args())

    data_dict = {key: [] for key in KEYS}
    data = parse_data(data_dict, args["directory"])
    df_dd = create_dataframe(data)

    services = [service for service in df_dd["SERVICEDESC"].unique().compute() if service not in SERVICE_BLACKLIST]
    for service in services:
        metric = f"icinga.service.{service}"
        df = df_dd[df_dd["SERVICEDESC"]==service]
        df = cleanup_df(service, df)
        df.compute().to_json(path_or_buf=metric, orient="records", lines=True)
        # df.compute().to_csv(path_or_buf=metric, index=False)