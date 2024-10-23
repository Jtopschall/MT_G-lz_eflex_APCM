import asyncio
from datetime import timedelta, datetime, date, timezone
from asyncua import Client, Node, ua
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# setup client connecting to Beckhoff PLC
Beckhoff_DLRA_Server_url = 'opc.tcp://192.168.116.129:4840'
Beckhoff_DLRA_Server_client = Client(url=Beckhoff_DLRA_Server_url)


# script to query temperatur of wash and cleaning tank

async def connect_to_beckhoff():
    # Connect to the server
    await Beckhoff_DLRA_Server_client.connect()
    print('Successfully connected')


async def query_temperature(string_of_node: str) -> float:
    node = Beckhoff_DLRA_Server_client.get_node(string_of_node)
    value = await node.read_value()
    return round(value, 1)


async def main():
    await connect_to_beckhoff()
    # set up strings to for temperature nodes
    string_SZ_temperature = "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.DAK.BvL_Tanks.BvL_Tank_SZ.Tankheizung.BvL_T_T.sensorState.fValue"
    string_RZ_temperature = "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.DAK.BvL_Tanks.BvL_Tank_RZ.Tankheizung.BvL_T_T.sensorState.fValue"

    node_SZ_temperature = Beckhoff_DLRA_Server_client.get_node(string_SZ_temperature)
    node_RZ_temperature = Beckhoff_DLRA_Server_client.get_node(string_RZ_temperature)

    hours_to_run = 0
    minutes_to_run = 10
    # sample time in minutes
    sample_time = 1
    times = []
    SZ_tmps = []
    RZ_tmps = []

    # set start_time to next quarter hour
    now = pd.Timestamp.now(tz='Europe/Brussels')
    """
    if now.minute < 15:
        start_time = pd.Timestamp.now(tz='Europe/Brussels').replace(minute=15, second=0, microsecond=0)

    elif now.minute < 30:
        start_time = pd.Timestamp.now(tz='Europe/Brussels').replace(minute=30, second=0, microsecond=0)

    elif now.minute < 45:
        start_time = pd.Timestamp.now(tz='Europe/Brussels').replace(minute=45, second=0, microsecond=0)

    else:
        start_time = pd.Timestamp.now(tz='Europe/Brussels').replace(minute=0, second=0, microsecond=0)
        """
    start_time = now

    times.append(start_time)
    print(f"start_time: {start_time}")

    end_time = start_time + timedelta(hours=hours_to_run, minutes=minutes_to_run)
    print(f"end_time: {end_time}")

    while pd.Timestamp.now(tz='Europe/Brussels') < end_time:
        times.append(pd.Timestamp.now(tz='Europe/Brussels').replace(second=0, microsecond=0))
        SZ_tmp = await query_temperature(string_SZ_temperature)
        RZ_tmp = await query_temperature(string_RZ_temperature)
        print(f"temperature SZ: {SZ_tmp}")
        print(f"temperature RZ: {RZ_tmp}")
        SZ_tmps.append(SZ_tmp)
        RZ_tmps.append(RZ_tmp)
        await asyncio.sleep(60*sample_time)

    print(f"measure times: {times}")
    print(f"temperatures SZ: {SZ_tmps}")
    print(f" temperatures RZ: {RZ_tmps}")

    # calc time frames in minutes
    start_time_in_minutes = start_time.hour * 60 + start_time.minute
    end_time_in_minutes = end_time.hour * 60 + end_time.minute

    # number of data points
    number_values = len(SZ_tmps)

    timeframes_in_minutes = np.linspace(start_time_in_minutes, end_time_in_minutes, number_values)
    timeframes_in_hours = [(int(zeit // 60), int(zeit % 60)) for zeit in timeframes_in_minutes]

    plt.plot(timeframes_in_minutes, SZ_tmps, marker='o')

    # Achsen beschriften
    plt.xlabel('Zeit (Minuten)')
    plt.ylabel('Werte')

    # X-Achse mit Stunden und Minuten beschriften
    plt.xticks(timeframes_in_minutes, [f"{h:02d}:{m:02d}" for h, m in timeframes_in_hours])

    # Titel hinzufügen
    plt.title('Messwerte über die Zeit')

    # Optional: Rasterlinien hinzufügen
    plt.grid(True)

    # Plot anzeigen
    plt.show()


asyncio.run(main())
