import asyncio
import logging
from asyncua import Client, Node, ua, Server, uamethod
from datetime import timedelta, datetime, date, timezone
from termcolor import colored
import xml.etree.ElementTree as ET
from entsoe import EntsoeRawClient
import pandas as pd
import math
import threading
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
import json
import sched
import time
import openpyxl
import uuid
import heapq
import re

# logging.basicConfig(level=logging.INFO)


# create global server object and specify nodeset files the server is based on, change proxy_server_url to run on other machines:
server = Server()
server_url = 'opc.tcp://127.0.0.1:5840'

# setup client connecting to central server
Central_Server_url = 'opc.tcp://127.0.0.1:4840'
Central_Server_client = Client(url=Central_Server_url)

# setup client connecting to Siemens PLC
Siemens_DLRA_Server_url = 'opc.tcp://192.168.116.140:4840'
Siemens_DLRA_Server_client = Client(url=Siemens_DLRA_Server_url)

# setup client connecting to Beckhoff PLC
Beckhoff_DLRA_Server_url = 'opc.tcp://192.168.116.129:4840'
Beckhoff_DLRA_Server_client = Client(url=Beckhoff_DLRA_Server_url)

# timeframes for client
tf_price_dict = {'tf1_price': 6012, 'tf2_price': 6021, 'tf3_price': 6027, 'tf4_price': 6033, 'tf5_price': 6039,
                 'tf6_price': 6045, 'tf7_price': 6051, 'tf8_price': 6057, 'tf9_price': 6063, 'tf10_price': 6069,
                 'tf11_price': 6075, 'tf12_price': 6081, 'tf13_price': 6087, 'tf14_price': 6093, 'tf15_price': 6099,
                 'tf16_price': 6105, 'tf17_price': 6111, 'tf18_price': 6117, 'tf19_price': 6123, 'tf20_price': 6129,
                 'tf21_price': 6135, 'tf22_price': 6141, 'tf23_price': 6147, 'tf24_price': 6153}

# paths to all nodesets
DI_path = 'C:/Users/j.goelz/poser-eflex-apcm/Implementierung/EnFlexServerDLRA/Opc.Ua.Di.NodeSet2.xml'
IA_path = 'C:/Users/j.goelz/poser-eflex-apcm/Implementierung/EnFlexServerDLRA/Opc.Ua.IA.NodeSet2.xml'
Machinery_path = 'C:/Users/j.goelz/poser-eflex-apcm/Implementierung/EnFlexServerDLRA/Opc.Ua.Machinery.NodeSet2.xml'
MT_path = 'C:/Users/j.goelz/poser-eflex-apcm/Implementierung/EnFlexServerDLRA/Opc.Ua.MachineTool.NodeSet2.xml'
CO2_path = 'C:/Users/j.goelz/poser-eflex-apcm/Implementierung/EnFlexServerDLRA/adp_mt_co2.xml'
EnFlex_path = 'C:/Users/j.goelz/poser-eflex-apcm/Implementierung/EnFlexServerDLRA/opc_ua_for_enflex.xml'
DLRAServer_path = 'C:/Users/j.goelz/poser-eflex-apcm/Implementierung/EnFlexServerDLRA/dlra_server.xml'

# how frequent CO2Emissions, Electricity should be calculated
calc_interval = 10

# Import mapping table with pandas:
MappingtablePath = 'C:/Users/j.goelz/poser-eflex-apcm/Implementierung/EnFlexServerDLRA/EnFlexMappingtable_Beckhoff.xlsx'
Mappingtable = pd.read_excel(MappingtablePath)

################################################################flexLoad config start

# flexloadID dict
flexloadID_dict = {
    "Heizstab_ST_12": "d162e83e-abbd-11ee-bb33-6003088b0288",
    "Heizstab_ST_34": "dc5d3190-abbd-11ee-b8be-6003088b0288",
    "Heizstab_ST_5": "e8afa00e-abbd-11ee-90fc-6003088b0288",
    "Heizstab_WT_12": "f3d5cea4-abbd-11ee-b295-6003088b0288",
    "Heizstab_WT_34": "07d6e686-abbe-11ee-b37d-6003088b0288",
    "Heizstab_WT_5": "0eac2192-abbe-11ee-b3f4-6003088b0288",
    "IR_Strahler_14": "169b0fa8-abbe-11ee-9706-6003088b0288",
    "IR_Strahler_23": "296844b6-abbe-11ee-afac-6003088b0288"
}

# flexload object nodes in AllFlexibleLoads folder
FlexLoads_Objects_dict = {"Heizstab_ST_12": 5022, "Heizstab_ST_34": 5023, "Heizstab_ST_5": 5024, "Heizstab_WT_12": 5004,
                          "Heizstab_WT_34": 5020, "Heizstab_WT_5": 5021, "IR_Strahler_14": 5032, "IR_Strahler_23": 5033}

flexload_toBeckhoff = {
    "Heizstab_ST_12": "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.DAK.BvL_Tanks.BvL_Tank_SZ.Tankheizung.Heizstaebe.Heizstab_12.control.bSetStatusOnManual",
    "Heizstab_ST_34": "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.DAK.BvL_Tanks.BvL_Tank_SZ.Tankheizung.Heizstaebe.Heizstab_34.control.bSetStatusOnManual",
    "Heizstab_ST_5": "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.DAK.BvL_Tanks.BvL_Tank_SZ.Tankheizung.Heizstaebe.Heizstab_5.control.bSetStatusOnManual",
    "Heizstab_WT_12": "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.DAK.BvL_Tanks.BvL_Tank_RZ.Tankheizung.Heizstaebe.Heizstab_12.control.bSetStatusOnManual",
    "Heizstab_WT_34": "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.DAK.BvL_Tanks.BvL_Tank_RZ.Tankheizung.Heizstaebe.Heizstab_34.control.bSetStatusOnManual",
    "Heizstab_WT_5": "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.DAK.BvL_Tanks.BvL_Tank_RZ.Tankheizung.Heizstaebe.Heizstab_5.control.bSetStatusOnManual",
    "IR_Strahler_14": "has to be found",
    "IR_Strahler_23": "has to be found"
}

# nodes of beckhoff server to activate manual control

BVL_control_with_OPCUA = "ns=4;s=MAIN.VarPOU_LoTuS.localSetParameters.bOPC_Write_Enable"
BVL_HeizTankModus_set_manual = "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.DAK.BvL_Tanks.BvL_HeizTankModus.control.bManualModeActivated"
BVL_HeizTankModus_set_Status = "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.DAK.BvL_Tanks.BvL_HeizTankModus.control.bSetStatusOnManual"

activate_manual_control_SZ_Heizstab12 = "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.DAK.BvL_Tanks.BvL_Tank_SZ.Tankheizung.Heizstaebe.Heizstab_12.control.bManualModeActivated"
activate_manual_control_SZ_Heizstab34 = "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.DAK.BvL_Tanks.BvL_Tank_SZ.Tankheizung.Heizstaebe.Heizstab_34.control.bManualModeActivated"
activate_manual_control_SZ_Heizstab5 = "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.DAK.BvL_Tanks.BvL_Tank_SZ.Tankheizung.Heizstaebe.Heizstab_5.control.bManualModeActivated"

activate_manual_control_RZ_Heizstab12 = "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.DAK.BvL_Tanks.BvL_Tank_RZ.Tankheizung.Heizstaebe.Heizstab_12.control.bManualModeActivated"
activate_manual_control_RZ_Heizstab34 = "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.DAK.BvL_Tanks.BvL_Tank_RZ.Tankheizung.Heizstaebe.Heizstab_34.control.bManualModeActivated"
activate_manual_control_RZ_Heizstab5 = "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.DAK.BvL_Tanks.BvL_Tank_RZ.Tankheizung.Heizstaebe.Heizstab_5.control.bManualModeActivated"


# create nodeIDs from FlexLoads_Objects_dict
for k, v in FlexLoads_Objects_dict.items():
    globals()['ID_FlexLoads_' + k] = ua.NodeId(v, 8)

# all loads object nodes in AllLoads folder
AllLoads_Objects_dict = {"Abluefter": 5039, "Bandantrieb": 5028, "Heizstab_ST_12": 5025, "Heizstab_ST_34": 5026,
                         "Heizstab_ST_5": 5031, "Heizstab_WT_12": 5011, "Heizstab_WT_34": 5029, "Heizstab_WT_5": 5030,
                         "IR_Strahler_14": 5034, "IR_Strahler_23": 5035, "Induktor": 5042, "Kuehlpumpe": 5027,
                         "Luefter_SchwaKo": 5038, "Spritzpumpe_SZ": 5037, "Spritzpumpe_WZ": 5036,
                         "Umlufttrockner": 5040, "Vakuumtrockner": 5041}


# create nodeIDs from AllLoads_Objects_dict
for k, v in AllLoads_Objects_dict.items():
    globals()['ID_AllLoads_' + k] = ua.NodeId(v, 8)

# all nodes that need a timestamp as value
All_BeginOfCalcualtion_Nodes_dict = {"CO2PerWorkpiece_BeginOfCalculation": 6042,
                                     "ElCostPerWorkpiece_BeginOfCalculation": 6035,
                                     "RunTimeData_BeginOfCalculation": 6011, "TotalCO2_BeginOfCalculation": 6014,
                                     "ElPowerConsumtion_BeginOfCalculation": 6018,
                                     "AirConsumption_BeginOfCalculation": 6027}
# create nodeIDs from All_BeginOfCalcualtion_Nodes_dict
for k, v in All_BeginOfCalcualtion_Nodes_dict.items():
    globals()['ID_' + k] = ua.NodeId(v, 8)

# initialize variables, will be updated from central server
globals()['V_El_Price'] = 0.0
globals()['V_CEF_total'] = 0.0

# set up scheduler for automation
scheduler = sched.scheduler(datetime.now, time.sleep)
# try new scheduler with time.time and time.sleep. Datetime.now doesn't return number
scheduler2 = sched.scheduler(time.time, time.sleep)

tasks_queue = []
task_counter = 0


##########################################machine data funs start

async def update_PLC_values():
    tries = 0

    while tries < 6:
        try:
            await Beckhoff_DLRA_Server_client.connect()
            print('Client successfully connected')

            tries = 7
        except:
            print("failed to connect to DLRA, retrying in 3s")
            tries += 1
    if tries == 6:
        print("max tries reached, try simulating the machine data")

    # now retrieve values from siemens plc if available
    for index, row in Mappingtable.iterrows():

        locals()['N_' + row.iloc[0]] = server.get_node(ua.NodeId(row.iloc[1], 8))
        # print(row.iloc[1])
        # print(ua.NodeId(row.iloc[1], 8))
        # print('server data')
        # print('N_' + row.iloc[0])

        if str(row.iloc[3]) != "Null":

            # print('string')
            # print(str(row.iloc[3]))
            locals()['Siemens_N_' + row.iloc[0]] = Beckhoff_DLRA_Server_client.get_node(str(row.iloc[3]))
            # print('siemens data')
            # print(locals()['Siemens_N_' + row.iloc[0]])
            # print(locals()['Siemens_N_' + row.iloc[0]].read_value())

            locals()['Siemens_V_' + row.iloc[0]] = await locals()['Siemens_N_' + row.iloc[0]].read_value()
            # print('value')
            # print(locals()['Siemens_V_' + row.iloc[0]])

            try:
                if row.iloc[5] == "Bool" and row.iloc[2] != "Null":
                    ANode = server.get_node(ua.NodeId(int(row.iloc[2]), 8))
                    await ANode.set_value(locals()['Siemens_V_' + row.iloc[0]])
                    await locals()['N_' + row.iloc[0]].set_value(locals()['Siemens_V_' + row.iloc[0]])

                elif row.iloc[5] == "Bool":
                    await locals()['N_' + row.iloc[0]].set_value(locals()['Siemens_V_' + row.iloc[0]])

                elif row.iloc[5] == "Float" and row.iloc[2] != "Null":
                    ANode = server.get_node(ua.NodeId(int(row.iloc[2]), 8))
                    await ANode.set_value(float(locals()['Siemens_V_' + row.iloc[0]]))
                    await locals()['N_' + row.iloc[0]].set_value(float(locals()['Siemens_V_' + row.iloc[0]]))

                elif row[5] == "Float":
                    await locals()['N_' + row[0]].set_value(float(locals()['Siemens_V_' + row[0]]))

            except Exception as e:
                print(e)


        # use simulated values when not available from siemens server
        else:

            try:
                if row.iloc[5] == "Bool" and row.iloc[2] != "Null":
                    ANode = server.get_node(ua.NodeId(int(row[2]), 8))
                    await ANode.set_value(eval(row[4]))
                    await locals()['N_' + row[0]].set_value(eval(row[4]))

                elif row.iloc[5] == "Bool":
                    await locals()['N_' + row.iloc[0]].set_value(eval(row.iloc[4]))

                elif row.iloc[5] == "Float" and row.iloc[2] != "Null":
                    ANode = server.get_node(ua.NodeId(int(row[2]), 8))
                    await ANode.set_value(float(row[4]))
                    await locals()['N_' + row[0]].set_value(float(row[4]))

                elif row.iloc[5] == "Float":
                    await locals()['N_' + row.iloc[0]].set_value(float(row.iloc[4]))

                elif row.iloc[5] == "Int":
                    await locals()['N_' + row.iloc[0]].set_value(ua.uatypes.Int32(row.iloc[4]))

            except Exception as e:
                print(e)


async def simulate_PLC_Values():
    # create node objects from relevant nodes of mappingtable for proxy server:
    for index, row in Mappingtable.iterrows():
        locals()['N_' + row.iloc[0]] = server.get_node(ua.NodeId(row.iloc[1], 8))

        try:
            if row.iloc[5] == "Bool" and row.iloc[2] != "Null":
                ANode = server.get_node(ua.NodeId(int(row.iloc[2]), 8))
                await ANode.set_value(eval(row.iloc[4]))
                await locals()['N_' + row.iloc[0]].set_value(eval(row.iloc[4]))
            elif row.iloc[5] == "Bool":
                await locals()['N_' + row.iloc[0]].set_value(eval(row.iloc[4]))
            elif row.iloc[5] == "Float" and row.iloc[2] != "Null":
                ANode = server.get_node(ua.NodeId(int(row.iloc[2]), 8))
                await ANode.set_value(float(row.iloc[4]))
                await locals()['N_' + row.iloc[0]].set_value(float(row.iloc[4]))
            elif row.iloc[5] == "Float":
                await locals()['N_' + row.iloc[0]].set_value(float(row.iloc[4]))
            elif row.iloc[5] == "Int":
                await locals()['N_' + row.iloc[0]].set_value(ua.uatypes.Int32(row.iloc[4]))

        except Exception as e:
            print(e)


async def set_manual():
    try:
        await set_server_node_true(BVL_control_with_OPCUA)
        await set_server_node_true(BVL_HeizTankModus_set_manual)
        await set_server_node_true(BVL_HeizTankModus_set_Status)
        await set_server_node_true(activate_manual_control_SZ_Heizstab12)
        await set_server_node_true(activate_manual_control_SZ_Heizstab34)
        await set_server_node_true(activate_manual_control_SZ_Heizstab5)
        await set_server_node_true(activate_manual_control_RZ_Heizstab12)
        await set_server_node_true(activate_manual_control_RZ_Heizstab34)
        await set_server_node_true(activate_manual_control_RZ_Heizstab5)

    except:
        print('failed to set control to manual')




# use machine interval for advancing workpiece counter, also calcs KPIs per workpiece
async def workpiece_counter():
    N_CompletedWorkpieces = server.get_node(ua.NodeId(6025, 8))

    N_CO2pWP_CurrentValue = server.get_node(ua.NodeId(6041, 8))
    N_CO2pWP_Improvement = server.get_node(ua.NodeId(6043, 8))
    N_CO2pWP_Baseline = server.get_node(ua.NodeId(6039, 8))
    V_CO2pWP_Baseline = await N_CO2pWP_Baseline.read_value()
    # V_CO2pWP_Baseline = 1 #for testing

    N_EURpWP_CurrentValue = server.get_node(ua.NodeId(6034, 8))
    N_EURpWP_Improvement = server.get_node(ua.NodeId(6036, 8))
    N_EURpWP_Baseline = server.get_node(ua.NodeId(6032, 8))
    V_EURpWP_Baseline = await N_EURpWP_Baseline.read_value()
    # V_EURpWP_Baseline = 1 #for testing

    while True:
        val = await N_CompletedWorkpieces.read_value()
        await locals()['N_CompletedWorkpieces'].set_value(ua.uatypes.Int32(val + 1))

        V_CO2pWP_CurrentValue = (await globals()['N_total_CurrentCO2Emission'].read_value()) * 12 * (1 / 3600)
        await N_CO2pWP_CurrentValue.set_value(V_CO2pWP_CurrentValue)
        await N_CO2pWP_Improvement.set_value(1 - (V_CO2pWP_CurrentValue / V_CO2pWP_Baseline))

        V_EURpWP_CurrentValue = (await globals()['N_total_CurrentElectricPowerConsumption'].read_value()) * 12 * (
                1 / 3600) * globals()['V_El_Price'] * (
                                        1 / 1000000)  # eur/MWh, divide for Wh equasion, get from central server
        await N_EURpWP_CurrentValue.set_value(V_EURpWP_CurrentValue)
        await N_EURpWP_Improvement.set_value(1 - (V_EURpWP_CurrentValue / V_EURpWP_Baseline))

        time.sleep(12)  # interval between workpieces


# OPC UA client periodically retrieves CEF_total and electricity price from central server
async def CentralServer_client():
    connected = False
    while connected == False:
        try:
            await Central_Server_client.connect()
            print('Client successfully connected to ' + Central_Server_url)
            connected = True
        except:
            print("could not connect to central server, retrying in 5s")
            time.sleep(5)

    globals()['N_CEF_total'] = Central_Server_client.get_node(ua.NodeId(6009, 8))

    while True:
        try:
            globals()['V_CEF_total'] = await globals()['N_CEF_total'].read_value()
            #print(globals()['V_CEF_total'])

            timeframe_number = datetime.now().hour + 1
            timeframe_key = 'tf' + str(timeframe_number) + '_price'
            globals()['N_El_Price'] = Central_Server_client.get_node(ua.NodeId(tf_price_dict[timeframe_key], 8))
            #print(tf_price_dict[timeframe_key])
            globals()['V_El_Price'] = await globals()['N_El_Price'].read_value()
            #print(globals()['V_El_Price'])


        except Exception as e:
            print(e)
            print("error while reading nodes from central server, retrying")
            try:
                await Central_Server_client.disconnect()
                await Central_Server_client.connect()
            except:
                pass

        # for testing:
        # print('price: ' + str(globals()['V_El_Price']))
        # print('cef: ' + str(globals()['V_CEF_total']))

        time.sleep(10)  # 10s for now


###################################calc funs start


# fun to calculate the CO2 emissions using CEF_total and either the IsOn and AveragePower Child or directly the Power child
async def CO2_calc():
    # iterate over nodes in FlexLoads folder
    for k, v in FlexLoads_Objects_dict.items():

        # get relevant childen as node objects
        try:
            N_Power = await globals()['N_FlexLoads_' + k].get_child(ua.QualifiedName("AveragePower", 7))
        except:
            N_Power = await globals()['N_FlexLoads_' + k].get_child(ua.QualifiedName("Power", 7))
        N_IsOn = await globals()['N_FlexLoads_' + k].get_child(ua.QualifiedName("IsOn", 7))

        # get values of relevant node objects in separate corutines
        V_Power = await N_Power.read_value()
        V_IsOn = await N_IsOn.read_value()

        # for testing set all flex loads to on
        # V_IsOn = True

        # calculate co2Emission with CEF_total, CHECK UNITS!
        V_CurrentCO2Emission = V_Power * V_IsOn * globals()['V_CEF_total'] * (1 / 1000)

        # directly write node values
        N_CurrentCO2Emission = await globals()['N_FlexLoads_' + k].get_child(ua.QualifiedName("CurrentCO2Emission", 7))
        await N_CurrentCO2Emission.set_value(V_CurrentCO2Emission)

    # iterate over nodes in AllLoads Folder
    for k, v in AllLoads_Objects_dict.items():

        # get relevant childen as node objects
        try:
            N_Power = await globals()['N_AllLoads_' + k].get_child(ua.QualifiedName("AveragePower", 7))
        except:
            N_Power = await globals()['N_AllLoads_' + k].get_child(ua.QualifiedName("Power", 7))
        N_IsOn = await globals()['N_AllLoads_' + k].get_child(ua.QualifiedName("IsOn", 7))

        # get values of relevant node objects in separate corutines
        V_Power = await N_Power.read_value()
        V_IsOn = await N_IsOn.read_value()

        # for testing set all flex loads to on
        # V_IsOn = True

        # calculate co2Emission with CEF_total
        V_CurrentCO2Emission = V_Power * V_IsOn * globals()['V_CEF_total'] * (1 / 1000)

        # directly write node values
        N_CurrentCO2Emission = await globals()['N_AllLoads_' + k].get_child(ua.QualifiedName("CurrentCO2Emission", 7))
        await N_CurrentCO2Emission.set_value(V_CurrentCO2Emission)


async def totalizer():
    total_CurrentCO2Emission = 0
    total_CurrentElectricPowerConsumption = 0

    # iterate over all loads
    for k, v in AllLoads_Objects_dict.items():

        # add current emissions
        N_CurrentCO2Emission = await globals()['N_AllLoads_' + k].get_child(ua.QualifiedName("CurrentCO2Emission", 7))
        V_CurrentCO2Emission = await N_CurrentCO2Emission.read_value()
        total_CurrentCO2Emission = total_CurrentCO2Emission + V_CurrentCO2Emission

        # add current electricity consumption
        try:
            N_Power = await globals()['N_AllLoads_' + k].get_child(ua.QualifiedName("AveragePower", 7))
        except:
            N_Power = await globals()['N_AllLoads_' + k].get_child(ua.QualifiedName("Power", 7))
        N_IsOn = await globals()['N_AllLoads_' + k].get_child(ua.QualifiedName("IsOn", 7))
        V_Power = await N_Power.read_value()
        V_IsOn = await N_IsOn.read_value()

        # for testing set V_IsOn to True
        # V_IsOn = True

        V_el_Consumption = V_Power * V_IsOn
        total_CurrentElectricPowerConsumption = total_CurrentElectricPowerConsumption + V_el_Consumption

    # write total node values
    await globals()['N_total_CurrentCO2Emission'].set_value(total_CurrentCO2Emission)
    await globals()['N_total_CurrentElectricPowerConsumption'].set_value(total_CurrentElectricPowerConsumption)


async def amount_calc():
    N_CO2Current = server.get_node(ua.NodeId(6015, 8))
    N_CO2Amount = server.get_node(ua.NodeId(6013, 8))
    V_CO2Current = await N_CO2Current.read_value()
    V_CO2Amount = await N_CO2Amount.read_value()
    V_CO2Amount = V_CO2Amount + V_CO2Current * (1 / 3600) * 10  # for calculation interval of 10s
    await N_CO2Amount.set_value(float(V_CO2Amount))

    N_ElPowerCurrent = server.get_node(ua.NodeId(6019, 8))
    N_ElPowerAmount = server.get_node(ua.NodeId(6017, 8))
    V_ElPowerCurrent = await N_ElPowerCurrent.read_value()
    V_ElPowerAmount = await N_ElPowerAmount.read_value()
    V_ElPowerAmount = V_ElPowerAmount + V_ElPowerCurrent * (1 / 3600) * 10  # for calculation interval of 10s
    await N_ElPowerAmount.set_value(float(V_ElPowerAmount))

    N_AirCurrent = server.get_node(ua.NodeId(6028, 8))
    N_AirAmount = server.get_node(ua.NodeId(6026, 8))
    V_AirCurrent = await N_AirCurrent.read_value()
    V_AirAmount = await N_AirAmount.read_value()
    V_AirAmount = V_AirAmount + V_AirCurrent * (1 / 3600) * 10  # for calculation interval of 10s
    await N_AirAmount.set_value(float(V_AirAmount))


#######################################################################time funs start

# initial timestamp for startup
async def timestamper():
    globals()['V_BeginOfCalculation'] = datetime.now()
    global V_BeginOfCalculation
    # print(V_BeginOfCalculation)

    for k, v in All_BeginOfCalcualtion_Nodes_dict.items():
        await globals()['N_' + k].set_value(V_BeginOfCalculation)


# working hours counter
async def WorkingHours_counter():
    global V_BeginOfCalculation

    while True:
        elapsed_time = datetime.now() - V_BeginOfCalculation
        elapsed_seconds = elapsed_time.seconds
        V_WorkingHours = math.floor((elapsed_seconds / 3600) * 4) / 4
        await globals()['N_WorkingHours'].set_value(V_WorkingHours)

        # for testing/feedback
        print("WorkingHours: " + str(V_WorkingHours))

        # wait 15 mins before next execution
        await asyncio.sleep(2 * 60)


#######################################################################efdm funs start

#############testing how to create new node obj in planned measures folder

# sets load changes for a Measure Obj
async def set_loadchanges(browsename, nodeID, LoadChangeNumber, AssEnflexMeasureID, AssFlexLoadID, power, timestamp,
                          unit):
    # for testing:
    # global V_BeginOfCalculation

    date_format = "%Y-%m-%dT%H:%M:%SZ"

    await globals()['N_' + browsename + '_LoadChangesFolder'].add_object(ua.NodeId(nodeID + (20 * LoadChangeNumber), 8),
                                                                         ua.QualifiedName(
                                                                             browsename + '_LoadChange' + str(
                                                                                 LoadChangeNumber), 8),
                                                                         ua.NodeId(1008, 7), True)  # This works

    New_LoadChange = server.get_node(ua.NodeId(nodeID + (20 * LoadChangeNumber), 8))

    globals()['N_' + browsename + '_LoadChange' + str(
        LoadChangeNumber) + '_AssEnFlexMeasureID'] = await New_LoadChange.get_child(
        ua.QualifiedName("AssociatedEnFlexMeasureID", 7))
    await globals()['N_' + browsename + '_LoadChange' + str(LoadChangeNumber) + '_AssEnFlexMeasureID'].set_value(
        AssEnflexMeasureID)

    globals()[
        'N_' + browsename + '_LoadChange' + str(LoadChangeNumber) + '_AssFlexLoadID'] = await New_LoadChange.get_child(
        ua.QualifiedName("AssociatedFlexibleLoadID", 7))
    await globals()['N_' + browsename + '_LoadChange' + str(LoadChangeNumber) + '_AssFlexLoadID'].set_value(
        AssFlexLoadID)

    globals()['N_' + browsename + '_LoadChange' + str(LoadChangeNumber) + '_Power'] = await New_LoadChange.get_child(
        ua.QualifiedName("Power", 7))
    await globals()['N_' + browsename + '_LoadChange' + str(LoadChangeNumber) + '_Power'].set_value(float(power))

    globals()[
        'N_' + browsename + '_LoadChange' + str(LoadChangeNumber) + '_Timestamp'] = await New_LoadChange.get_child(
        ua.QualifiedName("Timestamp", 7))
    await globals()['N_' + browsename + '_LoadChange' + str(LoadChangeNumber) + '_Timestamp'].set_value(
        datetime.strptime(timestamp, date_format))

    globals()['N_' + browsename + '_LoadChange' + str(LoadChangeNumber) + '_Unit'] = await New_LoadChange.get_child(
        ua.QualifiedName("Unit", 7))
    await globals()['N_' + browsename + '_LoadChange' + str(LoadChangeNumber) + '_Unit'].set_value(unit)

    # do the same for AllLoadChanges folder:
    await globals()['N_AllLoadChangesFolder'].add_object(ua.NodeId(nodeID + 300 + (20 * LoadChangeNumber), 8),
                                                         ua.QualifiedName(
                                                             browsename + '_LoadChange' + str(LoadChangeNumber), 8),
                                                         ua.NodeId(1008, 7), True)  # This works

    New_LoadChange = server.get_node(ua.NodeId(nodeID + 300 + (20 * LoadChangeNumber), 8))

    globals()['N_' + browsename + '_LoadChange' + str(
        LoadChangeNumber) + '_AssEnFlexMeasureID'] = await New_LoadChange.get_child(
        ua.QualifiedName("AssociatedEnFlexMeasureID", 7))
    await globals()['N_' + browsename + '_LoadChange' + str(LoadChangeNumber) + '_AssEnFlexMeasureID'].set_value(
        AssEnflexMeasureID)

    globals()[
        'N_' + browsename + '_LoadChange' + str(LoadChangeNumber) + '_AssFlexLoadID'] = await New_LoadChange.get_child(
        ua.QualifiedName("AssociatedFlexibleLoadID", 7))
    await globals()['N_' + browsename + '_LoadChange' + str(LoadChangeNumber) + '_AssFlexLoadID'].set_value(
        AssFlexLoadID)

    globals()['N_' + browsename + '_LoadChange' + str(LoadChangeNumber) + '_Power'] = await New_LoadChange.get_child(
        ua.QualifiedName("Power", 7))
    await globals()['N_' + browsename + '_LoadChange' + str(LoadChangeNumber) + '_Power'].set_value(float(power))

    globals()[
        'N_' + browsename + '_LoadChange' + str(LoadChangeNumber) + '_Timestamp'] = await New_LoadChange.get_child(
        ua.QualifiedName("Timestamp", 7))
    await globals()['N_' + browsename + '_LoadChange' + str(LoadChangeNumber) + '_Timestamp'].set_value(
        datetime.strptime(timestamp, date_format))

    globals()['N_' + browsename + '_LoadChange' + str(LoadChangeNumber) + '_Unit'] = await New_LoadChange.get_child(
        ua.QualifiedName("Unit", 7))
    await globals()['N_' + browsename + '_LoadChange' + str(LoadChangeNumber) + '_Unit'].set_value(unit)


# creates a PlannedMeasure with given browsename
async def create_PlannedMeasure_node(efdm_path, browsename, nodeID):
    date_format = "%Y-%m-%dT%H:%M:%SZ"

    with open(efdm_path, 'r') as file:

        # load json data into python obj
        data = json.load(file)

        NewMeasure_FlexLoadID = data["flexibleLoadMeasures"][0]["flexibleLoadId"]["uuid"]
        NewMeasure_EnFlexMeasureID = data["flexibleLoadMeasures"][0]["flexibleLoadMeasureId"]["uuid"]
        NewMeasure_RewardAmount = data["flexibleLoadMeasures"][0]["reward"]["value"]
        NewMeasure_RewardUnit = data["flexibleLoadMeasures"][0]["reward"]["unit"]
        NewMeasure_Status = "planned"
        NewMeasure_AllLoadChanges = data["flexibleLoadMeasures"][0]["loadChangeProfile"]

    await globals()['N_PlannedEnFlexMeasuresFolder'].add_object(ua.NodeId(nodeID, 8), ua.QualifiedName(browsename, 8),
                                                                ua.NodeId(1004, 7), True)  # This works

    New_Measure = server.get_node(ua.NodeId(nodeID, 8))

    # write AssociatedFlexLoadID
    globals()['N_' + browsename + '_AssFlexLoadID'] = await New_Measure.get_child(
        ua.QualifiedName("AssociatedFlexLoadID", 7))
    await globals()['N_' + browsename + '_AssFlexLoadID'].set_value(NewMeasure_FlexLoadID)

    # write EnFlexMeasureID
    globals()['N_' + browsename + '_EnFlexMeasureID'] = await New_Measure.get_child(
        ua.QualifiedName("EnFlexMeasureID", 7))
    await globals()['N_' + browsename + '_EnFlexMeasureID'].set_value(NewMeasure_EnFlexMeasureID)

    # get Reward Obj
    New_Measure_Reward = await New_Measure.get_child(ua.QualifiedName("Reward", 7))

    # write RewardAmount
    globals()['N_' + browsename + '_RewardAmount'] = await New_Measure_Reward.get_child(ua.QualifiedName("Amount", 7))
    await globals()['N_' + browsename + '_RewardAmount'].set_value(float(NewMeasure_RewardAmount))

    # write RewardAmount
    globals()['N_' + browsename + '_RewardUnit'] = await New_Measure_Reward.get_child(ua.QualifiedName("Unit", 7))
    await globals()['N_' + browsename + '_RewardUnit'].set_value(NewMeasure_RewardUnit)

    # write Status
    globals()['N_' + browsename + '_Status'] = await New_Measure.get_child(ua.QualifiedName("Status", 7))
    await globals()['N_' + browsename + '_Status'].set_value(NewMeasure_Status)

    # get LoadChanges folder node obj
    globals()['N_' + browsename + '_LoadChangesFolder'] = await New_Measure.get_child(
        ua.QualifiedName("LoadChanges", 7))

    # get first LoadChange node obj
    globals()['N_' + browsename + '_LoadChange1'] = await globals()['N_' + browsename + '_LoadChangesFolder'].get_child(
        ua.QualifiedName("<LoadChange1>", 7))

    # delete blank mandatory node change
    node_to_delete = server.get_node(globals()['N_' + browsename + '_LoadChange1'])
    nodes_to_delete = []
    nodes_to_delete.append(node_to_delete)
    await server.delete_nodes(nodes_to_delete)

    # set loadchanges
    i = 0
    while i < len(NewMeasure_AllLoadChanges):
        NewLoadChange_Number = i + 1
        power = NewMeasure_AllLoadChanges[i]["power"]["value"]
        timestamp = NewMeasure_AllLoadChanges[i]["timestamp"]
        unit = NewMeasure_AllLoadChanges[i]["power"]["unit"]

        await set_loadchanges(browsename, nodeID, NewLoadChange_Number, NewMeasure_EnFlexMeasureID,
                              NewMeasure_FlexLoadID, power, timestamp, unit)

        # schedule to move measure to completed once last load change happened
        if i == (len(NewMeasure_AllLoadChanges) - 1):
            # print(datetime.strptime(timestamp, date_format))
            # scheduler.enterabs(datetime.strptime(timestamp, date_format), 1, move_completed_measure,
            #                    argument=(nodeID, browsename))

            delay = (datetime.strptime(timestamp, date_format) - datetime.now()).total_seconds()

            scheduler2.enterabs(delay, 1, move_completed_measure,
                                argument=(nodeID, browsename))

            print(browsename + " is scheduled to complete on " + str(timestamp))

        i += 1


# what happens when a measure should be removed
async def move_completed_measure(NodeID_of_measure_to_move, browsename_of_measure_to_move):
    # first save all necessary values
    N_measure_to_move = server.get_node(ua.NodeId(NodeID_of_measure_to_move, 8))

    N_FlexloadID_to_move = await N_measure_to_move.get_child(ua.QualifiedName("AssociatedFlexLoadID", 7))
    V_FlexloadID_to_move = await N_FlexloadID_to_move.read_value()

    N_EnflexMeasrueID_to_move = await N_measure_to_move.get_child(ua.QualifiedName("EnFlexMeasureID", 7))
    V_EnflexMeasrueID_to_move = await N_EnflexMeasrueID_to_move.read_value()

    N_Reward_to_move = await N_measure_to_move.get_child(ua.QualifiedName("Reward", 7))

    N_Reward_Amount_to_move = await N_Reward_to_move.get_child(ua.QualifiedName("Amount", 7))
    V_Reward_Amount_to_move = await N_Reward_Amount_to_move.read_value()

    N_Reward_Unit_to_move = await N_Reward_to_move.get_child(ua.QualifiedName("Unit", 7))
    V_Reward_Unit_to_move = await N_Reward_Unit_to_move.read_value()

    N_LoadChanges_Folder_to_move = await N_measure_to_move.get_child(ua.QualifiedName("LoadChanges", 7))
    LoadChanges = await N_LoadChanges_Folder_to_move.get_children()

    i = 0
    while i < len(LoadChanges):
        locals()['V_LoadChange' + str(i + 1) + '_Power_to_move'] = await LoadChanges[i].get_child(
            ua.QualifiedName("Power", 7))
        locals()['V_LoadChange' + str(i + 1) + '_Timestamp_to_move'] = await LoadChanges[i].get_child(
            ua.QualifiedName("Timestamp", 7))
        locals()['V_LoadChange' + str(i + 1) + '_Unit_to_move'] = await LoadChanges[i].get_child(
            ua.QualifiedName("Unit", 7))
        i += 1

    # then delete node
    nodes_to_move = []
    nodes_to_move.append(N_measure_to_move)
    await server.delete_nodes(nodes_to_move)

    # create new node in completed folder and set values
    await globals()['N_CompletedEnFlexMeasuresFolder'].add_object(ua.NodeId(NodeID_of_measure_to_move, 8),
                                                                  ua.QualifiedName(browsename_of_measure_to_move, 8),
                                                                  ua.NodeId(1004, 7), True)  # This works

    New_Completed_Measure = server.get_node(ua.NodeId(NodeID_of_measure_to_move, 8))

    # write AssociatedFlexLoadID
    N_New_Completed_Measure_AssociatedFlexLoadID = await New_Completed_Measure.get_child(
        ua.QualifiedName("AssociatedFlexLoadID", 7))
    await N_New_Completed_Measure_AssociatedFlexLoadID.set_value(V_FlexloadID_to_move)

    # write EnFlexMeasureID
    N_New_Completed_Measure_EnFlexMeasureID = await New_Completed_Measure.get_child(
        ua.QualifiedName("EnFlexMeasureID", 7))
    await N_New_Completed_Measure_EnFlexMeasureID.set_value(V_EnflexMeasrueID_to_move)

    # get Reward Obj
    New_Completed_Measure_Reward = await New_Completed_Measure.get_child(ua.QualifiedName("Reward", 7))

    # write RewardAmount
    New_Completed_Measure_RewardAmount = await New_Completed_Measure_Reward.get_child(ua.QualifiedName("Amount", 7))
    await New_Completed_Measure_RewardAmount.set_value(float(V_Reward_Amount_to_move))

    # write RewardAmount
    New_Completed_Measure_RewardUnit = await New_Completed_Measure_Reward.get_child(ua.QualifiedName("Unit", 7))
    await New_Completed_Measure_RewardUnit.set_value(V_Reward_Unit_to_move)

    # write Status
    New_Completed_Measure_Status = await New_Completed_Measure.get_child(ua.QualifiedName("Status", 7))
    await New_Completed_Measure_Status.set_value("completed")

    # get LoadChanges folder node obj
    New_Completed_Measure_LoadChangesFolder = await New_Completed_Measure.get_child(ua.QualifiedName("LoadChanges", 7))

    # get first LoadChange node obj
    New_Completed_Measure_LoadChangesFolder_LoadChange1 = await New_Completed_Measure_LoadChangesFolder.get_child(
        ua.QualifiedName("<LoadChange1>", 7))

    # delete blank mandatory node change
    nodes_to_delete = []
    nodes_to_delete.append(New_Completed_Measure_LoadChangesFolder_LoadChange1)
    await server.delete_nodes(nodes_to_delete)

    i = 0
    while i < len(LoadChanges):
        await New_Completed_Measure_LoadChangesFolder.add_object(ua.NodeId(13000 + 20 * i, 8),
                                                                 ua.QualifiedName("LoadChange" + str(i + 1), 8),
                                                                 ua.NodeId(1008, 7), True)  # This works

        New_Completed_LoadChange = server.get_node(ua.NodeId(13000 + 20 * i, 8))

        LoadChange_Power = New_Completed_LoadChange.get_child(ua.QualifiedName("Power", 7))
        await LoadChange_Power.set_value(locals()['V_LoadChange' + str(i + 1) + '_Power_to_move'])

        LoadChange_Timestamp = New_Completed_LoadChange.get_child(ua.QualifiedName("Timestamp", 7))
        await LoadChange_Timestamp.set_value(locals()['V_LoadChange' + str(i + 1) + '_Timestamp_to_move'])

        LoadChange_Unit = New_Completed_LoadChange.get_child(ua.QualifiedName("Unit", 7))
        await LoadChange_Unit.set_value(locals()['V_LoadChange' + str(i + 1) + '_Unit_to_move'])


def extrahiere_zahl(string):
    # Der reguläre Ausdruck sucht nach einer oder mehreren Ziffern (\d+)
    # am Ende des Strings ($ bedeutet Ende des Strings)
    match = re.search(r'(\d+)$', string)
    if match:
        return match.group(1)
    else:
        return None


# what happens if watchdog detects a creation of an efdm file maybe this needs a mediator, so that the watchdog can run an async fun
def on_efdm_created(event):
    print("new efdm received")
    path = event.src_path
    print(path)
    Split = path.split(".")
    print(Split)
    MeasurePart = Split[1]
    MeasureNumber = extrahiere_zahl(MeasurePart)
    print(MeasureNumber)

    asyncio.run(
        create_PlannedMeasure_node(event.src_path, "Measure" + MeasureNumber, 9999 + (int(MeasureNumber) - 1) * 700))


# create watchdog to look for new efdm files
def create_efdm_watchdog():
    The_event_handler = PatternMatchingEventHandler("*", None, False, False)
    The_event_handler.on_created = on_efdm_created
    efdmObserver = Observer()
    efdmObserver.schedule(The_event_handler,
                          "C:/Users/j.goelz/poser-eflex-apcm/Implementierung/MeasuresExchangeFolder",
                          True)
    efdmObserver.start()


async def set_server_node_true(id):
    server_node = Beckhoff_DLRA_Server_client.get_node(id)
    new_value = ua.DataValue(ua.Variant(True, ua.VariantType.Boolean))
    await server_node.write_value(new_value)


async def set_server_node_false(id):
    server_node = Beckhoff_DLRA_Server_client.get_node(id)
    new_value = ua.DataValue(ua.Variant(False, ua.VariantType.Boolean))
    await server_node.write_value(new_value)


# change flexload: called by flexload_automator to edit Power/IsOn value of specific flexible load
async def change_flexload(FlexLoad_to_automate, Power_to_automate):
    print('change_flexload')
    print(FlexLoad_to_automate)
    for k, v in flexloadID_dict.items():
        if v == FlexLoad_to_automate:
            N_IsOn_to_automate = await globals()['N_FlexLoads_' + k].get_child(ua.QualifiedName("IsOn", 7))
            print('flexload:')
            print(k)
            if Power_to_automate == 0:
                await N_IsOn_to_automate.set_value(False)
                await set_server_node_false(flexload_toBeckhoff[k])

            else:
                await N_IsOn_to_automate.set_value(True)
                await set_server_node_true(flexload_toBeckhoff[k])
                try:
                    N_Power_Value_to_automate = await globals()['N_FlexLoads_' + k].get_child(
                        ua.QualifiedName("Power", 7))
                    await N_Power_Value_to_automate.set_value(Power_to_automate)
                except:
                    pass
            print("LoadChange for " + k + " completed")
        else:
            pass


# method to check tasks_que for new tasks and execute them when scheduled
async def own_scheduler():
    global tasks_queue
    print('tasks_queue')
    print(tasks_queue)
    while True:
        now = datetime.now()
        if tasks_queue:
            task_time, _, task = tasks_queue[0]
            time_diff = (now - task_time).total_seconds()

            if time_diff > 300:
                # Verwirft die Aufgabe, da sie mehr als 1 Minute in der Vergangenheit liegt
                heapq.heappop(tasks_queue)
                print(
                    f"Discarding {task['FlexLoad_to_automate']} scheduled for {task_time} (more than 5 minute in the past)")
            elif task_time <= now:
                # Führe die Aufgabe aus, wenn der Zeitpunkt erreicht oder überschritten ist
                heapq.heappop(tasks_queue)
                await change_flexload(task['FlexLoad_to_automate'], task['Power_to_automate'])
            else:
                # Warte kurz, bevor erneut geprüft wird
                await asyncio.sleep(1)
        else:
            # Warte kurz, wenn keine Aufgaben vorhanden sind
            await asyncio.sleep(1)


# add task to minimal heap
def add_task(task):
    global tasks_queue, task_counter
    task_counter += 1
    heapq.heappush(tasks_queue, (task['time'], task_counter, task))

async def start_scheduler():
    print('own schedule started')
    # Starte den Scheduler und eine Task zum Hinzufügen neuer Aufgaben
    # scheduler started
    await asyncio.gather(
        own_scheduler(),
        flexload_automator()
    )


# flexload_automator: this schedules load changes for flexible loads by comparing timestamps in AllLoadChanges Folder
async def flexload_automator():
    while True:

        children = await globals()['N_AllLoadChangesFolder'].get_children()

        i = globals()['Initial_Children']

        if len(children) == globals()['Initial_Children']:
            print("no new LoadChanges to schedule")
        else:

            i = globals()['Initial_Children']
            priority_number = 1

            while i < len(children):
                # grab all parameters
                N_FlexLoad_to_automate = await children[i].get_child(ua.QualifiedName("AssociatedFlexibleLoadID", 7))
                V_FlexLoad_to_automate = await N_FlexLoad_to_automate.read_value()

                N_Power_to_automate = await children[i].get_child(ua.QualifiedName("Power", 7))
                V_Power_to_automate = await N_Power_to_automate.read_value()

                N_Time_to_automate = await children[i].get_child(ua.QualifiedName("Timestamp", 7))
                V_Time_to_automate = await N_Time_to_automate.read_value()

                # scheduler.enterabs(V_Time_to_automate, priority_number, change_flexload,
                #                   argument=(V_FlexLoad_to_automate, V_Power_to_automate))

                delay = (V_Time_to_automate - datetime.now()).total_seconds()
                # if delay > 0:
                #    await asyncio.sleep(delay)

                # await change_flexload(V_FlexLoad_to_automate, V_Power_to_automate)

                #scheduler2.enterabs(delay, priority_number, change_flexload,
                #                    argument=(V_FlexLoad_to_automate, V_Power_to_automate))

                print("successfully scheduled load change for " + V_FlexLoad_to_automate + ": " + str(
                    V_Time_to_automate) + ", " + str(V_Power_to_automate) + "W")
                print('time')
                print(V_Time_to_automate)
                print('Power')
                print(V_Power_to_automate)

                add_task({'FlexLoad_to_automate': V_FlexLoad_to_automate, 'time': V_Time_to_automate, 'Power_to_automate': V_Power_to_automate})


                priority_number += 1  # count up priority to schedule on/off loads correctly

                i += 1

            # save number of children to not create events twice
            globals()['Initial_Children'] = len(children)

        await asyncio.sleep(5)


#######################################################################opc ua server funs start

# initialize server - set security and load information models, then load datatypes from these models
async def opcua_proxy_server_init():
    await server.init()
    server.set_endpoint(server_url)
    server.set_server_name('Energy Flexibility DLRA Server ETA')
    server.set_security_policy(
        [
            ua.SecurityPolicyType.NoSecurity,
            # ua.SecurityPolicyType.Basic256Sha256_SignAndEncrypt,
            # ua.SecurityPolicyType.Basic256Sha256_Sign,
        ]
    )

    # Load information models from respective XML file
    await server.import_xml(DI_path)
    print(colored('DI loaded', 'green'))
    await server.import_xml(IA_path)
    print(colored('IA loaded', 'green'))
    await server.import_xml(Machinery_path)
    print(colored('Machinery loaded', 'green'))
    await server.import_xml(MT_path)
    print(colored('MT loaded', 'green'))
    await server.import_xml(CO2_path)
    print(colored('CO2 loaded', 'green'))
    await server.import_xml(EnFlex_path)
    print(colored('EnFlex loaded', 'green'))
    await server.import_xml(DLRAServer_path)
    print(colored('DLRA Server nodeset loaded', 'green'))

    # load type defs from imported nodesets
    await server.load_data_type_definitions()
    await server.start()

    # create node objects from relevant nodes of FlexLoads_Objects_dict:
    for k, v in FlexLoads_Objects_dict.items():
        globals()['N_FlexLoads_' + k] = server.get_node(globals()['ID_FlexLoads_' + k])

    # create node objects from relevant nodes of AllLoads_Objects_dict:
    for k, v in AllLoads_Objects_dict.items():
        globals()['N_AllLoads_' + k] = server.get_node(globals()['ID_AllLoads_' + k])

    # create node object for TotalCO2Emission
    globals()['N_total_CurrentCO2Emission'] = server.get_node(ua.NodeId(6015, 8))

    # create node object for TotalCO2Emission
    globals()['N_total_CurrentElectricPowerConsumption'] = server.get_node(ua.NodeId(6019, 8))

    # create node objects from relevant nodes of All_BeginOfCalcualtion_Nodes_dict:
    for k, v in All_BeginOfCalcualtion_Nodes_dict.items():
        globals()['N_' + k] = server.get_node(globals()['ID_' + k])

    # create node object for WorkingHours
    globals()['N_WorkingHours'] = server.get_node(ua.NodeId(6012, 8))

    # create node object for PlannedEnFlexMeasuresFolder
    globals()['N_PlannedEnFlexMeasuresFolder'] = server.get_node(ua.NodeId(5007, 8))

    # create node object for PlannedEnFlexMeasuresFolder
    globals()['N_AllLoadChangesFolder'] = server.get_node(ua.NodeId(5005, 8))

    # save how many children are in the AllLoadChangesFolder
    globals()['Initial_Children'] = len(await globals()['N_AllLoadChangesFolder'].get_children())

    # create node object for CompletedEnFlexMeasuresFolder
    globals()['N_CompletedEnFlexMeasuresFolder'] = server.get_node(ua.NodeId(5006, 8))

    print('Server started, listening on ' + server_url)


# async mediator needed to run WorkingHours_counter in separate thread
def WorkingHours_async_mediator():
    WH_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(WH_loop)
    WH_loop.create_task(WorkingHours_counter())
    WH_loop.run_forever()


# mediator to run async workpiece counter in separate thread
def workpiece_counter_async_mediator():
    WP_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(WP_loop)
    WP_loop.create_task(workpiece_counter())
    WP_loop.run_forever()


# mediator to run async central server client in separate thread
def CClient_async_mediator():
    CClient_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(CClient_loop)
    CClient_loop.create_task(CentralServer_client())
    CClient_loop.run_forever()



# Main coro
async def main():
    print('')
    print(
        '###############################################################################################################')
    print(
        'This is the BvL DLRA server, which executes energy flexibility measures and provides data for energy management')
    print(
        '###############################################################################################################')

    await opcua_proxy_server_init()
    await timestamper()

    # WorkingHours_counter in sepearate thread
    WH_thread = threading.Thread(target=WorkingHours_async_mediator)
    WH_thread.start()

    # sim plc values or receive plc values
    # await simulate_PLC_Values()
    await update_PLC_values()
    await set_manual()

    # workpiece_counter in separate thread
    WP_thread = threading.Thread(target=workpiece_counter_async_mediator)
    WP_thread.start()

    # client to central server in separate thread
    CClient_thread = threading.Thread(target=CClient_async_mediator)
    CClient_thread.start()

    # start efdm watchdog
    create_efdm_watchdog()
    print("efdm watchdog started")

    while True:
        print('server running...')

        await CO2_calc()
        await totalizer()

        #await flexload_automator()
        #print(str(len(scheduler2.queue)) + " entries in scheduler queue:")
        #print(scheduler2.queue)

        # run load change scheduler

        # scheduler2.run()
        print('await start_scheduler')
        await start_scheduler()
        print("load change scheduler started")

        await asyncio.sleep(10)

        # amount calc is currently hard set on 10s
        await amount_calc()


main_loop = asyncio.new_event_loop()
asyncio.set_event_loop(main_loop)
main_loop.create_task(main())
main_loop.run_forever()

