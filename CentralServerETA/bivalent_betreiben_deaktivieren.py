import asyncio
from asyncua import Client, Node, ua

# setup client connecting to Beckhoff PLC
Beckhoff_DLRA_Server_url = 'opc.tcp://192.168.116.129:4840'
Beckhoff_DLRA_Server_client = Client(url=Beckhoff_DLRA_Server_url)

async def read_server_node_value(id):
    server_node = Beckhoff_DLRA_Server_client.get_node(id)
    value = await server_node.read_value()
    print(f"Current value for {id}: {value}")

async def set_server_node_value(id, value):
    server_node = Beckhoff_DLRA_Server_client.get_node(id)
    if isinstance(value, bool):
        new_value = ua.DataValue(ua.Variant(value, ua.VariantType.Boolean))
    elif isinstance(value, (float, int)):
        new_value = ua.DataValue(ua.Variant(float(value), ua.VariantType.Float))
    else:
        raise ValueError(f"Unsupported type for value: {type(value)}")
    await server_node.write_value(new_value)
    print(f"New value set for {id}: {value}")
async def deactivate_red_net_heating():
    # Connect to the server
    await Beckhoff_DLRA_Server_client.connect()

    print('Successfully connected')

    BVL_control_with_OPCUA = "ns=4;s=MAIN.VarPOU_LoTuS.localSetParameters.bOPC_Write_Enable"

    activate_manual_control_Temperatur_Prozesswärme_RZ = "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.DAK.BvL_Tanks.BvL_Tank_RZ.Tankheizung.Technische_Waerme.Heiz_Soll_Pww.control.bManualModeActivated"
    activate_Temperatur_Prozesswärme_RZ = "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.DAK.BvL_Tanks.BvL_Tank_RZ.Tankheizung.Technische_Waerme.Heiz_Soll_Pww.control.bSetStatusOnManual"
    temperatur_prozesswärme_RZ = "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.DAK.BvL_Tanks.BvL_Tank_RZ.Tankheizung.Technische_Waerme.Heiz_Soll_Pww.setSetPoint.fSetPointManual"

    activate_manual_control_Temperatur_Prozesswärme_SZ = "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.DAK.BvL_Tanks.BvL_Tank_SZ.Tankheizung.Technische_Waerme.Heiz_Soll_Pww.control.bManualModeActivated"
    activate_Temperatur_Prozesswärme_SZ = "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.DAK.BvL_Tanks.BvL_Tank_SZ.Tankheizung.Technische_Waerme.Heiz_Soll_Pww.control.bSetStatusOnManual"
    temperatur_prozesswärme_SZ = "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.DAK.BvL_Tanks.BvL_Tank_SZ.Tankheizung.Technische_Waerme.Heiz_Soll_Pww.setSetPoint.fSetPointManual"
    # WICHTIG:
    # OperatingPoint = IST-Wert
    # SetPoint = SOLL-Wert
    # zur ansteuerung immer soll-werte benutzen

    await read_server_node_value(BVL_control_with_OPCUA)
    await set_server_node_value(BVL_control_with_OPCUA, True)

    await read_server_node_value(activate_manual_control_Temperatur_Prozesswärme_RZ)
    await set_server_node_value(activate_manual_control_Temperatur_Prozesswärme_RZ, False)
    await read_server_node_value(activate_Temperatur_Prozesswärme_RZ)
    await set_server_node_value(activate_Temperatur_Prozesswärme_RZ, False)
    await read_server_node_value(temperatur_prozesswärme_RZ)
    await set_server_node_value(temperatur_prozesswärme_RZ, 15)

    await read_server_node_value(activate_manual_control_Temperatur_Prozesswärme_SZ)
    await set_server_node_value(activate_manual_control_Temperatur_Prozesswärme_SZ, False)
    await read_server_node_value(activate_Temperatur_Prozesswärme_SZ)
    await set_server_node_value(activate_Temperatur_Prozesswärme_SZ, False)
    await read_server_node_value(temperatur_prozesswärme_SZ)
    await set_server_node_value(temperatur_prozesswärme_SZ, 15)


    # Don't forget to disconnect when done
    await Beckhoff_DLRA_Server_client.disconnect()

# Run the asynchronous function in an event loop
asyncio.run(deactivate_red_net_heating())
