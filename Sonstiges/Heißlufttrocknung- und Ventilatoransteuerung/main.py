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


async def connect_to_server():
    # Connect to the server
    await Beckhoff_DLRA_Server_client.connect()

    print('Successfully connected')

    BVL_control_with_OPCUA = "ns=4;s=MAIN.VarPOU_LoTuS.localSetParameters.bOPC_Write_Enable"

    activate_manual_control_Ventilator = "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.Trocknung.UL.BvL_L_UL.control.bManualModeActivated"
    activate_Ventilator = "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.Trocknung.UL.BvL_L_UL.control.bSetStatusOnManual"
    ventilator_speed = "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.Trocknung.UL.BvL_L_UL.setSetPoint.fSetPointManual"

    activate_manual_control_Trocknungstemperatur = "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.Trocknung.UL.BvL_Heiz_UL.control.bManualModeActivated"
    activate_Trocknungstemperatur = "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.Trocknung.UL.BvL_Heiz_UL.control.bSetStatusOnManual"
    trocknungstemperatur = "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.Trocknung.UL.BvL_Heiz_UL.setSetPoint.fSetPointManual"

    # WICHTIG:
    # OperatingPoint = IST-Wert
    # SetPoint = SOLL-Wert
    # zur ansteuerung immer soll-werte benutzen
    """
    await read_server_node_value(BVL_control_with_OPCUA)
    await set_server_node_value(BVL_control_with_OPCUA, True)
    
    await read_server_node_value(activate_manual_control_Ventilator)
    await set_server_node_value(activate_manual_control_Ventilator, True)
    await read_server_node_value(activate_Ventilator)
    await set_server_node_value(activate_Ventilator, True)
    await read_server_node_value(ventilator_speed)
    await set_server_node_value(ventilator_speed, 2905)
    
    await read_server_node_value(activate_manual_control_Trocknungstemperatur)
    await set_server_node_value(activate_manual_control_Trocknungstemperatur, True)
    await read_server_node_value(activate_Trocknungstemperatur)
    await set_server_node_value(activate_Trocknungstemperatur, True)
    await read_server_node_value(trocknungstemperatur)
    await set_server_node_value(trocknungstemperatur, 70)
    """

    await read_server_node_value(BVL_control_with_OPCUA)
    await set_server_node_value(BVL_control_with_OPCUA, True)

    await read_server_node_value(activate_manual_control_Ventilator)
    await set_server_node_value(activate_manual_control_Ventilator, False)
    await read_server_node_value(activate_Ventilator)
    await set_server_node_value(activate_Ventilator, False)
    await read_server_node_value(ventilator_speed)
    await set_server_node_value(ventilator_speed, 0)

    await read_server_node_value(activate_manual_control_Trocknungstemperatur)
    await set_server_node_value(activate_manual_control_Trocknungstemperatur, False)
    await read_server_node_value(activate_Trocknungstemperatur)
    await set_server_node_value(activate_Trocknungstemperatur, False)
    await read_server_node_value(trocknungstemperatur)
    await set_server_node_value(trocknungstemperatur, 0)

    # Don't forget to disconnect when done
    await Beckhoff_DLRA_Server_client.disconnect()


# Run the asynchronous function in an event loop
asyncio.run(connect_to_server())
