import asyncio

from asyncua import Client, Node, ua

# setup client connecting to Beckhoff PLC
Beckhoff_DLRA_Server_url = 'opc.tcp://192.168.116.129:4840'
Beckhoff_DLRA_Server_client = Client(url=Beckhoff_DLRA_Server_url)


async def set_server_node_true(id):
    server_node = Beckhoff_DLRA_Server_client.get_node(id)
    new_value = ua.DataValue(ua.Variant(True, ua.VariantType.Boolean))
    await server_node.write_value(new_value)


async def set_server_node_false(id):
    server_node = Beckhoff_DLRA_Server_client.get_node(id)
    new_value = ua.DataValue(ua.Variant(False, ua.VariantType.Boolean))
    await server_node.write_value(new_value)


async def connect_to_server():
    # Connect to the server
    await Beckhoff_DLRA_Server_client.connect()

    print('Successfully connected')

    BVL_control_with_OPCUA = "ns=4;s=MAIN.VarPOU_LoTuS.localSetParameters.bOPC_Write_Enable"
    BVL_HeizTankModus_set_manual = "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.DAK.BvL_Tanks.BvL_HeizTankModus.control.bManualModeActivated"
    BVL_HeizTankModus_set_Status = "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.DAK.BvL_Tanks.BvL_HeizTankModus.control.bSetStatusOnManual"

    activate_manual_control_SZ_Heizstab12 = "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.DAK.BvL_Tanks.BvL_Tank_SZ.Tankheizung.Heizstaebe.Heizstab_12.control.bManualModeActivated"
    activate_manual_control_SZ_Heizstab34 = "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.DAK.BvL_Tanks.BvL_Tank_SZ.Tankheizung.Heizstaebe.Heizstab_34.control.bManualModeActivated"
    activate_manual_control_SZ_Heizstab5 = "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.DAK.BvL_Tanks.BvL_Tank_SZ.Tankheizung.Heizstaebe.Heizstab_5.control.bManualModeActivated"

    activate_manual_control_RZ_Heizstab12 = "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.DAK.BvL_Tanks.BvL_Tank_RZ.Tankheizung.Heizstaebe.Heizstab_12.control.bManualModeActivated"
    activate_manual_control_RZ_Heizstab34 = "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.DAK.BvL_Tanks.BvL_Tank_RZ.Tankheizung.Heizstaebe.Heizstab_34.control.bManualModeActivated"
    activate_manual_control_RZ_Heizstab5 = "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.DAK.BvL_Tanks.BvL_Tank_RZ.Tankheizung.Heizstaebe.Heizstab_5.control.bManualModeActivated"

    activate_SZ_Heizstab12 = "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.DAK.BvL_Tanks.BvL_Tank_SZ.Tankheizung.Heizstaebe.Heizstab_12.control.bSetStatusOnManual"
    activate_SZ_Heizstab34 = "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.DAK.BvL_Tanks.BvL_Tank_SZ.Tankheizung.Heizstaebe.Heizstab_34.control.bSetStatusOnManual"
    activate_SZ_Heizstab5 = "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.DAK.BvL_Tanks.BvL_Tank_SZ.Tankheizung.Heizstaebe.Heizstab_5.control.bSetStatusOnManual"

    activate_RZ_Heizstab12 = "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.DAK.BvL_Tanks.BvL_Tank_RZ.Tankheizung.Heizstaebe.Heizstab_12.control.bSetStatusOnManual"
    activate_RZ_Heizstab34 = "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.DAK.BvL_Tanks.BvL_Tank_RZ.Tankheizung.Heizstaebe.Heizstab_34.control.bSetStatusOnManual"
    activate_RZ_Heizstab5 = "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.DAK.BvL_Tanks.BvL_Tank_RZ.Tankheizung.Heizstaebe.Heizstab_5.control.bSetStatusOnManual"

    await set_server_node_false(activate_RZ_Heizstab12)
    await set_server_node_false(activate_RZ_Heizstab34)
    await set_server_node_false(activate_RZ_Heizstab5)

    await set_server_node_false(activate_SZ_Heizstab12)
    await set_server_node_false(activate_SZ_Heizstab34)
    await set_server_node_false(activate_SZ_Heizstab5)

    await set_server_node_false(activate_manual_control_SZ_Heizstab12)
    await set_server_node_false(activate_manual_control_SZ_Heizstab34)
    await set_server_node_false(activate_manual_control_SZ_Heizstab5)
    await set_server_node_false(activate_manual_control_RZ_Heizstab12)
    await set_server_node_false(activate_manual_control_RZ_Heizstab34)
    await set_server_node_false(activate_manual_control_RZ_Heizstab5)
    await set_server_node_false(BVL_HeizTankModus_set_manual)
    await set_server_node_false(BVL_HeizTankModus_set_Status)
    await set_server_node_false(BVL_control_with_OPCUA)


    """
    await set_server_node_true(BVL_control_with_OPCUA)
    await set_server_node_true(BVL_HeizTankModus_set_manual)
    await set_server_node_true(BVL_HeizTankModus_set_Status)
    await set_server_node_true(activate_manual_control_SZ_Heizstab12)
    await set_server_node_true(activate_manual_control_SZ_Heizstab34)
    await set_server_node_true(activate_manual_control_SZ_Heizstab5)
    await set_server_node_true(activate_manual_control_RZ_Heizstab12)
    await set_server_node_true(activate_manual_control_RZ_Heizstab34)
    await set_server_node_true(activate_manual_control_RZ_Heizstab5)

    await set_server_node_true(activate_RZ_Heizstab12)
    await set_server_node_true(activate_RZ_Heizstab34)
    await set_server_node_true(activate_RZ_Heizstab5)

    await set_server_node_true(activate_SZ_Heizstab12)
    await set_server_node_true(activate_SZ_Heizstab34)
    await set_server_node_true(activate_SZ_Heizstab5)
    """




    """
    # setup client connecting to Beckhoff PLC
    Beckhoff_DLRA_Server_url = 'opc.tcp://192.168.116.129:4840'
    Beckhoff_DLRA_Server_client = Client(url=Beckhoff_DLRA_Server_url)

    # Connect to the server
    await Beckhoff_DLRA_Server_client.connect()

    print('Successfully connected')


    node_BvL_HeizTankModus_control_bManualModeActivated = Beckhoff_DLRA_Server_client.get_node(
        "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.DAK.BvL_Tanks.BvL_Tank_RZ.Tankheizung.BvL_T_T.sensorState.fValue")

    node_BvL_HeizTankModus_control_bManualModeActivated2 = Beckhoff_DLRA_Server_client.get_node(
        "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.DAK.BvL_Tanks.BvL_Tank_SZ.Tankheizung.BvL_T_T.sensorState.fValue")


    value = await node_BvL_HeizTankModus_control_bManualModeActivated.read_value()
    value2 = await node_BvL_HeizTankModus_control_bManualModeActivated2.read_value()
    print(value)
    print(value2)
    """

    """
    string = "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.Trocknung.UL.BvL_Band_UTR.controlState.bStatusOn"
    node_test = Beckhoff_DLRA_Server_client.get_node(string)
    print(string)

    node_BvL_HeizTankModus_control_bManualModeActivated = Beckhoff_DLRA_Server_client.get_node(
        "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.DAK.BvL_Tanks.BvL_HeizTankModus.control.bManualModeActivated")
    value = await node_BvL_HeizTankModus_control_bManualModeActivated.read_value()
    print(value)

    new_value = ua.DataValue(ua.Variant(True, ua.VariantType.Boolean))
    await node_BvL_HeizTankModus_control_bManualModeActivated.write_value(new_value)

    node_BvL_HeizTankModus_control_bSetStatusOnManual = Beckhoff_DLRA_Server_client.get_node(
        "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.DAK.BvL_Tanks.BvL_HeizTankModus.control.bSetStatusOnManual")
    value = await node_BvL_HeizTankModus_control_bSetStatusOnManual.read_value()
    print(value)
    new_value = ua.DataValue(ua.Variant(False, ua.VariantType.Boolean))
    await node_BvL_HeizTankModus_control_bSetStatusOnManual.write_value(new_value)

    node_SZ_Heizstab_12_control_bManualModeActivated = Beckhoff_DLRA_Server_client.get_node(
        "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.DAK.BvL_Tanks.BvL_Tank_SZ.Tankheizung.Heizstaebe.Heizstab_12.control.bManualModeActivated")
    value = await node_SZ_Heizstab_12_control_bManualModeActivated.read_value()
    print(value)
    new_value = ua.DataValue(ua.Variant(False, ua.VariantType.Boolean))
    await node_SZ_Heizstab_12_control_bManualModeActivated.write_value(new_value)

    # NS4|String|MAIN.VarPOU_LoTuS.LoTuS.DAK.BvL_Tanks.BvL_Tank_SZ.Tankheizung.Heizstaebe.Heizstab_12.control.bSetStatusOnManual

    node_SZ_Heizstab_12_control_bSetStatusOnManual = Beckhoff_DLRA_Server_client.get_node(
        "ns=4;s=MAIN.VarPOU_LoTuS.LoTuS.DAK.BvL_Tanks.BvL_Tank_SZ.Tankheizung.Heizstaebe.Heizstab_12.control.bSetStatusOnManual")
    value = await node_SZ_Heizstab_12_control_bSetStatusOnManual.read_value()
    print(value)
    new_value = ua.DataValue(ua.Variant(True, ua.VariantType.Boolean))
    await node_SZ_Heizstab_12_control_bSetStatusOnManual.write_value(new_value)

    """

    # Don't forget to disconnect when done
    await Beckhoff_DLRA_Server_client.disconnect()


# Run the asynchronous function in an event loop
asyncio.run(connect_to_server())

