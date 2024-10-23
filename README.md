# MT_G-lz_eflex_APCM
# Code
This code creates energy flexibility measures based on the electricity price market and executes them on a BvL YukonDAD cleaning machine at the ETA factory of TUD.

# Usage
CentralServerEta connects to the web interface Entso-E and requires energy market data. Multiple methods for measure generation are implemented. The measures are sent in JSON format to the MeasureExchangeFolder, which works as an interface between measure generation (CentralServerEta) and execution (EnFlexServerDLRA). The Device has to be connected to the wifi of Eta-factory and botth files (CentralServerEta and EnFlexServerDLRA) have to be executed to run the code.

# OPC UA
In the uaExpertConfig.uap file you can see OPC UA Servers for CentralServerEta and EnFlexServerDLRA. You can connect to the servers, after you started the code and can see all the node change live. 
