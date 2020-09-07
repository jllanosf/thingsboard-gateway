#     Copyright 2020. ThingsBoard
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

import json
import struct
from simplejson import dumps
from thingsboard_gateway.connectors.mqtt.mqtt_uplink_converter import MqttUplinkConverter, log


class CustomMqttUplinkConverter(MqttUplinkConverter):
    def __init__(self, config):
        self.__config = config.get('converter')
        self.dict_result = {}

    def convert(self, topic, body):
        try:
            self.dict_result["deviceName"] = topic.split("/")[-1] # getting all data after last '/' symbol in this case: if topic = 'devices/temperature/sensor1' device name will be 'sensor1'.
            self.dict_result["deviceType"] = "Accelerometer"  # just hardcode this
            self.dict_result["telemetry"] = []  # template for telemetry array
            bytes_to_read = body.replace("0x", "")  # Replacing the 0x (if '0x' in body), needs for converting to bytearray
            converted_bytes = bytearray.fromhex(bytes_to_read)  # Converting incoming data to bytearray

            output_dict = {}
            output_dict["ts"] = 0
            output_dict["values"] = {}

            data_order = ["ts", "x001", "x002"]

            if self.__config.get("extension-config") is not None:
                remainder=""
                if 'tsBytes' in config["extension-config"]:
                    chunk = bytes_to_read[:config["extension-config"]["tsBytes"]*2]
                    remainder = bytes_to_read[config["extension-config"]["tsBytes"]*2:]
                    ts_value = str(struct.unpack("l", bytearray.fromhex(chunk))[0])
                    ts_value = int(ts_value[:13])
                    output_dict["ts"] = ts_value
                else:
                    pass    #TODO: Throw exception on missing tsBytes

                array_format = "d"*(len(config["extension-config"])-1)
                #array_format = ""
                #for _ in range(len(config["extension-config"])-1):    #iterate over number of objects excluding timestamp
                #    array_format += "d"
                data_array = struct.unpack(array_format, bytearray.fromhex(remainder))

                for telemetry_key in self.__config["extension-config"]:  # Processing every telemetry key in config for extension
                    struct_key = telemetry_key.replace("Bytes", "")
                    if struct_key == "ts":
                        pass
                    else:
                        output_dict["values"][struct_key] = data_array[data_order.index(struct_key)-1]
                self.dict_result["telemetry"].append(output_dict)     # adding data to telemetry array
            else:
                self.dict_result["telemetry"] = {"data": int(body, 0)}  # if no specific configuration in config file - just send data which received
            return self.dict_result

        except Exception as e:
            log.exception('Error in converter, for config: \n%s\n and message: \n%s\n', dumps(self.__config), body)
            log.exception(e)
