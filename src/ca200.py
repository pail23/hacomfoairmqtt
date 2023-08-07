import paho.mqtt.client as mqtt
import serial
import time
import sys
import json

serial_port = "/dev/ttyUSB0"
refresh_interval = 10

# configuration values
ha_auto_discovery_device_id = "ca200"
ha_auto_discovery_device_name = "CA200"
ha_auto_discovery_device_manufacturer = "Zehnder"
ha_auto_discovery_device_model="ComfoAir 200"
ha_enable_auto_discovery_sensors = True
ha_enable_auto_discovery_climate = True
mqtt_user = "smarthome"
mqtt_password = "smarthome!23"
mqtt_server = "homeassistant.stadel15.net"
mqtt_port = 1883
mqtt_keep_alive = 45
ha_mqtt_topic = f"comfoair/{ha_auto_discovery_device_id}"
debug = True


#Command answers
READ_INPUTS = 0x4
READ_FAN_STATUS = 0x0c
READ_DELAYS = 0xca # Filter weeks in data[4]
READ_VENTILATION = 0xce
READ_TEMPERATURES = 0xd2
READ_ERRORS = 0xda # Filter Status in data[8]
READ_OPERATING_HOURS = 0xde # filter hours in int.from_bytes(data[15:17], byteorder='big')
READ_BYPASS_STATUS = 0xe0
READ_PREHEATING_STATUS = 0xe2
READ_RF_STATUS = 0xe6
READ_EWT = 0xec

class Command:
    def __init__(self, command, data):
        self._command = command
        self._data = data
    
    @property
    def command(self):
        return self._command
    
    @property
    def command_name(self):
        if self._command == READ_INPUTS:
            return "read inputs"
        elif self._command == READ_OPERATING_HOURS:
            return "read operating hours"
        elif self._command == READ_TEMPERATURES:
            return "read temperatures"
        elif self._command == READ_RF_STATUS:
            return "read rf status"
        elif self._command == READ_DELAYS:
            return "read delays"
        elif self._command == READ_VENTILATION:
            return "read ventilation"
        elif self._command == READ_ERRORS:
            return "read errors"
        elif self._command == READ_EWT:
            return "read ewt"
        elif self._command == READ_PREHEATING_STATUS:
            return "read preheating status"
        elif self._command == READ_BYPASS_STATUS:
            return "read bypass status"
        elif self._command == READ_FAN_STATUS:
            return "read fan status"
        else:
            return hex(self._command)

    @property
    def data(self):
        return self._data


    
def debug_msg(message):
    if debug is True:
        print('{0} DEBUG: {1}'.format(time.strftime("%d-%m-%Y %H:%M:%S", time.gmtime()), message))

def warning_msg(message):
    print('{0} WARNING: {1}'.format(time.strftime("%d-%m-%Y %H:%M:%S", time.gmtime()), message))

def info_msg(message):
    print('{0} INFO: {1}'.format(time.strftime("%d-%m-%Y %H:%M:%S", time.gmtime()), message))

def read_serial(ser):
    try:
        data = b''
        while ser.inWaiting() > 0:
            data += ser.read(1)
        if len(data) > 0:
            return data
        else:
            return None
    except:
        warning_msg('Serial command write and read exception:')
        warning_msg(sys.exc_info())
        return None      

def serial_command(cmd, ser):
    try:
        data = b''
        ser.write(cmd)
        time.sleep(2)

        while ser.inWaiting() > 0:
            data += ser.read(1)
        if len(data) > 0:
            return data
        else:
            return None
    except:
        warning_msg('Serial command write and read exception:')
        warning_msg(sys.exc_info())
        return None

# Write serial data for the given command and data.
# Start, end as well as the length and checksum are added automatically.
def send_command(command, data, ser, expect_reply=True):
    start = b'\x07\xF0'
    end = b'\x07\x0F'
    if data is None:
        length = b'\x00'
        command_plus_data = command + length
    else:
        length_int = len(data)
        length = length_int.to_bytes(((length_int.bit_length() + 8) // 8), byteorder='big')[-1:]
        command_plus_data = command + length + data

    checksum = calculate_checksum(command_plus_data)

    cmd = start + command_plus_data + checksum + end

    result = serial_command(cmd, ser)

    if expect_reply:
        if result:
            # Increment the command by 1 to get the expected result command for RS232
            result_command_int = int.from_bytes(command, byteorder='big') + 1
            result_command = result_command_int.to_bytes(2, byteorder='big')
            filtered_result = filter_and_validate(result, result_command)
            if filtered_result:
                ser.write(b'\x07\xF3')  # Send an ACK after receiving the correct result
                return filtered_result
    else:
        # TODO: Maybe check if there was an "ACK", but given the noise on the serial bus, not sure if that makes sense.
        return True

    return None        

# Calculate the checksum for a given byte string received from the serial connection.
# The checksum is calculated by adding all bytes (excluding start and end) plus 173.
# If the value 0x07 appears twice in the data area, only one 0x07 is used for the checksum calculation.
# If the checksum is greater than one byte, the least significant byte is used.
def calculate_checksum(serial_data_slice):
    checksum = 173
    seven_encountered = False

    for byte in serial_data_slice:
        if byte == 0x07:
            if not seven_encountered:
                seven_encountered = True  # Mark that we have encountered the first 0x07
            else:
                seven_encountered = False # Next one will be counted again
                continue  # Skip the seconds 0x07

        checksum += int(byte)

    return checksum.to_bytes(((checksum.bit_length() + 8) // 8), byteorder='big')[-1:]

# Split the data at \x07\f0 (start) or \x07\xf3 (ACK)
def split_result(data):
    split_data = []
    line = b''

    for index in range(len(data)):
        byte = data[index:index+1]
        nextbyte = data[index+1:index+2]
        if index > 0 and len(data) > index+2 and (byte == b'\x07' and nextbyte == b'\xf0' or byte == b'\x07' and nextbyte == b'\xf3'):
            split_data.append(line)
            line = b''
        line += byte

    split_data.append(line)
    return split_data

# Calculate the length for a given byte string received from the serial connection.
# If the value 0x07 appears twice in the data area, only one 0x07 is used for the checksum calculation.
def calculate_length(serial_data_slice):
    length = 0
    seven_encountered = False

    for byte in serial_data_slice:
        if byte == 0x07:
            if not seven_encountered:
                seven_encountered = True  # Mark that we have encountered the first 0x07
            else:
                seven_encountered = False # Next one will be counted again
                continue  # Skip the seconds 0x07

        length += 1

    return length.to_bytes(1, byteorder='big')    

# Get the checksum from the serial data (third to last byte)
def get_returned_checksum(serial_data):
    return serial_data[-3:-2]

# Filter the data from the serial connection to find the output we're looking for.
# The serial connection is sometimes busy with input/output from other devices (e. g. ComfoSense).
# Then, validate the checksum for the output we're looking for.
# Currently, the data returned is passed as a string, so we'll need to convert it back to bytes for easier handling.
def filter_and_validate(data, result_command):
    split_data = split_result(data)

    for line in split_data:
        if not (len(line) == 2 and line[0] == b'\x07' and line[1] == b'\xf3'):  # Check if it's not an ACK
            if (
                    len(line) >= 7 and
                    line[0:2] == b'\x07\xf0' and  # correct start
                    line[-2:] == b'\x07\x0f' and  # correct end
                    line[2:4] == result_command[0:2] # is it the return we're looking for
            ):
                # Validate length of data
                line_length = calculate_length(line[5:-3])  # Strip start, command, length, checksum and end
                if line[4:5] != line_length:
                    warning_msg('Incorrect length')
                    return None

                # Validate checksum
                returned_checksum = get_returned_checksum(line)
                calculated_checksum = calculate_checksum(line[2:-3])  # Strip start, checksum and end
                if returned_checksum != calculated_checksum:
                    warning_msg('Incorrect checksum')
                    return None

                return line[5:-3]  # Only return data, no start, end, length and checksum

    warning_msg('Expected return not found')
    return None

def process_data(data):
    result = []
    index = 0
    try:
        while index < len(data):
            if data[index] == 0x07:    
                index = index +1
                if data[index] == 0xf0:
                    index = index + 1
                    command = (data[index] << 8) + data[index + 1]
                    index = index + 2
                    l = data[index]
                    index = index + 1
                    package_data = data[index:index + l]
                    index = index + l
                    #checksum
                    index = index + 1
                    #package["end"] = data[index: index + 2]
                    index = index + 2
                    result.append(Command(command, package_data))
            else:
                index = index + 1
    except IndexError as e:
        print(f"index error {e}")
    return result


def print_commands(packages):
    for p in packages:
        print(f"Command {p.command_name}; data: {''.join(format(x, '02x') for x in p.data)}")


def process_temperatures(data, mqtt_client):
    if len(data) > 4:
        comfort_temp = data[0] / 2.0 - 20
        outside_air_temp = data[1] / 2.0 - 20
        supply_air_Temp = data[2] / 2.0 - 20
        return_air_temp = data[3] / 2.0 - 20
        exhaust_air_temp = data[4] / 2.0 - 20    
        ewt_temp = data[6] / 2.0 - 20
        print(f"outside temperature: {outside_air_temp}")
        publish_message(mqtt_client, msg=str(comfort_temp), mqtt_path=f"{ha_mqtt_topic}/comforttemp")
        publish_message(mqtt_client,msg=str(outside_air_temp), mqtt_path=f"{ha_mqtt_topic}/outsidetemp")
        publish_message(mqtt_client,msg=str(supply_air_Temp), mqtt_path=f"{ha_mqtt_topic}/supplytemp")
        publish_message(mqtt_client,msg=str(exhaust_air_temp), mqtt_path=f"{ha_mqtt_topic}/exhausttemp")
        publish_message(mqtt_client,msg=str(return_air_temp), mqtt_path=f"{ha_mqtt_topic}/returntemp")
        publish_message(mqtt_client,msg=str(ewt_temp), mqtt_path=f"{ha_mqtt_topic}/ewttemp")


def process_operating_hours(data, mqtt_client):
    if len(data) > 16:
        filter_hours = int.from_bytes(data[15:17], byteorder='big')
        publish_message(mqtt_client, msg=str(filter_hours), mqtt_path=f"{ha_mqtt_topic}/filterhours")

def process_delays(data, mqtt_client):
    if data is None:
        warning_msg('process_delays could not get serial data')
    else:
        if len(data) > 4:
            FilterWeeks = data[4]
            publish_message(mqtt_client, msg=str(FilterWeeks), mqtt_path=f"{ha_mqtt_topic}/filterweeks")
        else:
            warning_msg('process_delays data array too short')    

def process_errors(data, mqtt_client):
    if data is None:
        warning_msg('get_filter_status function could not get serial data')
    else:
        if len(data) > 16:
            
            if data[8] == 0:
                filter_status = 'Ok'
                filter_status_binary = 'OFF'
            elif data[8] == 1:
                filter_status = 'Full'
                filter_status_binary = 'ON'
            else:
                filter_status = 'Unknown'
                filter_status_binary = 'OFF'
            publish_message(mqtt_client, msg=str(filter_status), mqtt_path=f"{ha_mqtt_topic}/filterstatus")
            publish_message(mqtt_client, msg=str(filter_status_binary), mqtt_path=f"{ha_mqtt_topic}/filterstatus_binary")
            debug_msg('FilterStatus: {0}'.format(filter_status))
        else:
            warning_msg('process_errors data array too short')

def process_ventilation(data, mqtt_client):
    if data is None:
        warning_msg('get_ventilation_status function could not get serial data')
    else:
        if len(data) > 9:
            return_air_level = data[6]
            supply_air_level = data[7]
            fan_level = data[8]
            intake_fan_active = data[9]

            if intake_fan_active == 1:
                intake_fan_active_str = 'Yes'
            elif intake_fan_active == 0:
                intake_fan_active_str = 'No'
            else:
                intake_fan_active_str = 'Unknown'

            debug_msg('ReturnAirLevel: {}, SupplyAirLevel: {}, FanLevel: {}, IntakeFanActive: {}'.format(return_air_level, supply_air_level, fan_level, intake_fan_active_str))

            if fan_level == 1:
                publish_message(mqtt_client, msg='off', mqtt_path=f"{ha_mqtt_topic}/ha_climate_mode")
                publish_message(mqtt_client, msg='off', mqtt_path=f"{ha_mqtt_topic}/ha_climate_mode/fan")
            elif fan_level == 2 or fan_level == 3 or fan_level == 4:
                publish_message(mqtt_client, msg='fan_only', mqtt_path=f"{ha_mqtt_topic}/ha_climate_mode")
                if fan_level == 2:
                  publish_message(mqtt_client, msg='low', mqtt_path=f"{ha_mqtt_topic}/ha_climate_mode/fan")
                elif fan_level == 3:
                  publish_message(mqtt_client, msg='medium', mqtt_path=f"{ha_mqtt_topic}/ha_climate_mode/fan")
                elif fan_level == 4:
                  publish_message(mqtt_client, msg='high', mqtt_path=f"{ha_mqtt_topic}/ha_climate_mode/fan")
            else:
                warning_msg('Wrong FanLevel value: {0}'.format(fan_level))
        else:
            warning_msg('process_ventilation function: incorrect data received')    

def process_fan_status(data, mqtt_client):
    if len(data) > 5:
        intake_fan_speed  = data[0]
        exhaust_fan_speed = data[1]
        if intake_fan_speed != 0 and int.from_bytes(data[2:4], byteorder='big') != 0:
            intake_fan_RPM    = int(1875000 / int.from_bytes(data[2:4], byteorder='big'))
        else:
            intake_fan_RPM    = 0
        if exhaust_fan_speed != 0 and int.from_bytes(data[4:6], byteorder='big') != 0:
            exhaust_fan_RPM   = int(1875000 / int.from_bytes(data[4:6], byteorder='big'))
        else:
            exhaust_fan_RPM   = 0

        publish_message(mqtt_client, msg=str(intake_fan_speed), mqtt_path=f"{ha_mqtt_topic}/intakefanspeed")
        publish_message(mqtt_client, msg=str(exhaust_fan_speed), mqtt_path=f"{ha_mqtt_topic}/exhaustfanspeed")
        publish_message(mqtt_client, msg=str(intake_fan_RPM), mqtt_path=f"{ha_mqtt_topic}/intakefanrpm")
        publish_message(mqtt_client, msg=str(exhaust_fan_RPM), mqtt_path=f"{ha_mqtt_topic}/exhaustfanrpm")
        debug_msg('IntakeFanSpeed {0}%, ExhaustFanSpeed {1}%, IntakeAirRPM {2}, ExhaustAirRPM {3}'.format(intake_fan_speed, exhaust_fan_speed, intake_fan_RPM, exhaust_fan_RPM))
    else:
        warning_msg('function get_fan_status data array too short')

def process_command(command, mqtt_client):
    if command.command == READ_TEMPERATURES:
        process_temperatures(command.data, mqtt_client)
    elif command.command == READ_OPERATING_HOURS:
        process_operating_hours(command.data, mqtt_client)
    elif command.command == READ_DELAYS:
        process_delays(command.data, mqtt_client)
    elif command.command == READ_ERRORS:
        process_errors(command.data, mqtt_client)
    elif command.command == READ_VENTILATION:
        process_ventilation(command.data, mqtt_client)
    elif command.command == READ_FAN_STATUS:
        process_fan_status(command.data, mqtt_client)


def get_fan_status(mqtt_client, ser):
    data = send_command(b'\x00\x0B', None, ser)

    if data is None:
        warning_msg('function get_fan_status could not get serial data')
    else:
        if len(data) > 5:
            intake_fan_speed  = data[0]
            exhaust_fan_speed = data[1]
            if intake_fan_speed != 0 and int.from_bytes(data[2:4], byteorder='big') != 0:
                intake_fan_RPM    = int(1875000 / int.from_bytes(data[2:4], byteorder='big'))
            else:
                intake_fan_RPM    = 0
            if exhaust_fan_speed != 0 and int.from_bytes(data[4:6], byteorder='big') != 0:
                exhaust_fan_RPM   = int(1875000 / int.from_bytes(data[4:6], byteorder='big'))
            else:
                exhaust_fan_RPM   = 0

            publish_message(mqtt_client, msg=str(intake_fan_speed), mqtt_path=f"{ha_mqtt_topic}/intakefanspeed")
            publish_message(mqtt_client, msg=str(exhaust_fan_speed), mqtt_path=f"{ha_mqtt_topic}/exhaustfanspeed")
            publish_message(mqtt_client, msg=str(intake_fan_RPM), mqtt_path=f"{ha_mqtt_topic}/intakefanrpm")
            publish_message(mqtt_client, msg=str(exhaust_fan_RPM), mqtt_path=f"{ha_mqtt_topic}/exhaustfanrpm")
            debug_msg('IntakeFanSpeed {0}%, ExhaustFanSpeed {1}%, IntakeAirRPM {2}, ExhaustAirRPM {3}'.format(intake_fan_speed, exhaust_fan_speed, intake_fan_RPM, exhaust_fan_RPM))
        else:
            warning_msg('function get_fan_status data array too short')

def filter_commands(commands):
    result = {}
    for c in commands:
        result[c.command] = c
    return list(result.values())

def publish_message(mqtt_client, msg, mqtt_path):
    try:
        mqtt_client.publish(mqtt_path, payload=msg, qos=0, retain=True)
    except:
        warning_msg('Publishing message '+msg+' to topic '+mqtt_path+' failed.')
        warning_msg('Exception information:')
        warning_msg(sys.exc_info())
    else:
        time.sleep(0.1)
        debug_msg('published message {0} on topic {1} at {2}'.format(msg, mqtt_path, time.asctime(time.localtime(time.time()))))

def delete_message(mqtt_client, mqtt_path):
    try:
        mqtt_client.publish(mqtt_path, payload="", qos=0, retain=False)
    except:
        warning_msg('Deleting topic ' + mqtt_path + ' failed.')
        warning_msg('Exception information:')
        warning_msg(sys.exc_info())
    else:
        time.sleep(0.1)
        debug_msg('delete topic {0} at {1}'.format(mqtt_path, time.asctime(time.localtime(time.time()))))

def send_autodiscover(mqtt_client, name, entity_id, entity_type, state_topic = None, device_class = None, unit_of_measurement = None, icon = None, attributes = {}, command_topic = None, min_value = None, max_value = None):
    mqtt_config_topic = "homeassistant/" + entity_type + "/" + entity_id + "/config"
    sensor_unique_id = ha_auto_discovery_device_id + "-" + entity_id

    discovery_message = {
        "name": name,
        "has_entity_name": True,
        "availability_topic":f"{ha_mqtt_topic}/status",
        "payload_available":"online",
        "payload_not_available":"offline",
        "unique_id": sensor_unique_id,
        "device": {
            "identifiers":[
                ha_auto_discovery_device_id
            ],
            "name": ha_auto_discovery_device_name,
            "manufacturer": ha_auto_discovery_device_manufacturer,
            "model": ha_auto_discovery_device_model
        }
    }
    if state_topic:
        discovery_message["state_topic"] = state_topic
        
    if command_topic:
        discovery_message["command_topic"] = command_topic
        
    if unit_of_measurement:
        discovery_message["unit_of_measurement"] = unit_of_measurement

    if device_class:
        discovery_message["device_class"] = device_class

    if icon:
        discovery_message["icon"] = icon
    if min_value:
        discovery_message["min"] = min_value
    if max_value:
        discovery_message["max"] = max_value
        
    if len(attributes) > 0:
        for attribute_key, attribute_value in attributes.items():
            discovery_message[attribute_key] = attribute_value

    mqtt_message = json.dumps(discovery_message)
    
    debug_msg('Sending autodiscover for ' + mqtt_config_topic)
    publish_message(mqtt_client, mqtt_message, mqtt_config_topic)

def on_connect(client, userdata, flags, rc):
    publish_message(client, "online",f"{ha_mqtt_topic}/status")
    if ha_enable_auto_discovery_sensors:
        info_msg('Home Assistant MQTT Autodiscovery Topic Set: homeassistant/sensor/ca350_[nametemp]/config')

        # Temperature readings
        send_autodiscover(client,
            name="Outside temperature", entity_id=f"{ha_auto_discovery_device_id}_outsidetemp", entity_type="sensor",
            state_topic=f"{ha_mqtt_topic}/outsidetemp", device_class="temperature", unit_of_measurement="°C"
        )
        send_autodiscover(client,
            name="Supply temperature", entity_id=f"{ha_auto_discovery_device_id}_supplytemp", entity_type="sensor",
            state_topic=f"{ha_mqtt_topic}/supplytemp", device_class="temperature", unit_of_measurement="°C"
        )
        send_autodiscover(client,
            name="Exhaust temperature", entity_id=f"{ha_auto_discovery_device_id}_exhausttemp", entity_type="sensor",
            state_topic=f"{ha_mqtt_topic}/exhausttemp", device_class="temperature", unit_of_measurement="°C"
        )
        send_autodiscover(client,
            name="Return temperature", entity_id=f"{ha_auto_discovery_device_id}_returntemp", entity_type="sensor",
            state_topic=f"{ha_mqtt_topic}/returntemp", device_class="temperature", unit_of_measurement="°C"
        )
        # Fan speeds
        send_autodiscover(client,
            name="Supply fan speed", entity_id=f"{ha_auto_discovery_device_id}_fan_speed_supply", entity_type="sensor",
            state_topic=f"{ha_mqtt_topic}/intakefanrpm", unit_of_measurement="rpm", icon="mdi:fan"
        )
        send_autodiscover(client,
            name="Exhaust fan speed", entity_id=f"{ha_auto_discovery_device_id}_fan_speed_exhaust", entity_type="sensor",
            state_topic=f"{ha_mqtt_topic}/exhaustfanrpm", unit_of_measurement="rpm", icon="mdi:fan"
        )

        send_autodiscover(client,
            name="Supply air level", entity_id=f"{ha_auto_discovery_device_id}_supply_air_level", entity_type="sensor",
            state_topic=f"{ha_mqtt_topic}/intakefanspeed", unit_of_measurement="%", icon="mdi:fan"
        )
        send_autodiscover(client,
            name="Return air level", entity_id=f"{ha_auto_discovery_device_id}_return_air_level", entity_type="sensor",
            state_topic=f"{ha_mqtt_topic}/exhaustfanspeed", unit_of_measurement="%", icon="mdi:fan"
        )        
        # Filter
        send_autodiscover(client,
            name="Filter status", entity_id=f"{ha_auto_discovery_device_id}_filterstatus", entity_type="binary_sensor",
            state_topic=f"{ha_mqtt_topic}/filterstatus_binary", device_class="problem", icon="mdi:air-filter"
        )
        send_autodiscover(client,
            name="Filter Weeks", entity_id=f"{ha_auto_discovery_device_id}_filter_weeks", entity_type="sensor",
            state_topic=f"{ha_mqtt_topic}/filterweeks", unit_of_measurement="weeks", icon="mdi:air-filter"
        )        
        send_autodiscover(client,
            name="Filter Hours", entity_id=f"{ha_auto_discovery_device_id}_filter_hours", entity_type="sensor",
            state_topic=f"{ha_mqtt_topic}/filterhours", unit_of_measurement="h", icon="mdi:timer"
        )        
    else:
        delete_message(client, f"homeassistant/sensor/{ha_auto_discovery_device_id}_outsidetemp/config")
        delete_message(client, f"homeassistant/sensor/{ha_auto_discovery_device_id}_supplytemp/config")
        delete_message(client, f"homeassistant/sensor/{ha_auto_discovery_device_id}_exhausttemp/config")
        delete_message(client, f"homeassistant/sensor/{ha_auto_discovery_device_id}_returntemp/config")

        delete_message(f"homeassistant/sensor/{ha_auto_discovery_device_id}_fan_speed_supply/config")
        delete_message(f"homeassistant/sensor/{ha_auto_discovery_device_id}_fan_speed_exhaust/config")
        delete_message(f"homeassistant/sensor/{ha_auto_discovery_device_id}_return_air_level/config")
        delete_message(f"homeassistant/sensor/{ha_auto_discovery_device_id}_supply_air_level/config")        

        delete_message(client, f"homeassistant/binary_sensor/{ha_auto_discovery_device_id}_filterstatus/config")
        delete_message(client, f"homeassistant/number/{ha_auto_discovery_device_id}_filter_weeks/config")        
        delete_message(client, f"homeassistant/sensor/{ha_auto_discovery_device_id}_filter_hours/config")

    if ha_enable_auto_discovery_climate:
        info_msg('Home Assistant MQTT Autodiscovery Topic Set: homeassistant/climate/ca200_climate/config')
        send_autodiscover(client,
            name="Climate", entity_id=f"{ha_auto_discovery_device_id}_climate", entity_type="climate",
            attributes={
                "temperature_state_topic":f"{ha_mqtt_topic}/comforttemp",
                "current_temperature_topic":f"{ha_mqtt_topic}/supplytemp",
                "min_temp":"15",
                "max_temp":"27",
                "temp_step":"1",
                "modes":["off", "fan_only"],
                "mode_state_topic":f"{ha_mqtt_topic}/ha_climate_mode",
                "mode_command_topic":f"{ha_mqtt_topic}/ha_climate_mode/set",
                "fan_modes":["off", "low", "medium", "high"],
                "fan_mode_state_topic":f"{ha_mqtt_topic}/ha_climate_mode/fan",
                "temperature_unit":"C"
            }
        )
    else:
        delete_message(client, f"homeassistant/climate/{ha_auto_discovery_device_id}_climate/config")


def on_disconnect(client, userdata, rc):
    if rc != 0:
        warning_msg('Unexpected disconnection from MQTT, trying to reconnect')
        recon(client)

def on_message(client, userdata, message):
    msg_data = str(message.payload.decode("utf-8"))
    print(f"mqtt message {msg_data}")

def recon(mqtt_client):
    try:
        mqtt_client.reconnect()
        info_msg('Successfull reconnected to the MQTT server')
        topic_subscribe()
    except:
        warning_msg('Could not reconnect to the MQTT server. Trying again in 10 seconds')
        time.sleep(10)
        recon()

def main():

    # Connect to the MQTT broker
    mqttc = mqtt.Client(ha_auto_discovery_device_id)
    if  mqtt_user != False and mqtt_password != False :
        mqttc.username_pw_set(mqtt_user, mqtt_password)

    # Define the mqtt callbacks
    mqttc.on_connect = on_connect
    mqttc.on_message = on_message
    mqttc.on_disconnect = on_disconnect
    mqttc.will_set(f"{ha_mqtt_topic}/status",payload="offline", qos=0, retain=True)    

    # Connect to the MQTT server
    while True:
        try:
            mqttc.connect(mqtt_server, mqtt_port, mqtt_keep_alive)
            break
        except:
            warning_msg('Can\'t connect to MQTT broker. Retrying in 10 seconds.')
            time.sleep(10)
            pass


    try:
        ser = serial.Serial(port = serial_port, baudrate = 9600, bytesize = serial.EIGHTBITS, parity = serial.PARITY_NONE, stopbits = serial.STOPBITS_ONE)
    except:
        warning_msg('Opening serial port exception:')
        warning_msg(sys.exc_info())
    else:    
        mqttc.loop_start()        
        while True:
            try:
                data = read_serial(ser)
                if data is not None:
                    commands=filter_commands(process_data(data))
                    print_commands(commands)
                    for c in commands:
                        process_command(c, mqttc)
                    
                    get_fan_status(mqttc, ser)

                else:
                    print(".")
                time.sleep(refresh_interval)
                pass
            except KeyboardInterrupt:
                mqttc.loop_stop()                
                ser.close()
                break    

if __name__ == "__main__":
    print("Hello from Comfoair!")
    main()