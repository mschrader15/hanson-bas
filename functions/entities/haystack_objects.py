

class Device:
    """
    This class emulates an equipment item in SkySpark
    """

    def __init__(self, name, ip_address, measurement_names, units, multipliers, equip_markers=None,
                 measurement_markers=None,):
        """
        Create a device instance

        :param name: device name
        :param ip_address: the ip address of BAS to poll for xml data
        :param measurement_names: a list of names of the measurements assigned to the devices
        :param units: a list of the units of the devices
        :param multipliers: a list of the multipliers of the devices
        """
        self.name = name
        self.ip_address = ip_address
        # this call instantiates a dictionary of measurements
        measurement_markers = measurement_markers if measurement_markers else [None] * len(measurement_names)
        self.measurements = {vals[0]: _Measurement(name, vals[0], vals[1], vals[2], vals[3])
                             for vals in zip(measurement_names, units, multipliers, measurement_markers)}
        self.measurement_names = measurement_names
        self.tags = equip_markers

    def get_markers(self):
        tags = self.tags.split(',')
        tags = [tag.strip() for tag in tags]
        return tags


class _Measurement:
    """
    Emulates a measurement point in SkySpark. Stores the measurement time and value
    """
    def __init__(self, device_name, name, units, multipliers, markers):
        """
        Creates a Measurement instance

        :param device_name: the name of the device that it is nested under
        :param name: the name of the measurement
        :param units: the units of the measurement
        :param multipliers: the multiplier of the measurement
        """
        self.name = name
        self.value = None
        self.time = None
        self.units = units
        self.multiplier = multipliers
        self.skyspark_name = "_".join([device_name, name])
        self.markers = markers

    def get_markers(self):
        markers = self.markers.split(',')
        markers = [tag.strip() for tag in markers]
        return markers
