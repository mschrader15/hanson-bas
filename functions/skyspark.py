import pyhaystack
from pyhaystack.util import filterbuilder as fb


class SkySpark:
    """
    This class is for interfacing with SkySpark.
    """

    def __init__(self, login_info):
        """
        Initializing the SkySpark class creates the SkySpark session (when the parameters are correct, the session
        object is bound to our SkySpark instance)

        :param login_info: a dict containing the login info
        """
        try:
            self.session = pyhaystack.connect(implementation='skyspark',
                                              uri=login_info['uri'],
                                              username=login_info['username'],
                                              password=login_info['password'],
                                              project=login_info['project'],
                                              pint=True)
        except pyhaystack.util.state.NotReadyError as e:
            # could do error handling here
            raise e

        # getting the site from the session. This is the top level item
        self._site = self.session.site
        self._equip = None
        self._equip_name = None

    def _set_equip(self, equip_name):
        """
        This function fetches the equipment object from SkySpark

        :param equip_name: the name of the equipment under the site (2nd level in SkySpark)
        :return: None
        """
        self._equip = self._site[equip_name]
        self._equip_name = equip_name

    def _get_point(self, point_name):
        """
        Fetching the point (measurement) object.

        :param point_name: the name of the measurement point. Needs to be exact
        :return: a measurement object
        """
        point_list = list(self._equip.find_entity(fb.Field('navName') == fb.Scalar(point_name)).result.values())

        # if more than 1 point is returned with the given name, there is an issue. point names must be unique
        if len(point_list) > 1:
            print("More than one value was returned for a point name of ", point_name)

        # catches if the point doesn't exist on the point
        elif len(point_list) < 1:
            print("the point name doesn't exist on the equipment ", point_name)
            raise AttributeError
        return point_list[0]

    def _write_point(self, point, time, value):
        """
        Writing the point as a historical to SkySpark

        :param point: The name of the point
        :param time: The time to write (will be localized by the SkySpark Sites location)
        :param value: the measurement value to write
        :return: None
        """
        res = self.session.his_write(point.id, {self._site.tz.localize(time): value})
        # do something with res write

    def write_point_val(self, equip_name, point_name, time, value):
        """
        The master function in the SkySpark class. It writes the value to the specified measurement point

        :param equip_name: The name of the equipment
        :param point_name: The name of the measurement point
        :param time: The time of the value being written
        :param value: The value to write
        :return: None
        """

        # only fetch the equipment if it is different than the last point write. To save time
        if equip_name != self._equip_name:
            self._set_equip(equip_name)
        try:
            point = self._get_point(point_name)
        except AttributeError:
            print('Point wasnt written: ', equip_name, point_name)
            return 0.
        self._write_point(point, time, value)