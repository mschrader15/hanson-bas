import hszinc
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

    def check_equipment_exists(self, name):
        self._set_equip(name)
        return True if self._equip else False

    def check_measurement_exists(self, name):
        point_list = list(self._site.find_entity(fb.Field('navName') == fb.Scalar(name)).result.values())
        return True if len(point_list) > 0 else False


class SkySparkCreator(SkySpark):

    def __init__(self, login_info):
        super().__init__(login_info)

    def add_equipment(self, name, markers):
        if not self.check_equipment_exists(name):
            g = self._create_equipment_grid(equip_name=name, markers=markers)
            r = self._post_grid(g)
            print(f"{name} did not fail {r.is_failed}")
            # have to refresh the equipment list now
            self._site.refresh()
            return r
        else:
            return None

    def add_measurement(self, equip_name, measurement_name, markers, unit):
        name = "_".join([equip_name, measurement_name])
        if not self.check_measurement_exists(name):
            self._set_equip(equip_name)
            g = self._create_measurement_grid(name=name, markers=markers, unit=unit)
            r = self._post_grid(g)
            print(f"{name} failed? {r.is_failed}")
            return r
        else:
            return None

    def _create_equipment_grid(self, equip_name, markers):
        grid = hszinc.Grid()
        grid.metadata['commit'] = 'add'
        # grid.metadata['projName'] = 'Springfield'
        grid.column['dis'] = {}
        grid.column["equip"] = {}
        grid.column["siteRef"] = {}
        grid.column["navName"] = {}
        #grid.column['disMacro'] = {}
        g = {'dis': equip_name, 'navName': equip_name, 'equip': hszinc.MARKER, "siteRef": self._site.id}
        for marker in markers:
            grid.column[marker] = {}
            g[marker] = hszinc.MARKER
        grid.append(g)
        return grid

    def _create_measurement_grid(self, name, markers, unit):
        grid = hszinc.Grid()
        grid.metadata['commit'] = 'add'
        # grid.column['dis'] = {}
        grid.column['navName'] = {}
        grid.column['disMacro'] = {}
        grid.column["siteRef"] = {}
        grid.column["equipRef"] = {}
        grid.column["tz"] = {}
        g = {'navName': name, 'disMacro': '$equipRef $navName', "siteRef": self._site.id, "equipRef": self._equip.id,
             'unit': unit, 'tz': str(self._site.hs_tz)}
        for marker in markers:
            grid.column[marker] = {}
            g[marker] = hszinc.MARKER
        if unit != None:
            grid.column['unit'] = {}
            g['unit'] = unit
        grid.append(g)
        return grid

    def _post_grid(self, g):
        r = self.session._post_grid(grid=g, callback=None, uri='commit')
        return r



