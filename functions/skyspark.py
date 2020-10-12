import hszinc
import pyhaystack
from pyhaystack.util import filterbuilder as fb
import pandas as pd

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
        self._point = None
        self._his_frame = None

    def _set_equip(self, equip_name):
        """
        This function fetches the equipment object from SkySpark

        :param equip_name: the name of the equipment under the site (2nd level in SkySpark)
        :return: None
        """
        self._equip = self._site[equip_name]
        self._equip_name = equip_name

    def _set_point(self, point_name):
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
        self._point = point_list[0]

    def _write_point(self, time, value):
        """
        Writing the point as a historical to SkySpark

        :param point: The name of the point
        :param time: The time to write (will be localized by the SkySpark Sites location)
        :param value: the measurement value to write
        :return: None
        """
        value = self._data_type_handler(value)
        res = self.session.his_write(self._point.id, {self._site.tz.localize(time): value})
        return res
        # do something with res write

    def _data_type_handler(self, value):
        if self._point.tags['kind'] == 'Bool':
            on_off = value.lower() in ['on', 'off']
            if on_off:
                value = True if value.lower() == 'on' else False
        elif self._point.tags['kind'] == 'Number':
            try:
                value = float(value)
            except ValueError:
                value = 0
                print(f"{self._point.id} has a data type mismatch")
        return value

    def _check_equip(self, equip_name):
        if equip_name != self._equip_name:
            self._set_equip(equip_name)

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
        self._check_equip(equip_name)
        try:
            self._set_point(point_name)
            res = self._write_point(time, value)
            if res.is_failed:
                print('Error writing point: ', equip_name, point_name)
            return res
        except AttributeError:
            print('Point wasnt written: ', equip_name, point_name)
            return 0.

    def check_equipment_exists(self, name):
        self._check_equip(name)
        return True if self._equip else False

    def check_measurement_exists(self, name):
        point_list = list(self._site.find_entity(fb.Field('navName') == fb.Scalar(name)).result.values())
        if len(point_list) > 0:
            self._point = point_list[0]
            return True
        else:
            return False

    def append_his_frame(self, equip_name, point_name, time, value):
        self._check_equip(equip_name)
        self._set_point(point_name)
        self._create_his_frame()
        self._add_his_value(time, self._data_type_handler(value))

    def _create_his_frame(self):
        if self._his_frame is None:
            self._his_frame = []

    def _add_his_value(self, time, value):
        self._his_frame.append({'id': self._point.id, 'mod': self._point.tags['mod'],
                                'ts': self._site.tz.localize(time), 'val': value})

    def submit_his_frame(self):
        _, r = self._simple_point_write(self._his_frame, 1)
        return r

    # def _threaded_submit(self):
    #     import numpy as np
    #     from concurrent.futures import ThreadPoolExecutor, as_completed
    #     threadnum = 4
    #     split_args = np.array_split(self._his_frame, threadnum)
    #     threads = []
    #     with ThreadPoolExecutor(max_workers=threadnum) as executor:
    #         for num, args in enumerate(split_args):
    #             threads.append(executor.submit(self._simple_point_write, args, num))
    #         results = sorted([res for res in [task.result() for task in as_completed(threads)]], key=lambda x: x[0])
    #     return results

    def _simple_point_write(self, pointlist, num):
        results = []
        for point_dict in pointlist:
            print("writing ", point_dict['id'])
            res = self.session.his_write(point_dict['id'], {point_dict['ts']: point_dict['val']})
            if res.is_failed:
                try:
                    print(point_dict['id'], 'failed ', res.result)
                except pyhaystack.exception.HaystackError as e:
                    print(e)
            else:
                print("completed ", point_dict['id'])
            results.append(res)
        return num, results


class SkySparkCreator(SkySpark):

    def __init__(self, login_info):        super().__init__(login_info)

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

    def add_measurement(self, equip_name, measurement_name, markers, unit, dataType, overwrite=False):
        name = "_".join([equip_name, measurement_name])
        exists = self.check_measurement_exists(name)
        method = 'update' if exists else 'add'
        if (not exists) or overwrite:
            self._set_equip(equip_name)
            g = self._create_measurement_grid(method=method, name=name, markers=markers, unit=unit, kind=dataType,)
            r = self._post_grid(g)
            print(f"{name} failed? {r.is_failed}")
            if r.is_failed:
                print(r.result)
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

    def _create_measurement_grid(self, method, name, markers, unit, kind,):
        grid = hszinc.Grid()
        grid.metadata['commit'] = method
        # grid.column['dis'] = {}
        grid.column['navName'] = {}
        grid.column['disMacro'] = {}
        grid.column["siteRef"] = {}
        grid.column["equipRef"] = {}
        grid.column["tz"] = {}
        grid.column["kind"] = {}
        g = {'navName': name, 'disMacro': '$equipRef $navName', "siteRef": self._site.id, "equipRef": self._equip.id,
             'unit': unit, 'tz': str(self._site.hs_tz), 'kind': kind}
        for marker in markers:
            grid.column[marker] = {}
            g[marker] = hszinc.MARKER
        if unit != None:
            grid.column['unit'] = {}
            g['unit'] = unit
        if method == 'update':
            grid.column['mod'] = {}
            g['mod'] = self._point.tags['mod']
            grid.column['id'] = {}
            g['id'] = self._point.id
        grid.append(g)
        return grid

    def _post_grid(self, g):
        r = self.session._post_grid(grid=g, callback=None, uri='commit')
        return r



