from pyhaystack.util import filterbuilder as fb
import pyhaystack


class SkySpark:

    def __init__(self, login_info):
        self.session = pyhaystack.connect(implementation='skyspark',
                                          uri=login_info['uri'],
                                          username=login_info['username'],
                                          password=login_info['password'],
                                          project=login_info['project'],
                                          pint=True)
        self._site = self.session.site
        self._equip = None
        self._equip_name = None

    def _set_equip(self, equip_name):
        self._equip = self._site[equip_name]
        self._equip_name = equip_name

    def _get_point(self, point_name):
        point_list = list(self._equip.find_entity(fb.Field('navName') == fb.Scalar(point_name)).result.values())
        if len(point_list) > 1:
            print("More than one value was returned for a point name of ", point_name)
        elif len(point_list) < 1:
            # catches if the point doesn't exist on the point
            print("the point name doesn't exist on the equipment ", point_name)
            raise AttributeError
        return point_list[0]

    def _write_point(self, point, time, value):
        res = self.session.his_write(point.id, {self._site.tz.localize(time): value})
        # do something with res write

    def write_point_val(self, equip_name, point_name, time, value):
        # only fetch the equipment if it is different than the last point write
        if equip_name != self._equip_name:
            self._set_equip(equip_name)
        try:
            point = self._get_point(point_name)
        except AttributeError:
            print('Point wasnt written: ', equip_name, point_name)
            return 0.
        self._write_point(point, time, value)