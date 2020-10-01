import logging
import numpy as np
logging.getLogger().setLevel(logging.INFO)
from pyhaystack.util import filterbuilder as fb
# %%
import pyhaystack
import datetime
from datetime import timezone
session = pyhaystack.connect(implementation='skyspark',
                                uri='http://skyspark.hanson-inc.com:8080',
                                username='administrator',
                                password='IownSkyspark',
                                project='springfieldHanson',
                                pint=True)

#%%
session
op = session.about()
op.wait()
nav = op.result
print(nav)

#%%
site=session.site
my_equip = site['rtu1']
# equip = site.equipments

#%%
# res = my_equip.find_points(fb.Field('navName') == fb.Scalar('rtu1_RACO2')).result
# res.points
point = list(my_equip.find_entity(fb.Field('navName') == fb.Scalar('rtu1_RACO2')).result.values())[0]
res = session.his_write(point.id, {site.tz.localize(datetime.datetime.now()): 0})
print(res.result)