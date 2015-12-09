import governorhub
import logging
import redis
import os
import loggly.handlers
from datetime import datetime

from similar_schools import update_similar_schools
from dfe_data import update_dfe_data

logging.basicConfig(level=logging.INFO)

# Turn off requests INFO level logging
requests_log = logging.getLogger("requests")
requests_log.setLevel(logging.WARNING)

REDIS_HOST = os.environ.get('REDIS_HOST', '127.0.0.1')
REDIS_PORT = os.environ.get('REDIS_PORT', 6379)
REDIS_PASSWORD = os.environ.get('REDIS_PASSWORD', None)
SENTINEL_HOST = os.environ.get('SENTINEL_HOST', None)
SENTINEL_PORT = os.environ.get('SENTINEL_PORT', 26379)
SENTINEL_MASTER = os.environ.get('SENTINEL_MASTER', 'base')

LOGGLY_TOKEN = os.environ.get('LOGGLY_TOKEN', None)

UPDATE_CHAN = 'or2:school:updatedata:channel'
UPDATE_Q = 'or2:school:updatedataq'

if LOGGLY_TOKEN is not None:
  handler = loggly.handlers.HTTPSHandler('https://logs-01.loggly.com/inputs/%s/tag/school-data' % LOGGLY_TOKEN)
  logging.getLogger('').addHandler(handler)


governorhub.connect()
School = governorhub.ModelType('school')

def update_school(school):

  if getattr(school, 'manualData', False):
    logging.warning('School requested that has manual data: %s. Not processing' % school._id)
    return

  update_similar_schools(school)
  update_dfe_data(school)
  setattr(school, 'lastRefreshed', datetime.now())
  school.save()

def clear_queue(client):
  while True:
    try:
      schoolId = client.lpop(UPDATE_Q)
      if schoolId is None:
        break

      schoolId = schoolId.decode('utf-8')

      try:
        logging.info('Updating ' + schoolId)
        school = School.get(schoolId)
        update_school(school)
        logging.info('Updated ' + schoolId)
      except Exception as ex:
        logging.error('Error updating data for school: ' + schoolId)
        logging.exception(ex)
    except Exception as ex:
      logging.exception(ex)

def listen_for_requests():
  if SENTINEL_HOST is None:
    client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD)
  else:
    sentinel = Sentinel([(SENTINEL_HOST, SENTINEL_PORT)])
    client = sentinel.master_for(SENTINEL_MASTER)

  clear_queue(client)

  ps = client.pubsub()
  ps.subscribe(UPDATE_CHAN)

  # Hang until we get a message
  try:
    for message in ps.listen():
      try:
        if message['type'] == 'message':
          data = message['data'].decode('utf-8')
          if data == 'update':
            clear_queue(client)
      except Exception as ex:
        logging.exception(ex)

  finally:
    ps.close()

if __name__ == '__main__':
  listen_for_requests()
