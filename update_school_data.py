import governorhub
import logging
import os
import json
import loggly.handlers
from datetime import datetime
from gcloud import pubsub

from dfe_data import update_dfe_data

logging.basicConfig(level=logging.INFO)

# Turn off requests INFO level logging
requests_log = logging.getLogger("requests")
requests_log.setLevel(logging.WARNING)

LOGGLY_TOKEN = os.environ.get('LOGGLY_TOKEN', None)

pubsubClient = pubsub.Client()

if LOGGLY_TOKEN is not None:
  handler = loggly.handlers.HTTPSHandler('https://logs-01.loggly.com/inputs/%s/tag/school-data' % LOGGLY_TOKEN)
  logging.getLogger('').addHandler(handler)


governorhub.connect()
School = governorhub.ModelType('school')

def update_school(school):

  if getattr(school, 'manualData', False):
    logging.warning('School requested that has manual data: %s. Not processing' % school._id)
    return

  update_dfe_data(school)
  setattr(school, 'lastRefreshed', datetime.now())
  school.save()

def listen_for_requests():
  topic = pubsubClient.topic('school-refresh')
  if not topic.exists():
      topic.create()
  subscription = topic.subscription('schooldata')
  if not subscription.exists():
    subscription.create()

  try:
    while True:
      received = subscription.pull()
      for (ackid, message) in received:
        try:
          data = json.loads(str(message.data, 'utf-8'))
          schoolId = data['schoolId']
          try:
            logging.info('Updating ' + schoolId)
            school = School.get(schoolId)
            update_school(school)
            subscription.acknowledge([ackid])
            logging.info('Updated ' + schoolId)
          except Exception as ex:
            logging.error('Error updating data for school: ' + schoolId)
            logging.exception(ex)
        except Exception as ex:
          logging.exception(ex)
  except Exception as ex:
    logging.exception(ex)

if __name__ == '__main__':
  listen_for_requests()
