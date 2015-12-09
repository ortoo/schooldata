import requests
import csv
import io
from datetime import datetime

DFE_DATA_URL = 'http://www.education.gov.uk/cgi-bin/schools/performance/school.pl'

def get_raw_dfe_data(school):
  payload = {
    'download': 'csv',
    'urn': school.urn
  }

  req = requests.get(DFE_DATA_URL, params=payload)
  return req.text

def parse_dfe_data(data):
  out = {}
  csvreader = csv.DictReader(io.StringIO(data))
  for row in csvreader:
    if row['VARIABLE'] == 'INSPDATE':
      out['lastInspection'] = datetime.strptime(row['VALUE'], '%d %B %Y')
    elif row['VARIABLE'] == 'INSPOUTCOME':
      out['outcome'] = int(row['VALUE'])
    elif row['VARIABLE'] == 'REPORTURL':
      out['reportUrl'] = row['VALUE']

  return out

def update_dfe_data(school):
  rawdata = get_raw_dfe_data(school)
  data = parse_dfe_data(rawdata)
  setattr(school, 'ofsted', data)
  return school