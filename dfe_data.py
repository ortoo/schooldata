import requests
import csv
import io
from datetime import datetime

DFE_DATA_URL = 'https://www.compare-school-performance.service.gov.uk/download-school-data'

def get_raw_dfe_data(school):
  payload = {
    'urn': school.urn
  }

  req = requests.get(DFE_DATA_URL, params=payload)
  return req.text

def parse_dfe_data(data):
  out = {}
  csvreader = csv.DictReader(io.StringIO(data))
  for row in csvreader:
    if row['Variable'] == 'OFSTED_INSPDATE':
      out['lastInspection'] = datetime.strptime(row['Value'], '%d/%m/%Y')
    elif row['Variable'] == 'OFSTED_INSPOUTCOME':
      out['outcome'] = int(row['Value'])

  return out

def update_dfe_data(school):
  rawdata = get_raw_dfe_data(school)
  data = parse_dfe_data(rawdata)
  data['reportUrl'] = 'http://reports.ofsted.gov.uk/inspection-reports/find-inspection-report/provider/ELS/' + school.urn
  setattr(school, 'ofsted', data)
  return school
