import governorhub
import requests
import re
import threading
import queue
from bs4 import BeautifulSoup

SIMILAR_SCHOOL_URL = "http://dashboard.ofsted.gov.uk/similar_schools.php"
SIMILAR_URN_RE = re.compile("URN: (\d+)")
keyStages = {
  2: ['GPS', 'read', 'write', 'maths'],
  4: ['overall', 'english', 'maths', 'science']
}

governorhub.connect()
School = governorhub.ModelType('school')

def get_simschool_content(school, keystage, category):
  # The correlator of for the school on the ofsted pages is the lacode followed by establishment
  # number
  if not (school.lacode and school.establishmentnumber):
    raise ArgumentError("Missing lacode or establishmentnumber for school " + school._id)

  ofstedId = school.lacode + school.establishmentnumber

  payload = {
    'simtable': 'ks%s_sim_schools_%s' % (keystage, category),
    'lst': ofstedId,
    'ks': keystage
  }

  req = requests.get(SIMILAR_SCHOOL_URL, params=payload)
  return req.text

def extract_sim_schools_urns(soup):
  contentDiv = soup.find_all(id="content")[0]
  schoolLinks = contentDiv.find_all("a")
  schoolData = [link.string for link in schoolLinks]
  urns = [SIMILAR_URN_RE.search(data).group(1) for data in schoolData]
  return urns

def get_similar_school_urns(school, keystage, category, resQ):
  content = get_simschool_content(school, keystage, category)
  soup = BeautifulSoup(content)
  urns = extract_sim_schools_urns(soup)
  for urn in urns:
    resQ.put(urn)

def get_all_similar_school_urns(school):
  resQ = queue.Queue()

  # Get the urns from all key stages
  threads = []
  for keystage, cats in keyStages.items():
    for category in cats:
      thread = threading.Thread(target=get_similar_school_urns, args=(school, keystage, category, resQ))
      thread.start()
      threads.append(thread)

  for thread in threads:
    thread.join()

  # Now pull all the urns off the queue
  urns = set()
  while True:
    try:
      urns.add(resQ.get(False))
    except queue.Empty:
      break

  return list(urns)

def get_similar_schools(school):
  urns = get_all_similar_school_urns(school)
  schools = School.query({
    "find": {
      "urn": {
        "$in": urns
      }
    }
  }, ids_only=True)
  return schools

def update_similar_schools(school):
  schools = get_similar_schools(school)
  setattr(school, 'similarSchools', [{'school': schl} for schl in schools])
  return school