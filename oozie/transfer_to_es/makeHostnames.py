from __future__ import print_function
import subprocess
import re
import json
import sys
import datetime

# This script produces the JSON map from hostnames - as stored in hive - to wiki dbnames

# Get the list of hostnames
now = datetime.datetime.now()
mon = now.month - 1
year = now.year
if mon == 0:
    mon = 12
    year -= 1
# This uses last months' data, since this month may be just beginning and not have good data yet
hostnames = subprocess.check_output(["hive", "-S", "-e",
    'select distinct project from wmf.discovery_popularity_score \
        where year=%d AND month=%d;' % (year, mon)]).split("\n")
names = {}
# The source file should be updated from:
# https://raw.githubusercontent.com/wikimedia/operations-mediawiki-config/master/dblists/all.dblist
dblist = set(open("all.dblist").read().split())
# The following abomination comes from:
# https://github.com/wikimedia/operations-mediawiki-config/blob/master/multiversion/MWMultiVersion.php#L134
# And should be updated when that changes.
# No, I have no idea how to ensure it's up to date. That's why one shouldn't hardcode such things.
staticMap = {
    'wikimediafoundation': 'foundation',
    'mediawiki': 'mediawiki',
    'wikidata': 'wikidata',
    'wikisource': 'sources',
    'be-tarask.wikipedia': 'be_x_old',
}
chapters = set(['ar', 'bd', 'be', 'br', 'ca', 'cn', 'co', 'dk', 'et', 'fi', 'il', 'mk', 'mx', 'nl',
    'noboard-chapters', 'no', 'nyc', 'nz', 'pa-us', 'pl', 'rs', 'ru', 'se', 'tr', 'ua', 'uk', 've'])
hostmatch = re.compile('^(.*)\\.([a-z]+)$')
for hostname in hostnames:
    if hostname == 'project' or len(hostname) == 0:
        # skip this hive artifact
        continue
    site = "wikipedia"
    if hostname in staticMap:
        lang = staticMap[hostname]
    else:
        m = hostmatch.match(hostname)
        if not m:
            print("BAD HOSTNAME: " + hostname, file=sys.stderr)
            continue
        lang = m.group(1)
        if m.group(2) != 'wikimedia' or lang in chapters:
            site = m.group(2)
    if site == 'wikipedia':
        site = 'wiki'

    dbname = (lang + site).replace("-", "_")
    if dbname not in dblist:
        print("UNKNOWN DBNAME: " + dbname, file=sys.stderr)
        continue
    names[hostname] = dbname

print(json.dumps(names, sort_keys=True, indent=1))
