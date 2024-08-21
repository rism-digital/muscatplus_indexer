import csv

import pysolr

c = pysolr.Solr("http://localhost:8983/solr/muscatplus_live")
fq = ["type:incipit", "normalized_fingerprint_lp:*"]
fl = ["id", "normalized_fingerprint_lp"]

result = c.search("*:*", fq=fq, fl=fl, rows=1000, cursorMark="*", sort="id asc")
headers = ["id", "fingerprint"]

with open("incipits.csv", "w", newline="") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=headers)
    writer.writeheader()

    for incipit in result:
        d = {"id": incipit["id"], "fingerprint": incipit["normalized_fingerprint_lp"]}
        writer.writerow(d)
