from simhash import hamming_distance
import csv
import operator

TARGET_FINGERPRINT: int = 2040975891049975274
PAE = "@timesig:c\n@clef:C-3\n@data:4-2'C4C/2D2F/1E/4-2E4E/2D"
"@timesig:c\n@clef:C-1\n@data:1''E2'A2''F1E2-1D2C4F4E4D4E1C"

results = []
with open("incipits.csv", 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        q: int = int(row['fingerprint'])
        d: int = hamming_distance(TARGET_FINGERPRINT, q)
        if d < 1 or q == TARGET_FINGERPRINT:
            r = {"id": row['id'],
                 "dist": d}
            results.append(r)

results.sort(key=operator.itemgetter("dist"))

for r in results:
    print(r)
