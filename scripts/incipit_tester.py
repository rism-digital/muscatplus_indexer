import sys
import pysolr
import ujson

# For using a local version
sys.path.insert(
    0, "/Users/ahankins/Documents/code/rism/Verovio/verovio/bindings/python/"
)
import verovio

# When using a local version
verovio.setDefaultResourcePath(
    "/Users/ahankins/Documents/code/rism/Verovio/verovio/data"
)


c = pysolr.Solr("http://localhost:8983/solr/muscatplus_live")
vrv_tk = verovio.toolkit()
vrv_tk.setInputFrom(verovio.PAE)
vrv_tk.setOptions(
    ujson.dumps(
        {
            "footer": "none",
            "header": "none",
            "breaks": "none",
        }
    )
)
fq = ["type:incipit", "music_incipit_s:*"]
r = c.search(
    "*:*",
    fq=fq,
    rows=1000,
    cursorMark="*",
    fl=["id", "original_pae_sni"],
    sort="id asc",
)

for result in r:
    pae = result.get("original_pae_sni")
    if pae:
        print("processing incipit: ", result.get("id"))
        vrv_tk.loadData(pae)
        res = vrv_tk.renderToPAE()
