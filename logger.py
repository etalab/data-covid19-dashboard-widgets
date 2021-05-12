import logging
import sys

log = logging.getLogger("covid-backend")
log.setLevel(getattr(logging, "DEBUG"))
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
handler.setFormatter(formatter)
log.addHandler(handler)
