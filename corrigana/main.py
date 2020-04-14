import logging
import sys
import argparse
from tabulate import tabulate
from mdbagent import mdbAvailable
from quadraenv import QuadraSetEnv
from corrigana import collectNoAnalytique, fixNoAnalytique


parser = argparse.ArgumentParser()
parser = argparse.ArgumentParser(description="Correction des centres analytiques manquants")
parser.add_argument("-m", "--mono", help="active mode monoposte", action="store_true")
parser.add_argument("-x", "--yolo", help="désactive sécurité", action="store_true")
parser.add_argument("-t", "--test", help="cherche les écritures, pas de modif", action="store_true")
parser.add_argument("-v", "--verbose", help="mode verbeux", action="store_true")
parser.add_argument("-d", "--dossier", dest="dossier", type=str, required=True, help="Code dossier")

args = parser.parse_args()

if args.verbose:
    loglevel = "DEBUG"
else:
    loglevel = "INFO"

logging.basicConfig(
    filename=f"corrigana.log",
    level=loglevel,
    format="%(asctime)s - %(module)s.%(funcName)s - %(levelname)s - %(message)s",
)

logging.info("----------START----------")

ipl = r"\\srvquadra\qappli\quadra\database\client\quadra.ipl"
if args.mono:
    logging.warning("Mode monoposte active")
    ipl = r"C:\quadra\database\client\quadra.ipl"

qenv = QuadraSetEnv(ipl)
mdbPath = qenv.make_db_path("DC", args.dossier)
logging.info(mdbPath)

if not args.yolo:
    if not mdbAvailable(mdbPath):
        logging.error("Dossier bloque (en mono ou ouvert)")
        sys.exit()

rows = collectNoAnalytique(mdbPath)

if args.test:
    logging.warning("Mode test, pas de correction")
    sys.exit()

outlist = fixNoAnalytique(mdbPath, rows)

logging.info("{} ecritures corrigees".format(len(outlist)))
print("{} ecritures corrigees".format(len(outlist)))
finalTab = "\n" + tabulate(outlist, tablefmt="grid")
logging.debug(finalTab)

# TEST
