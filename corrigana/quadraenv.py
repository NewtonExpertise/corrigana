import logging
import os
import sys
import pyodbc
# import exclusions


class QuadraSetEnv(object):
    def __init__(self, ipl_file):

        self.cpta = ""
        self.paie = ""
        self.gi = ""
        self.conn = ""
        self.cur = ""

        with open(ipl_file, "r") as f:
            lines = f.readlines()

        for line in lines:
            line = line.rstrip().replace("\\", "/")
            if "=" in line:
                key, item = line.split("=")[0:2]
                if key == "RACDATACPTA":
                    self.cpta = item.upper()
                elif key == "RACDATAPAIE":
                    self.paie = item.upper()
                elif key == "RACDATAGI":
                    self.gi = item.upper()
        if self.cpta:
            logging.debug("Acces fichier ipl OK")

    def make_db_path(self, type_dossier, num_dossier):
        type_dossier = type_dossier.upper()
        num_dossier = num_dossier.upper()
        db_path = ""
        if (
            type_dossier == "DC"
            or type_dossier.startswith("DA")
            or type_dossier.startswith("DS")
        ):
            db_path = os.path.join(self.cpta, type_dossier, num_dossier, "qcompta.mdb")

        elif type_dossier == "PAIE":
            db_path = os.path.join(self.paie, num_dossier, "qpaie.mdb")
        return os.path.abspath(db_path)

    def chemins_cpta(self, categ="D", tail=""):
        """
        Renvoie la liste des chemin menant vers les dossiers comptables découverts
        dans le dossier database/cpta (dc, archives, situations)
        tail : permet de rajouter un nom de fichier/dossier à la fin du chemin
        """
        liste = []
        with os.scandir(self.cpta) as it1:
            for entry in it1:
                name = entry.name.upper()
                if name.startswith(categ) and entry.is_dir():
                    base = os.path.join(self.cpta, name)
                    with os.scandir(base) as it2:
                        for entry in it2:
                            if entry.is_dir():
                                final = os.path.abspath(os.path.join(base, entry.name))
                                liste.append(final)

        if tail:
            liste_chemins_ = [os.path.join(x, tail) for x in liste]
            liste_chemins = [x for x in liste_chemins_ if os.path.isfile(x)]
        else:
            liste_chemins = liste

        return liste_chemins
    
    def chemins_paie(self, bannis=[]):
        """
        Renvoi la liste des chemins vers les bases paie
        bannis = permet d'ajouter une liste de dossiers exclus
        """
        liste = []
        with os.scandir(self.paie) as itr:
            dossList = [x.name for x in itr]
        accepted = [x for x in dossList if x not in bannis]
        
        for item in accepted:
            qpaie = os.path.abspath(os.path.join(self.paie, item, "qpaie.mdb"))
            if os.path.isfile(qpaie):
                liste.append(qpaie)
        return liste

    def gi_list_clients(self):

        mdb_path = os.path.join(self.gi, "0000", "qgi.mdb")
        constr = "Driver={Microsoft Access Driver (*.mdb, *.accdb)};Dbq=" + mdb_path
        logging.debug("openning qgi : {}".format(mdb_path))
        sql = """
            SELECT I.Code, I.Nom 
            FROM Intervenants I 
            INNER JOIN Clients C ON I.Code=C.Code 
            WHERE I.IsClient='1'
            """
        try:
            self.conn = pyodbc.connect(constr, autocommit=True)
            self.cur = self.conn.cursor()

            self.cur.execute(sql)
            data = list(self.cur)
        except pyodbc.Error:
            logging.error(
                ("erreur requete base {} \n {}".format(mdb_path, sys.exc_info()[1]))
            )
            return False

        return data

    def get_rs(self, code_dossier):
        rs = ""
        liste_dossiers = self.gi_list_clients()
        for code, nom in liste_dossiers:
            if code_dossier == code:
                rs = nom
        return rs
