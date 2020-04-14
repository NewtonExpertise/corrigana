import pyodbc
import logging
import os
import sys
from datetime import datetime
from collections import namedtuple

def mdbAvailable(mdbpath):
    """
    Vérifie la disponibilité de la base de données avant intervention
    Vérif 1 : la base est-elle déjà ouverte (ldb?)
    Vérif 2 : le dossier a-t-il été transféré en mono
    mdbpath : chemin vers le fichier mdb
    """
    # VERIF 1
    if os.path.isfile(mdbpath):
        root, ext = os.path.splitext(mdbpath)
        ldbpath = f"{root}.ldb"
        if os.path.isfile(ldbpath):
            logging.info("dossier ouvert")
            return False
    # VERIF 2
    with MdbConnect(mdbpath) as mdb:
        mdb.cursor.execute("SELECT DSDateSortie FROM Dossier2")
        sortie = mdb.cursor.fetchall()[0][0]
        if not sortie == datetime(1899, 12, 30):
            logging.info("dossier emporté")
            return False
    return True

class MdbConnect(object):
    """
    Wrapper pour les connexion à quadra avec pyodbc
    mdb_path = chemin complet vers le fichier *.mdb
    """

    def __init__(self, mdb_path):
        self.mdb_path = mdb_path
        self.conx = ""
        self.cursor = ""
        self.rs = ""
        self.code = os.path.normpath(mdb_path).split("\\")[-2]
        self.job = ""
        _, file = os.path.split(mdb_path)
        if file.lower() == "qcompta.mdb":
            self.job = "compta"
        elif file.lower() == "qpaie.mdb":
            self.job = "paie"

    def _connect(self):
        if not os.path.isfile(self.mdb_path):
            logging.error(f"Fichier mdb introuvable")
            return False

        constr = (
            "Driver={Microsoft Access Driver (*.mdb, *.accdb)};Dbq=" + self.mdb_path
        )
        try:
            self.conx = pyodbc.connect(constr, autocommit=True)
            self.cursor = self.conx.cursor()
            logging.debug("Ouverture de {}".format(self.mdb_path))
        except pyodbc.Error:
            logging.error(
                "erreur requete base {} \n {}".format(self.mdb_path, sys.exc_info()[1])
            )
        except:
            logging.error(
                "erreur ouverture base {} \n {}".format(
                    self.mdb_path, sys.exc_info()[0]
                )
            )

    def _close(self):
        logging.debug("fermeture de la connexion à quadra")
        self.conx.commit()
        self.conx.close()

    def __enter__(self):
        """Pour context manager"""
        self._connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._close()

    def query(self, sql):
        data = []
        try:
            self.cursor.execute(sql)
        except pyodbc.Error:
            logging.error("erreur requete base {} \n {}".format(sql, sys.exc_info()[1]))
            return data
        data = self.cursor.fetchall()
        # for row in self.cursor:
        #     data.append(row)
        # data.append([x for x in row])
        return data

    def query_namedt(self, sql):
        data = []
        try:
            self.cursor.execute(sql)
        except pyodbc.Error:
            logging.error("erreur requete base {} \n {}".format(sql, sys.exc_info()[1]))
            return data

        fields = [x[0] for x in self.cursor.description]
        Row = namedtuple("Row", fields)
        for row in self.cursor.fetchall():
            data.append(Row(*row))
        return data

    def queryInfoData(self, sql):
        desc = []
        data = []
        try:
            self.cursor.execute(sql)
        except pyodbc.Error:
            logging.error("erreur requete base {} \n {}".format(sql, sys.exc_info()[1]))
            return data
        data = self.cursor.fetchall()
        desc = [(x[0], x[1]) for x in self.cursor.description]
        return desc, data

    def getrs(self):
        if self.job == "compta":
            sql = "SELECT RaisonSociale FROM Dossier1"
        elif self.job == "paie":
            sql = "SELECT RaisonSociale FROM Etablissements WHERE CodeEtablissement=0"

        self.rs = self.query(sql)[0][0]
        return self.rs

