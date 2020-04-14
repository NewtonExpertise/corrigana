import logging
import pyodbc
import sys
from tabulate import tabulate
from mdbagent import MdbConnect

def collectNoAnalytique(mdbPath):
    """
    Pour rechercher les écritures non-affectées analytiquement
    mdbPath : chemin vers bdd qcompta
    Retourne liste des écritures concernées
    """

    # Liste les lignes type E, classe 6 & 7 sans code analytique
    # JOIN entre les ligne de type E et lignes de type A
    # On ne garde que les lignes dont le champ Centre est vide
    qry_select = """
        SELECT 
        E.NumUniq, E.CodeJournal, E.NumeroCompte, E.Folio, E.LigneFolio, 
        E.PeriodeEcriture, E.JourEcriture,
        E.MontantTenuDebit, E.MontantTenuCredit, E.Libelle
        FROM
        (
        SELECT
        NumUniq, NumeroCompte, CodeJournal, Folio, LigneFolio, PeriodeEcriture, 
        JourEcriture, Libelle, MontantTenuDEbit, MontantTenuCredit, TypeLigne
        FROM Ecritures
        WHERE (NumeroCompte LIKE '6%' OR NumeroCompte LIKE '7%')
        AND TypeLigne='E'
        ) E
        LEFT JOIN
        (
        SELECT CodeJournal, NumeroCompte, Folio, LigneFolio, PeriodeEcriture, 
        JourEcriture, Centre
        FROM Ecritures 
        WHERE TypeLigne='A'
        ) A
        ON E.CodeJournal=A.CodeJournal
        AND E.NumeroCompte=A.NumeroCompte
        AND E.Folio=A.Folio
        AND E.LigneFolio=A.LigneFolio
        AND E.PeriodeEcriture=A.PeriodeEcriture
        AND E.JourEcriture=A.JourEcriture
        WHERE Centre IS NULL
        ORDER BY E.NumUniq;    
    """
    dic = {}
    rows = []

    with MdbConnect(mdbPath) as mdb:

        # Recherches des lignes non-affectées
        rows = mdb.query_namedt(qry_select)
        logging.info("{} lignes non affectees".format(len(rows)))
    
    if rows : 
        for row in rows:
            dic.setdefault(row.PeriodeEcriture, {})
            dic[row.PeriodeEcriture].setdefault(row.CodeJournal, [0,])
            dic[row.PeriodeEcriture][row.CodeJournal][0] += 1

        logstr = "Liste des journaux concernés : "
        for key in dic.keys():
            tableau = (tabulate(dic[key], headers=dic[key].keys(), tablefmt="fancy_grid"))
            logstr += "\n" + key.strftime("%m/%Y") + "\n" + tableau

        print(logstr)

    return rows

def fixNoAnalytique(mdbPath, rows):
    """"
    Pour corriger les lignes dont le centre ana est manquant
    """

    outlist = []

    # Update pour le champ CentreSimple
    qry_update = """
        UPDATE Ecritures
        SET CentreSimple = ?
        WHERE CodeJournal=?
        AND Folio=?
        AND LigneFolio=?
        AND PeriodeEcriture=?
        AND NumLigne=0
    """

    # Insert pour la ligne type A
    qry_insert = """
        INSERT INTO Ecritures 
        (
            NumUniq, CodeJournal, NumeroCompte, Folio, LigneFolio, 
            PeriodeEcriture, JourEcriture,
            MontantTenuDebit, MontantTenuCredit, NumLigne, TypeLigne, 
            Centre, Nature, PrctRepartition, TypeSaisie, MontantAna
        )
        VALUES
        (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        );
    """    

    # requête pour obtenir le dernier UID
    qry_lastuniq = "SELECT MAX(NumUniq) from Ecritures;"

    with MdbConnect(mdbPath) as mdb:
        # Numero dernière ligne
        lastuniq = mdb.query(qry_lastuniq)[0][0]
        nextUniq = lastuniq + 1
        logging.debug(f"Prochain rang dispo : {nextUniq}")

        # Table des affecations analytiques
        affectations = mdb.query("SELECT NumCompte, CodeCentre FROM AffectationAna")        

        for row in rows:
            # Pour chaque ligne à corriger, il y a deux actions à réaliser
            # 1 - sur la même ligne : mettre à jour le champ CentreSimple
            # 2 - ajouter une ligne supplémentaire avec les données analytiques (type A)

            NumUniq = nextUniq
            NumLigne = 1
            TypeLigne = "A"
            Nature = "*"
            PrctRepartition = 100
            TypeSaisie = "P"
            MontantAna = abs(row.MontantTenuDebit - row.MontantTenuCredit)
            Centre = ""
            for compte, codeAna in affectations:
                if compte == row.NumeroCompte:
                    Centre = codeAna
                    break
            
            if not Centre : 
                continue
            else:
                # Paramètres pour l'update
                updValues = [
                    Centre,
                    row.CodeJournal,
                    row.Folio, 
                    row.LigneFolio,
                    row.PeriodeEcriture,
                ]
                # print(updValues)

                # Paramètres pour l'insert
                insValues = [
                    NumUniq,
                    row.CodeJournal,
                    row.NumeroCompte,
                    row.Folio, 
                    row.LigneFolio,
                    row.PeriodeEcriture,
                    row.JourEcriture,
                    row.MontantTenuDebit,
                    row.MontantTenuCredit, 
                    NumLigne,
                    TypeLigne,
                    Centre, 
                    Nature, 
                    PrctRepartition,
                    TypeSaisie,
                    MontantAna                
                ]
                try:
                    mdb.cursor.execute(qry_update, updValues)
                    # outlist.append(updValues)
                except pyodbc.Error:
                    logging.error("erreur sur update {} \n {}".format(insertTxt, sys.exc_info()[1]))

                try:
                    mdb.cursor.execute(qry_insert, insValues)
                    nextUniq += 1
                    outlist.append(insValues)
                except pyodbc.Error:
                    logging.error("erreur sur insert {} \n {}".format(insertTxt, sys.exc_info()[1]))

        return outlist

if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level="DEBUG",
        format="%(asctime)s - %(module)s.%(funcName)s - %(levelname)s - %(message)s",
    )
    mdb = r"C:\Users\nicolas\Documents\Sources\corrigana\geyser.mdb"
    rows = collectNoAnalytique(mdb)
    out = fixNoAnalytique(mdb, rows)
    # for line in out:
    #     print(line)

