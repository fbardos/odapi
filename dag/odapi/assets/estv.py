"""
Contains sourcing assets from the Eidgenössische Steuerverwaltung (ESTV), per municipality.
    - Natuerliche Personen
    - Juristische Personen

Source:
    - https://www.estv.admin.ch/estv/de/home/die-estv/steuerstatistiken-estv.html
    - https://www.estv.admin.ch/estv/de/home/die-estv/steuerstatistiken-estv/allgemeine-steuerstatistiken/direkte-bundessteuer.html
"""


from dataclasses import dataclass


# Source: Direkte Bundessteuer, natürliche Personen

@dataclass
class DbstNatPersConfig:
    source_url: str

class DbstNatPers2001Config(DbstNatPersConfig):
    source_url = 'https://www.estv.admin.ch/dam/estv/de/dokumente/estv/steuerstatistiken/direkte-bundessteuer/statistik-dbst-np-gemeinde-2001-auswertung-de.xls.download.xls/statistik-dbst-np-gemeinde-2001-auswertung-de.xls'


# DBST_NAT_PERS = [
    # DbstNatPersConfig(
        # source_url=

    # )

# ]
