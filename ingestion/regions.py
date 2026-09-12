"""Referentiel des 12 regions administratives du Maroc (ISO 3166-2:MA)."""

from dataclasses import dataclass


@dataclass(frozen=True)
class Region:
    """Une region administrative marocaine et son point de mesure climatique."""

    iso_code: str
    name: str
    capital: str
    latitude: float
    longitude: float
    altitude_m: float


REGIONS: tuple[Region, ...] = (
    Region("MA-01", "Tanger-Tétouan-Al Hoceïma", "Tanger-Assilah", 35.7669, -5.8000, 20),
    Region("MA-02", "L'Oriental", "Oujda", 34.6867, -1.9114, 450),
    Region("MA-03", "Fès-Meknès", "Fès", 34.0500, -4.9831, 400),
    Region("MA-04", "Rabat-Salé-Kénitra", "Rabat", 34.0211, -6.8414, 75),
    Region("MA-05", "Béni Mellal-Khénifra", "Béni Mellal", 32.3394, -6.3608, 500),
    Region("MA-06", "Casablanca-Settat", "Casablanca", 33.5785, -7.6066, 50),
    Region("MA-07", "Marrakech-Safi", "Marrakech", 31.6294, -7.9811, 450),
    Region("MA-08", "Darâa-Tafilalet", "Errachidia", 31.9319, -4.4244, 1009),
    Region("MA-09", "Souss-Massa", "Agadir", 30.4167, -9.6000, 47),
    Region("MA-10", "Guelmim-Oued Noun", "Guelmim", 28.9881, -10.0575, 456),
    Region("MA-11", "Laâyoune-Saguia El Hamra", "Laâyoune", 27.1536, -13.2033, 63),
    # Chef-lieu "Oued-Eddahab" (libelle ISO-strict) ; coordonnees = ville de Dakhla.
    Region("MA-12", "Dakhla-Oued Ed-Dahab", "Oued-Eddahab", 23.7167, -15.9500, 5),
)

_REGIONS_BY_CODE: dict[str, Region] = {region.iso_code: region for region in REGIONS}


def get_region(iso_code: str) -> Region:
    """Retourne la region correspondant a un code ISO (ex: 'MA-04')."""
    try:
        return _REGIONS_BY_CODE[iso_code]
    except KeyError:
        raise KeyError(f"Region inconnue: {iso_code!r}") from None


def iso_codes() -> tuple[str, ...]:
    """Liste des 12 codes ISO, dans l'ordre nord-sud."""
    return tuple(region.iso_code for region in REGIONS)
