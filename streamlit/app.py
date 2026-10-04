"""Dashboard Streamlit : KPI climat (secheresse) et potentiel solaire par region du Maroc.

Lancer : `make streamlit` (= uv run streamlit run streamlit/app.py), depuis la racine du repo
(la connexion Postgres est lue dans .env). Lit directement les marts dbt
marts.fct_region_climate_kpi et marts.fct_solar_potential (cf. dashboard/data.py).
La logique testable (filtres, agregats, couleurs, export) est dans dashboard/metrics.py.
"""

import pandas as pd
import plotly.express as px
import pydeck as pdk

import streamlit as st
from dashboard import data, metrics
from dashboard.metrics import MapMode

st.set_page_config(page_title="Climat & potentiel solaire — Maroc", layout="wide")

MAP_MODES: dict[str, MapMode] = {
    "Classe de sécheresse": "drought",
    "Potentiel solaire (score 0-100)": "solar",
}


@st.cache_data(ttl=600, show_spinner="Chargement des KPI depuis Postgres…")
def load_kpis() -> pd.DataFrame:
    return data.load_region_year_kpis()


def render_legend(mode: MapMode) -> None:
    if mode == "drought":
        items = [
            f"<span style='color:{metrics.DROUGHT_CLASS_COLORS[key]}'>●</span> "
            f"{metrics.DROUGHT_CLASS_LABELS[key]}"
            for key in metrics.DROUGHT_CLASS_LABELS
        ]
        items.append(
            f"<span style='color:{metrics.NO_DATA_COLOR}'>●</span> {metrics.NO_DATA_LABEL}"
        )
        st.markdown(" &nbsp;&nbsp; ".join(items), unsafe_allow_html=True)
    else:
        st.markdown(
            f"<span style='color:{metrics.SOLAR_RAMP_LOW}'>●</span> score faible → "
            f"<span style='color:{metrics.SOLAR_RAMP_HIGH}'>●</span> score élevé "
            f"&nbsp;&nbsp; <span style='color:{metrics.NO_DATA_COLOR}'>●</span> "
            f"{metrics.NO_DATA_LABEL}",
            unsafe_allow_html=True,
        )


def render_map(view: pd.DataFrame, mode: MapMode) -> None:
    map_data = metrics.build_map_data(view, mode)
    layer = pdk.Layer(
        "ScatterplotLayer",
        data=map_data,
        get_position="[longitude, latitude]",
        get_fill_color="color",
        get_radius=45000,
        pickable=True,
        stroked=True,
        get_line_color=[255, 255, 255],
        line_width_min_pixels=1,
    )
    deck = pdk.Deck(
        layers=[layer],
        initial_view_state=pdk.ViewState(latitude=29.5, longitude=-9.0, zoom=4.3),
        tooltip={"html": "<b>{region_name}</b><br/>{label}"},
    )
    st.pydeck_chart(deck)
    render_legend(mode)


def render_kpi_cards(view: pd.DataFrame, previous: pd.DataFrame) -> None:
    current = metrics.summarize(view)
    before = metrics.summarize(previous)

    def delta_text(key: str, digits: int) -> str | None:
        value = metrics.delta(current[key], before[key])
        return None if value is None else metrics.format_number(value, digits)

    columns = st.columns(4)
    columns[0].metric(
        "SPI simplifié (moyenne)",
        metrics.format_number(current["spi"], 2),
        delta_text("spi", 2),
        help="Z-score des précipitations annuelles vs la moyenne de la région (années complètes).",
    )
    columns[1].metric(
        "Ratio précipitations / ET0",
        metrics.format_number(current["ratio"], 2),
        delta_text("ratio", 2),
        help="Indice d'aridité annuel : <0,20 très sec · <0,50 sec · <0,65 normal · ≥0,65 humide.",
    )
    columns[2].metric(
        "Score de potentiel solaire",
        metrics.format_number(current["solar_score"], 1, " / 100"),
        delta_text("solar_score", 1),
        help="0,7 × score de rayonnement (3-7 kWh/m²/j) + 0,3 × part de jours ensoleillés.",
    )
    columns[3].metric(
        "Jours secs (moyenne)",
        metrics.format_number(current["dry_days"], 0),
        delta_text("dry_days", 0),
        delta_color="inverse",
        help="Jours avec moins de 1 mm de précipitations.",
    )


def render_rankings(view: pd.DataFrame) -> None:
    left, right = st.columns(2)
    solar = metrics.rank_regions(view, "solar_potential_score", ascending=False)
    dry = metrics.rank_regions(view, "precip_et0_ratio", ascending=True)
    with left:
        st.markdown("**Top potentiel solaire**")
        if solar.empty:
            st.caption("Pas de score pour cette sélection (année partielle).")
        else:
            st.dataframe(
                solar.rename(
                    columns={"region_name": "Région", "solar_potential_score": "Score (0-100)"}
                ),
                hide_index=True,
            )
    with right:
        st.markdown("**Régions les plus sèches** (ratio P/ET0 le plus bas)")
        if dry.empty:
            st.caption("Pas de ratio pour cette sélection.")
        else:
            st.dataframe(
                dry.rename(columns={"region_name": "Région", "precip_et0_ratio": "Ratio P/ET0"}),
                hide_index=True,
            )


def render_time_series(kpis: pd.DataFrame, regions: list[str]) -> None:
    label = st.selectbox("Indicateur", list(metrics.TIMESERIES_METRICS))
    column = metrics.TIMESERIES_METRICS[label]
    history = metrics.filter_kpis(kpis, None, regions)
    figure = px.line(
        history,
        x="year",
        y=column,
        color="region_name",
        markers=True,
        labels={"year": "Année", column: label, "region_name": "Région"},
    )
    figure.update_xaxes(dtick=1)
    st.plotly_chart(figure)
    st.caption("Points manquants : années incomplètes (indicateur non calculé).")


def main() -> None:
    st.title("Climat & potentiel solaire — régions du Maroc")

    try:
        kpis = load_kpis()
    except Exception as exc:
        st.error(
            "Impossible de lire les marts dans Postgres. Postgres est-il démarré "
            "(`make up`), `dbt run` a-t-il été exécuté et `.env` est-il présent ?"
        )
        st.exception(exc)
        st.stop()
    if kpis.empty:
        st.warning("Les marts sont vides : lancer `make dbt-seed` puis `make dbt-run`.")
        st.stop()

    # --- Filtres ---------------------------------------------------------------------------
    years = metrics.available_years(kpis)
    default_year = metrics.latest_complete_year(kpis) or years[0]
    year = st.sidebar.selectbox(
        "Année",
        years,
        index=years.index(default_year),
        format_func=lambda y: metrics.year_label(kpis, y),
    )
    names = dict(zip(kpis["region_code"], kpis["region_name"], strict=True))
    all_codes = sorted(names)
    regions = st.sidebar.multiselect(
        "Régions", all_codes, default=all_codes, format_func=lambda code: names[code]
    )
    mode_label = st.sidebar.radio("Couleur de la carte", list(MAP_MODES))
    mode = MAP_MODES[mode_label]
    if not regions:
        st.warning("Sélectionner au moins une région.")
        st.stop()

    view = metrics.filter_kpis(kpis, year, regions)
    partial_year = metrics.is_partial_year(kpis, year)
    # Pas d'ecart vs N-1 pour une annee partielle : la comparaison serait trompeuse.
    previous = kpis.iloc[0:0] if partial_year else metrics.filter_kpis(kpis, year - 1, regions)
    if partial_year:
        st.info(
            "Année partielle : les classes de sécheresse, le SPI et les scores ne sont calculés "
            "que pour les années complètes (couverture ≥ 95 %)."
        )

    # --- Vue d'ensemble ------------------------------------------------------------------
    render_kpi_cards(view, previous)
    st.subheader(f"Carte — {mode_label.lower()} ({year})")
    render_map(view, mode)

    st.subheader("Classements")
    render_rankings(view)

    st.subheader("Évolution annuelle")
    render_time_series(kpis, regions)

    # --- Detail + export -------------------------------------------------------------------
    st.subheader(f"Détail ({year})")
    if metrics.has_mock_agriculture(view):
        st.warning(
            "⚠️ Les colonnes agricoles marquées [mock] (production, surface irriguée, ressource "
            "hydrique) proviennent d'une fixture synthétique, pas de données réelles data.gov.ma."
        )
    table = metrics.display_table(view)
    st.dataframe(table, hide_index=True)
    st.download_button(
        "Exporter la vue courante (CSV)",
        data=metrics.to_csv_bytes(table),
        file_name=f"kpi_maroc_{year}.csv",
        mime="text/csv",
    )

    with st.expander("Méthodologie et limites"):
        st.markdown(
            "- **Source** : marts dbt `fct_region_climate_kpi` et `fct_solar_potential` "
            "(météo Open-Meteo, un point de mesure par région).\n"
            "- **Ratio P/ET0** : précipitations / évapotranspiration annuelles ; classes inspirées "
            "de l'indice d'aridité UNEP.\n"
            "- **SPI simplifié** : z-score des précipitations annuelles de la région (pas le SPI "
            "normalisé par loi gamma).\n"
            "- **Score solaire** : 0,7 × rayonnement moyen (bornes fixes 3-7 kWh/m²/j) + 0,3 × "
            "part de jours ≥ 5 kWh/m²/j.\n"
            "- **Années partielles** : indicateurs de classe, SPI et score non calculés.\n"
            "- **Agriculture** : données synthétiques tant qu'aucune source data.gov.ma n'est "
            "configurée."
        )


main()
