from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, Optional

import altair as alt
import ggsql
import polars as pl


class ggsqlOracle:
    """Thin wrapper around an existing oracledb connection or pool.

    You create the Oracle connection/pool yourself and pass it in.
    This class handles:
    - sync(): SQL -> cursor -> Polars DataFrame
    - plot(): Polars DataFrame -> ggsql.render_altair(...)
    - display(): return the current Altair chart
    - save(): save HTML/JSON and optionally PNG/SVG
    """

    def __init__(self, oracle_source: Any) -> None:
        """Accept either an oracledb connection or pool-like object.

        Expected behavior:
        - if the object has `.acquire()`, it is treated like a pool
        - otherwise it is treated like a connection
        """
        self.oracle_source = oracle_source
        self.df: Optional[pl.DataFrame] = None
        self.chart: Optional[alt.TopLevelMixin] = None
        self.last_sql: Optional[str] = None
        self.last_ggsql: Optional[str] = None

    def _get_connection(self):
        if hasattr(self.oracle_source, "acquire"):
            return self.oracle_source.acquire()
        return _ConnectionContext(self.oracle_source)

    def _rows_to_polars(
        self,
        columns: list[str],
        rows: Iterable[tuple[Any, ...]],
    ) -> pl.DataFrame:
        data = [dict(zip(columns, row)) for row in rows]
        df = pl.DataFrame(data)

        casts = [
            pl.col(name).cast(pl.String)
            for name, dtype in zip(df.columns, df.dtypes)
            if dtype == pl.Categorical or str(dtype).startswith("Enum")
        ]
        if casts:
            df = df.with_columns(casts)
        return df

    def sync(self, sql: str, *, arraysize: int = 1000) -> pl.DataFrame:
        """Run SQL against Oracle and store the result as a Polars DataFrame."""
        with self._get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.arraysize = arraysize
                cursor.execute(sql)
                rows = cursor.fetchall()
                columns = [desc[0].lower() for desc in cursor.description]

        self.last_sql = sql
        self.df = self._rows_to_polars(columns, rows)
        self.chart = None
        return self.df

    def plot(
        self,
        ggsql_query: str,
        *,
        width: int = 900,
        height: int = 480,
        validate: bool = False,
    ) -> alt.TopLevelMixin:
        if self.df is None:
            raise ValueError("No data loaded. Call sync(sql) first.")

        chart = ggsql.render_altair(self.df, ggsql_query, validate=validate)
        chart = chart.properties(width=width, height=height)

        self.last_ggsql = ggsql_query
        self.chart = chart
        return chart

    def display(self) -> alt.TopLevelMixin:
        if self.chart is None:
            raise ValueError("No chart available. Call plot(...) first.")
        return self.chart

    def save(self, path: str | Path, **kwargs: Any) -> Path:
        if self.chart is None:
            raise ValueError("No chart available. Call plot(...) first.")

        path = Path(path)
        suffix = path.suffix.lower()

        if suffix in {".html", ".json"}:
            self.chart.save(path, **kwargs)
            return path

        if suffix in {".png", ".svg"}:
            try:
                import vl_convert as vlc  # type: ignore
            except ImportError:
                try:
                    import vl_convert_python as vlc  # type: ignore
                except ImportError as exc:
                    raise RuntimeError(
                        "PNG/SVG export requires vl-convert-python. Install it with: pip install vl-convert-python"
                    ) from exc

            spec = self.chart.to_dict()
            if suffix == ".png":
                png_bytes = vlc.vegalite_to_png(spec)
                path.write_bytes(png_bytes)
            else:
                svg_text = vlc.vegalite_to_svg(spec)
                path.write_text(svg_text, encoding="utf-8")
            return path

        raise ValueError(f"Unsupported output format: {suffix}")


class _ConnectionContext:
    """Wrap a plain connection so it can be used in a with-statement."""

    def __init__(self, connection: Any) -> None:
        self.connection = connection

    def __enter__(self) -> Any:
        return self.connection

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

