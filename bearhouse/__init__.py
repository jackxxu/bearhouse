from pathlib import Path

try:
    from importlib.metadata import version, PackageNotFoundError
except ImportError:
    from importlib_metadata import version, PackageNotFoundError  # type: ignore[no-redef]

try:
    __version__ = version("bearhouse")
except PackageNotFoundError:
    # Fallback for running directly from source without installing the package.
    try:
        import tomllib  # type: ignore[import]
    except ImportError:
        try:
            import tomli as tomllib  # type: ignore[import,no-redef]
        except ImportError:
            tomllib = None  # type: ignore[assignment]

    _pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"

    if tomllib is not None and _pyproject.exists():
        try:
            with _pyproject.open("rb") as f:
                __version__ = tomllib.load(f).get("project", {}).get("version", "0.0.0")
        except Exception:
            __version__ = "0.0.0"
    else:
        __version__ = "0.0.0"
