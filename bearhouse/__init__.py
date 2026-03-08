from pathlib import Path

try:
	try:
		from importlib.metadata import version, PackageNotFoundError
	except Exception:
		from importlib_metadata import version, PackageNotFoundError

	__version__ = version("bearhouse")
except Exception:
	try:
		import tomllib as toml
	except Exception:
		try:
			import toml
		except Exception:
			toml = None

	if toml is not None:
		_pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"
		try:
			with _pyproject.open("rb") as f:
				data = toml.load(f)
			__version__ = data.get("project", {}).get("version", "0.0.0")
		except Exception:
			__version__ = "0.0.0"
	else:
		__version__ = "0.0.0"
