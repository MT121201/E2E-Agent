


from pathlib import Path


def solve_relative_paths_recursively(data: dict, abs_path: Path):
	for k, v in data.items():
		if isinstance(v, dict):
			solve_relative_paths_recursively(v, abs_path)
		if isinstance(v, str) and k.endswith(("file", "path", "dir")):
			data[k] = (abs_path / Path(v)).resolve().as_posix()