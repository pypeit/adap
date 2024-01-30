import sys
import argparse
from pykoa.koa import Koa 
from pathlib import Path




if __name__ == '__main__':

	parser = argparse.ArgumentParser("koa_download.py", "Downloads files from KOA query results")
	parser.add_argument("dest_path", type=Path, help="Where to put the downloaded files")
	parser.add_argument("query_results", type=Path, nargs="+", help="One or more result files from a KOA query.")
	args = parser.parse_args()
	assert args.dest_path.exists() and args.dest_path.is_dir()

	for path in args.query_results:
		assert path.exists() and path.is_file()
		if path.name.lower().endswith("ipac"):
			format = "ipac"
		else:
			format = "csv"
		output_path = args.dest_path / path.stem
		print(f"Downloading {path}")
		Koa.download(str(path), format, str(output_path), calibfile=0)

