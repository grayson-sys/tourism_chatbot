import sys
from pathlib import Path

import requests


def main() -> None:
    if len(sys.argv) < 2:
        print("Provide one or more asset URLs to download.")
        return
    target_dir = Path(__file__).resolve().parents[1] / "app" / "static" / "brand"
    target_dir.mkdir(parents=True, exist_ok=True)

    for url in sys.argv[1:]:
        filename = url.split("/")[-1].split("?")[0]
        dest = target_dir / filename
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
        dest.write_bytes(resp.content)
        print(f"Saved {dest}")


if __name__ == "__main__":
    main()
