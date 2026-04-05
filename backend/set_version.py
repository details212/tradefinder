"""
Admin helper — run this after distributing a new client build to tell the
server which version is now required.  Clients running an older version will
be prompted to download the update on their next login.

Usage:
  python set_version.py <version> [download_url]

Examples:
  python set_version.py 0.12.0
  python set_version.py 0.12.0 https://example.com/downloads/TradeFinder.exe
"""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from app import create_app
from models import Setting


def main() -> None:
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    version      = sys.argv[1].strip()
    download_url = sys.argv[2].strip() if len(sys.argv) > 2 else None

    app = create_app()
    with app.app_context():
        Setting.set(
            "app_required_version",
            version,
            "Minimum client version required to log in",
        )
        print(f"  app_required_version  ->  {version}")

        if download_url:
            Setting.set(
                "app_download_url",
                download_url,
                "URL shown to users who need to update the client",
            )
            print(f"  app_download_url      ->  {download_url}")
        else:
            current_url = Setting.get("app_download_url", "(not set)")
            print(f"  app_download_url      ->  {current_url}  (unchanged)")

    print("\nDone.")


if __name__ == "__main__":
    main()
