import sys
from pathlib import Path

from streamlit.web.cli import main


if __name__ == "__main__":
    app_path = Path(__file__).with_name("app.py")
    sys.argv = [
        "streamlit",
        "run",
        str(app_path),
        "--server.headless=true",
        "--browser.gatherUsageStats=false",
    ]
    raise SystemExit(main())
