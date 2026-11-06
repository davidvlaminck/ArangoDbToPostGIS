"""Allow ``python -m arangodbtopostgis`` to run the full sync."""

import logging

from arangodbtopostgis.main import main

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    main()
