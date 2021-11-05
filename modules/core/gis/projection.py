"""
    Map Projections

    Copyright: (c) 2010-2021 Sahana Software Foundation

    Permission is hereby granted, free of charge, to any person
    obtaining a copy of this software and associated documentation
    files (the "Software"), to deal in the Software without
    restriction, including without limitation the rights to use,
    copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the
    conditions:

    The above copyright notice and this permission notice shall be
    included in all copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
    EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
    OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
    NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
    HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
    WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
    FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
    OTHER DEALINGS IN THE SOFTWARE.
"""

from gluon import current
from gluon.storage import Storage

from .base import GIS

# =============================================================================
class Projection:
    """
        Represents a Map Projection
    """

    def __init__(self, projection_id=None):

        if projection_id:
            s3db = current.s3db
            table = s3db.gis_projection
            query = (table.id == projection_id)
            projection = current.db(query).select(table.epsg,
                                                  limitby = (0, 1),
                                                  cache = s3db.cache
                                                  ).first()
        else:
            # Default projection
            config = GIS.get_config()
            projection = Storage(epsg = config.epsg)

        self.epsg = projection.epsg

# END =========================================================================

