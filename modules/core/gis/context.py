"""
    Map Context

    Copyright: (c) 2022-2022 Sahana Software Foundation

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

import json

from gluon import current

from ..tools import JSONSEPARATORS

DEFAULT_MIME_TYPE = "application/xml"

# =============================================================================
class MapContext:
    """
        Resources and context specification for a map
    """

    def __init__(self, uri):
        """
            Args:
                uri: a universal identifier for this context
        """

        self.uri = uri
        self.resources = []

        self.modified_on = current.request.utcnow.replace(microsecond=0)

    # -------------------------------------------------------------------------
    def append(self, resource):
        """
            Append a resource to this context
        """

        self.resources.append(resource)

    # -------------------------------------------------------------------------
    def as_dict(self):
        """
            Returns the context as JSON-serializable dict
        """

        resources = [r.as_dict() for r in self.resources]

        context = {"type": "FeatureCollection",
                   "id": self.uri,
                   "properties": {
                       "title": "Eden Map Configuration",
                       "updated": self.modified_on.isoformat() + "Z",
                       "lang": "en", # TODO use session language?
                       },
                   "features": resources,
                   }

        return context

    # -------------------------------------------------------------------------
    def json(self):
        """
            Returns this context in GeoJSON notation

            Returns:
                a str containing a GeoJSON object
        """

        return json.dumps(self.as_dict(), separators=JSONSEPARATORS)

# =============================================================================
class MapResource:
    """
        A service resource providing map data
    """

    def __init__(self, uri, title, folder="/", active=False):
        """
            Args:
                uri: a universal identifier for this resource
                title: a title for this resource
                folder: folder for grouping of resources (e.g. layer tree)
        """

        self.uri = uri
        self.title = title
        self.folder = folder
        self.active = active

        self.modified_on = current.request.utcnow

        self.offerings = []
        self.contents = []

    # -------------------------------------------------------------------------
    def add_offering(self, offering):
        """
            Add (append) a service offering to this resource

            Args:
                offering: the Offering instance
        """

        if offering:
            self.offerings.append(offering)

    # -------------------------------------------------------------------------
    def as_dict(self):
        """
            Returns the resource object as JSON-serializable dict
        """

        properties = {"title": self.title,
                      "updated": self.modified_on.isoformat() + "Z",
                      "active": bool(self.active),
                      }
        if self.folder:
            properties["folder"] = self.folder

        if self.offerings:
            properties["offerings"] = [o.as_dict() for o in self.offerings]

        if self.contents:
            properties["contents"] = [c.as_dict() for c in self.contents]

        resource = {"type": "Feature",
                    "id": self.uri,
                    #"geometry": ?,
                    "properties": properties
                    }

        return resource

# =============================================================================
class Offering:
    """
        Service offerings of a MapResource
    """

    # OWS Context Extensions
    extensions = {# OGC-defined extensions:
                 "wfs": "http://www.opengis.net/spec/owc-geojson/1.0/req/wfs",
                 "wms": "http://www.opengis.net/spec/owc-geojson/1.0/req/wms",
                 "wmts": "http://www.opengis.net/spec/owc-geojson/1.0/req/wmts",
                 "kml": "http://www.opengis.net/spec/owc-geojson/1.0/req/kml",
                 "gml": "http://www.opengis.net/spec/owc-geojson/1.0/req/gml",
                 # Custom extensions:
                 "osm": "http://www.opengis.net/spec/owc-geojson/1.0/req/osm",
                 "geojson": "http://www.opengis.net/spec/owc-geojson/1.0/req/geojson",
                 }


    def __init__(self, service_type):
        """
            Args:
                service_type: the service type
        """

        self.code = self.extensions.get(service_type)

        self.operations = []
        self.contents = []

    # -------------------------------------------------------------------------
    def __bool__(self):

        return bool(self.operations) or bool(self.contents)

    # -------------------------------------------------------------------------
    def add_operation(self, code, method="GET", href=None, mime=None, request=None, result=None):
        """
            Add an operation to this offering

            Args:
                code: the name of the operation
                href: the URL to access the operation
                method: the HTTP method to access the operation
                mime: the MIME-type of the expected results
                request: the request body (for method=POST)
                result: the result of the service request as str
                        (e.g. if the request was performed server-side)

            Notes:
                - to specify the MIME-types of request and/or result,
                  use tuples like (MIME-type, content); otherwise the
                  respective content type will default to DEFAULT_MIME_TYPE
        """

        # TODO prevent duplication?
        #self.operations = [op for op in self.operations if op.code != code]

        op = Operation(code,
                       method = method,
                       href = href,
                       mime = mime,
                       request = request,
                       result = result,
                       )
        self.operations.append(op)
        return op

    # -------------------------------------------------------------------------
    def add_content(self, content=None, href=None, mime=DEFAULT_MIME_TYPE):
        """
            Add inline-content to this offering

            Args:
                content: the inline-content (as str)
                href: the URL to access the content (overrides content)
                mime: the MIME-type of the content

            Returns:
                the generated Content-element
        """

        c = Content(content=content, href=href, mime=mime)
        self.contents.append(c)
        return c

    # -------------------------------------------------------------------------
    def as_dict(self):
        """
            Returns the offering object as JSON-serializable dict
        """

        offering = {"code": self.code}
        if self.operations:
            offering["operations"] = [o.as_dict() for o in self.operations]
        elif self.contents:
            offering["contents"] = [c.as_dict() for c in self.contents]
        return offering

# =============================================================================
class Operation:
    """
        Web map service operations (i.e. end point+method)
    """

    def __init__(self,
                 code,
                 method="GET",
                 href=None,
                 mime=None,
                 request=None,
                 result=None,
                 ):
        """
            Args:
                code: the code for the operation
                method: the HTTP method
                href: the URL for the service request
                mime: the expected MIME-type of the result
                request: the request body (for method=POST)
                result: the result of the service request as str
                        (e.g. if the request was performed server-side)

            Notes:
                - to specify the MIME-types of request and/or result,
                  use tuples like (MIME-type, content); otherwise the
                  respective content type will default to DEFAULT_MIME_TYPE
        """

        self.code = code
        self.method = method
        self.href = href
        self.mime = mime

        self.request = request
        self.result = result

    # -------------------------------------------------------------------------
    def __bool__(self):

        return bool(self.code) and bool(self.href)

    # -------------------------------------------------------------------------
    def as_dict(self):
        """
            Returns the operation object as JSON-serializable dict
        """

        op = {"code": self.code,
              "method": self.method,
              "href": self.href,
              }

        mime = self.mime
        if mime:
            op["type"] = mime

        if self.method == "POST":
            op["request"] = self.typed(self.request)

        result = self.result
        if result:
            if mime and not isinstance(result, tuple):
                # Default to expected MIME type
                result = (mime, result)
            op["result"] = self.typed(result)

        return op

    # -------------------------------------------------------------------------
    @staticmethod
    def typed(content):
        """
            Helper to produce typed content tags for request/result

            Args:
                content: the content as str or as tuple (mime-type, content)

            Returns:
                a dict {"type": mime-type, "content": content-str}
        """

        if isinstance(content, tuple):
            mime, content = (content + (None, None))[:2]
        else:
            mime = None

        return {"type": mime if mime else DEFAULT_MIME_TYPE,
                "content": str(content) if content else "",
                }

# =============================================================================
class Content:
    """
        Map contents
    """

    def __init__(self, content=None, href=None, mime=DEFAULT_MIME_TYPE):
        """
            Args:
                content: the inline-content (as str)
                href: the URL to access the content (overrides inline-content)
                mime: the MIME type of the content
        """

        self.mime = mime

        self.href = href
        self.content = content

    # -------------------------------------------------------------------------
    def __bool__(self):

        return bool(self.content) or bool(self.href)

    # -------------------------------------------------------------------------
    def as_dict(self):
        """
            Returns the content object as JSON-serializable dict
        """

        output = {"type": self.mime if self.mime else DEFAULT_MIME_TYPE,
                  }

        href, content = self.href, self.content
        if href:
            output["href"] = str(href)
        else:
            output["content"] = str(content) if content else ""

        return content

# END =========================================================================
