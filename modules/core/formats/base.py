"""
    Format Parser/Writer Base

    Copyright: 2011-2022 (c) Sahana Software Foundation

    Permission is hereby granted, free of charge, to any person
    obtaining a copy of this software and associated documentation
    files (the "Software"), to deal in the Software without
    restriction, including without limitation the rights to use,
    copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the
    Software is furnished to do so, subject to the following
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

__all__ = ("FormatWriter",)

from gluon import current

# =============================================================================
class FormatWriter:

    @staticmethod
    def encode(resource, **attr):
        """
            Method to encode a resource in the target format,
            to be implemented by the subclass (mandatory)

            Args:
                resource: the CRUDResource

            Returns:
                a handle to the output
        """

        raise NotImplementedError

# =============================================================================
class FormatParser:

    @staticmethod
    def decode(resource, source, **attr):
        """
            Method to decode a source into an ElementTree,
            to be implemented by the subclass

            Args:
                resource: the CRUDResource
                source: the source

            Returns:
                an ElementTree
        """

        return current.xml.tree()

# End =========================================================================
