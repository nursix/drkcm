"""
    Provider Commissioning documents for RLPPTM

    Copyright: 2022 (c) AHSS

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

import base64
import datetime
import hashlib
import os
import secrets

from io import BytesIO
from lxml import etree
from reportlab.pdfgen import canvas
from uuid import UUID

from reportlab.graphics import renderPDF
from reportlab.graphics.barcode import qr
from reportlab.graphics.shapes import Drawing
from reportlab.lib.enums import TA_LEFT, TA_JUSTIFY
from reportlab.lib.pagesizes import A4, LETTER
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.lib.utils import ImageReader
from reportlab.platypus import BaseDocTemplate, Frame, KeepTogether, PageTemplate, Paragraph

from gluon import current

from core import s3_str

# Fonts we use in this layout
NORMAL = "Helvetica"
BOLD = "Helvetica-Bold"

# =============================================================================
class ProviderCommission:
    """ Provider Commissioning Note """

    def __init__(self, commission_id):
        """
            Args:
                commission_id: the org_commission record ID
        """

        self.commission_id = commission_id

        self._commission = None
        self._organisation = None
        self._provider_id = None

        self._vcode = None

    # -------------------------------------------------------------------------
    @property
    def commission(self):
        """
            The relevant org_commission record

            Returns:
                Row
        """

        commission = self._commission

        if not commission:
            table = current.s3db.org_commission
            query = (table.id == self.commission_id) & \
                    (table.deleted == False)
            row = current.db(query).select(table.id,
                                           table.uuid,
                                           table.organisation_id,
                                           table.date,
                                           table.end_date,
                                           table.status,
                                           table.cnote,
                                           table.vhash,
                                           limitby = (0, 1),
                                           ).first()
            commission = self._commission = row

        return commission

    # -------------------------------------------------------------------------
    @property
    def organisation_id(self):
        """
            The relevant org_organisation record ID

            Returns:
                id
        """

        return self.commission.organisation_id if self.commission else None

    # -------------------------------------------------------------------------
    @property
    def organisation(self):
        """
            The relevant org_organisation record

            Returns:
                Row
        """

        organisation = self._organisation

        if not organisation:
            table = current.s3db.org_organisation
            query = (table.id == self.organisation_id) & \
                    (table.deleted == False)
            row = current.db(query).select(table.id,
                                           table.name,
                                           limitby = (0, 1),
                                           ).first()
            organisation = self._organisation = row

        return organisation

    # -------------------------------------------------------------------------
    @property
    def organisation_name(self):
        """
            The name of the organisation

            Returns:
                str
        """

        return self.organisation.name if self.organisation else None

    # -------------------------------------------------------------------------
    @property
    def provider_id(self):
        """
            The organisation ID (tag) of the relevant organisation

            Returns:
                str
        """

        provider_id = self._provider_id
        if not provider_id:
            table = current.s3db.org_organisation_tag
            query = (table.organisation_id == self.organisation_id) & \
                    (table.tag == "OrgID") & \
                    (table.deleted == False)
            row = current.db(query).select(table.value,
                                           limitby = (0, 1),
                                           ).first()
            provider_id = row.value if row else None
            self._provider_id = provider_id

        return provider_id

    # -------------------------------------------------------------------------
    @property
    def vcode(self):
        """
            A unique code representing the document identity and
            commissioning details

            Returns:
                bytes (Base64-encoded)
        """

        vcode = self._vcode

        if not vcode:
            record = self.commission

            # The record UUID as plain hex
            uuid = UUID(self.commission.uuid).hex

            # Start and end dates in ISO-format
            nodate = "0000-00-00"
            start = record.date.isoformat() if record.date else nodate
            end = record.end_date.isoformat() if record.end_date else nodate

            # Generate code
            items = [self.provider_id,
                     uuid,
                     start,
                     end,
                     secrets.token_hex(16).upper(),
                     ]
            vcode = base64.b64encode("|".join(items).encode("utf-8"))

            self._vcode = vcode

        return vcode

    # -------------------------------------------------------------------------
    @property
    def vhash(self):
        """
            An SHA256 Hash of the vcode

            Returns:
                str, hex representation of the hash
        """

        return hashlib.sha256(self.vcode).hexdigest().lower()

    # -------------------------------------------------------------------------
    @staticmethod
    def get_template(date):
        """
            Get the commission document template from CMS

            Args:
                date: the date of the commission, the template version
                      date must not be later than that

            Returns:
                tuple (version_date, template)
        """

        module = "org"
        resource = "commission"
        prefix = "Commission"

        db = current.db
        s3db = current.s3db

        ctable = s3db.cms_post
        ltable = s3db.cms_post_module
        join = ltable.on((ltable.post_id == ctable.id) & \
                         (ltable.module == module) & \
                         (ltable.resource == resource) & \
                         (ltable.deleted == False))

        name = "%s%%" % prefix
        query = (ctable.name.like(name)) & \
                (ctable.deleted == False)
        rows = db(query).select(ctable.id,
                                ctable.name,
                                ctable.body,
                                join = join,
                                )
        version = template = None
        for row in rows:
            suffix = row.name[len(prefix):]
            try:
                vdate = datetime.datetime.strptime(suffix, "%Y%m%d").date()
            except ValueError:
                continue
            if vdate > date:
                continue
            if version is None or vdate > version:
                version, template = vdate, row.body

        return version, template

    # -------------------------------------------------------------------------
    def contents_xml(self):
        """
            Render the contents XML for the commission document, using
            the template from CMS and substituting the variables:
                - name (commissioned organisation name)
                - id (commissioned organisation provider ID)
                - start (start date of the commission)
                - end (end date of the commission)
                - version (version date of the referenced law)

            Returns:
                str containing the XML
        """

        record = self.commission

        table = current.s3db.org_commission

        # Get the correct template
        start = record.date
        if not start:
            start = datetime.datetime.utcnow().date()
        version_date, template = self.get_template(start)
        if template:
            # Collect the variables
            field = table.date
            start = field.represent(start)
            version = field.represent(version_date)

            field = table.end_date
            end = field.represent(record.end_date) if record.end_date else "***"

            variables = {"name": self.organisation_name,
                         "id": self.provider_id,
                         "start": start,
                         "end": end,
                         "version": version,
                         }

            # Substitute placeholders
            from .notifications import formatmap
            xmlstr = formatmap(template, variables)
        else:
            xmlstr = None

        return xmlstr

    # -------------------------------------------------------------------------
    def pdf(self):
        """
            Renders this commission document as PDF

            Returns:
                Byte stream (BytesIO) if successful, otherwise None
        """

        doc = CommissionDocTemplate(self)

        try:
            contents = self.contents_xml()
        except Exception:
            contents = None
        if contents:
            try:
                output_stream = BytesIO()
                flow = doc.get_flowables(contents)
                doc.build(flow,
                          output_stream,
                          canvasmaker=NumberedCanvas,
                          )
            except Exception:
                output_stream = None
            else:
                output_stream.seek(0)
        else:
            output_stream = None

        return output_stream

    # -------------------------------------------------------------------------
    def issue_note(self):
        """
            Issue a commissioning note as PDF document in the record, and
            store the hash for verification of the document
        """

        commission = self.commission
        if commission.status != "CURRENT" or commission.vhash:
            return

        note = self.pdf()
        if note is None:
            # Remove any existing document
            commission.update_record(cnote=None, vhash=None)
        else:
            # Store new document and hash
            table = current.s3db.org_commission
            filename = table.cnote.store(note, filename="commission_note.pdf")
            commission.update_record(cnote = filename,
                                     vhash = self.vhash,
                                     )

# =============================================================================
class CommissionDocTemplate(BaseDocTemplate):
    """
        Platypus document template for the commission PDF
    """

    def __init__(self, commission, pagesize=None):
        """
            Args:
                commission: the ProviderCommission record
                pagesize: "A4"|"Letter"|(width,height), default "A4"
        """

        self.commission = commission

        # Page size (default A4)
        if pagesize == "A4":
            pagesize = A4
        elif pagesize == "Letter":
            pagesize = LETTER
        elif not isinstance(pagesize, (tuple, list)):
            pagesize = A4

        margins = (2*cm, 1.5*cm, 1.5*cm, 2.5*cm)

        pages = self.page_layouts(pagesize, margins)

        # Call super-constructor
        super().__init__(None, # filename, unused
                         pagesize = pagesize,
                         pageTemplates = pages,
                         topMargin = margins[0],
                         rightMargin = margins[1],
                         bottomMargin = margins[2],
                         leftMargin = margins[3],
                         title = "COVID-19 Test Provider Commissioning Note",
                         author = "LSJV Rheinland-Pfalz",
                         creator = "RLP Test Station Portal",
                         )

    # -------------------------------------------------------------------------
    def page_layouts(self, pagesize, margins):
        """
            Instantiates the necessary PageTemplates with Frames

            Returns:
                list of PageTemplates
        """

        footer_height = 1*cm
        header_height = 5.4*cm

        pagewidth, pageheight = pagesize
        margin_top, margin_right, margin_bottom, margin_left = margins

        printable_width = pagewidth - margin_left - margin_right
        printable_height = pageheight - margin_top - margin_bottom

        frame = Frame(margin_left,
                      margin_bottom + footer_height,
                      printable_width,
                      printable_height - footer_height - header_height,
                      topPadding = 8,
                      rightPadding = 0,
                      bottomPadding = 8,
                      leftPadding = 0,
                      )

        return [PageTemplate(id="AllPages", frames=[frame], onPage=self.draw_fixed)]

    # -------------------------------------------------------------------------
    @staticmethod
    def get_flowables(contents):
        """
            Converts the contents XML into a list of Flowables

            Args:
                contents: the contents XML (str)

            Returns:
                list of Flowables
        """

        style_sheet = getSampleStyleSheet()
        style = style_sheet["Normal"]
        style.spaceAfter = 10
        style.alignment = TA_JUSTIFY
        style.fontName = NORMAL

        body = []

        root = etree.fromstring(contents)

        for elem in root.xpath("section|para"):
            if elem.tag == "section":
                items = []
                for para in elem.findall("para"):
                    item = etree.tostring(para).decode("utf-8")
                    items.append(Paragraph(item, style=style))
                body.append(KeepTogether(items))
            else:
                item = etree.tostring(elem).decode("utf-8")
                body.append(Paragraph(item, style=style))

        return body

    # -------------------------------------------------------------------------
    def draw_fixed(self, canvas, doc):
        """
            Draws all fixes page elements

            Args:
                canvas: the Canvas to draw on
                doc: the document
        """

        w, h = doc.pagesize
        commission = self.commission

        # Commissioned organisation details
        name = commission.organisation_name
        if not name:
            name = "***"
        self.draw_line_with_label(canvas,
                                  self.leftMargin,
                                  h - self.topMargin - 3*cm,
                                  width = 220,
                                  label = "Beauftragte(r)",
                                  text = name,
                                  )

        provider_id = commission.provider_id
        if not provider_id:
            provider_id = "***"
        self.draw_line_with_label(canvas,
                                  self.leftMargin,
                                  h - self.topMargin - 4.2*cm,
                                  width = 220,
                                  label = "Organisations-ID",
                                  text = provider_id,
                                  )

        # Logo
        img = os.path.join(current.request.folder, "static", "themes", "RLP", "img", "logo_lsjv.png")
        self.draw_image(canvas,
                        img,
                        self.leftMargin,
                        h - self.topMargin,
                        width = 4*cm,
                        proportional = True,
                        valign = "top",
                        )

        # QR-Code
        vcode = self.commission.vcode.decode("utf-8")
        self.draw_qrcode(canvas,
                         vcode,
                         w - self.rightMargin - 4*cm,
                         h - self.topMargin,
                         size=3.8*cm,
                         level="M",
                         valign="top",
                         )

    # -------------------------------------------------------------------------
    @staticmethod
    def draw_qrcode(canvas, value, x, y, size=40, level="M", halign=None, valign=None):
        """
            Helper function to draw a QR code

            Args:
                value: the string to encode
                x: drawing position
                y: drawing position
                size: the size (edge length) of the QR code
                level: error correction level ("L", "M", "Q", "H")
                halign: horizontal alignment ("left"|"center"|"right"), default left
                valign: vertical alignment ("top"|"middle"|"bottom"), default bottom
        """

        qr_code = qr.QrCodeWidget(value, barLevel=level)

        try:
            bounds = qr_code.getBounds()
        except ValueError:
            # Value contains invalid characters
            return

        w = bounds[2] - bounds[0]
        h = bounds[3] - bounds[1]

        transform = [float(size) / w, 0, 0, float(size) / h, 0, 0]
        d = Drawing(size, size, transform=transform)
        d.add(qr_code)

        hshift = vshift = 0
        if halign == "right":
            hshift = size
        elif halign == "center":
            hshift = float(size) / 2.0

        if valign == "top":
            vshift = size
        elif valign == "middle":
            vshift = float(size) / 2.0

        renderPDF.draw(d, canvas, x - hshift, y - vshift)

    # -------------------------------------------------------------------------
    @staticmethod
    def draw_image(canvas,
                   img,
                   x,
                   y,
                   width=None,
                   height=None,
                   proportional=True,
                   scale=None,
                   halign=None,
                   valign=None,
                   ):
        """
            Helper function to draw an image
                - requires PIL (required for ReportLab image handling anyway)

            Args:
                img: the image (filename or BytesIO buffer)
                x: drawing position
                y: drawing position
                width: the target width of the image (in points)
                height: the target height of the image (in points)
                proportional: keep image proportions when scaling to width/height
                scale: scale the image by this factor (overrides width/height)
                halign: horizontal alignment ("left"|"center"|"right"), default left
                valign: vertical alignment ("top"|"middle"|"bottom"), default bottom
        """

        if hasattr(img, "seek"):
            is_buffer = True
            img.seek(0)
        else:
            is_buffer = False

        try:
            from PIL import Image as pImage
        except ImportError:
            current.log.error("Image rendering failed: PIL not installed")
            return

        pimg = pImage.open(img)
        img_size = pimg.size

        if not img_size[0] or not img_size[1]:
            # This image has at least one dimension of zero
            return

        # Compute drawing width/height
        if scale:
            width = img_size[0] * scale
            height = img_size[1] * scale
        elif width and height:
            if proportional:
                scale = min(float(width) / img_size[0], float(height) / img_size[1])
                width = img_size[0] * scale
                height = img_size[1] * scale
        elif width:
            height = img_size[1] * (float(width) / img_size[0])
        elif height:
            width = img_size[0] * (float(height) / img_size[1])
        else:
            width = img_size[0]
            height = img_size[1]

        # Compute drawing position from alignment options
        hshift = vshift = 0
        if halign == "right":
            hshift = width
        elif halign == "center":
            hshift = width / 2.0

        if valign == "top":
            vshift = height
        elif valign == "middle":
            vshift = height / 2.0

        # Draw the image
        if is_buffer:
            img.seek(0)
        ir = ImageReader(img)

        canvas.drawImage(ir,
                         x - hshift,
                         y - vshift,
                         width = width,
                         height = height,
                         preserveAspectRatio = proportional,
                         mask = "auto",
                         )

    # -------------------------------------------------------------------------
    @classmethod
    def draw_line_with_label(cls, canvas, x, y, width=120, label=None, text=None):
        """
            Draw a placeholder line with label underneath (paper form style),
            and text above (if provided)

            Args:
                x: the horizontal position (from left)
                y: the vertical position (from bottom)
                width: the horizontal length of the line
                label: the label
        """

        label_size, text_size = 7, 9

        canvas.saveState()

        canvas.setLineWidth(0.5)
        canvas.line(x, y, x + width, y)

        if label:
            canvas.setFont("Helvetica", label_size)
            canvas.setFillGray(0.3)
            canvas.drawString(x, y - label_size - 1, label)

        canvas.restoreState()

        if text:
            cls.draw_value(canvas,
                           x + width / 2,
                           y + 5,
                           text,
                           width = width - 10,
                           size = text_size,
                           )

    # -------------------------------------------------------------------------
    @staticmethod
    def draw_value(canvas, x, y, value,
                   width=120,
                   height=40,
                   size=7,
                   bold=True,
                   ):
        """
            Helper function to draw a centered text above position (x, y);
            allows the text to wrap if it would otherwise exceed the given
            width

            Args:
                canvas: the canvas to draw on
                x: drawing position
                y: drawing position
                value: the text to render
                width: the maximum available width (points)
                height: the maximum available height (points)
                size: the font size (points)
                bold: use bold font

            Returns:
                the actual height of the text element drawn
        """

        # Preserve line breaks by replacing them with <br/> tags
        value = s3_str(value).strip("\n").replace('\n','<br />\n')

        style_sheet = getSampleStyleSheet()
        style = style_sheet["Normal"]
        style.fontName = BOLD if bold else NORMAL
        style.fontSize = size
        style.leading = size + 2
        style.splitLongWords = False
        style.alignment = TA_LEFT

        para = Paragraph(value, style)
        aw, ah = para.wrap(width, height)

        while((ah > height or aw > width) and style.fontSize > 4):
            # Reduce font size to make fit
            style.fontSize -= 1
            style.leading = style.fontSize + 2
            para = Paragraph(value, style)
            aw, ah = para.wrap(width, height)

        para.drawOn(canvas, x - para.width / 2, y)

        return ah

# =============================================================================
class NumberedCanvas(canvas.Canvas):
    """ Canvas type with page numbers """

    def __init__(self, *args, **kwargs):

        canvas.Canvas.__init__(self, *args, **kwargs)
        self._saved_page_states = []

    def showPage(self):

        self._saved_page_states.append(dict(self.__dict__))
        self._startPage()

    def save(self):

        num_pages = len(self._saved_page_states)
        for state in self._saved_page_states:
            self.__dict__.update(state)
            self.draw_page_number(num_pages)
            canvas.Canvas.showPage(self)
        canvas.Canvas.save(self)

    def draw_page_number(self, page_count):

        self.setFont("Helvetica", 7)
        self.drawRightString(self._pagesize[0] - 2.1*cm,
                             1.8*cm,
                             "%d / %d" % (self._pageNumber, page_count),
                             )

# END =========================================================================
