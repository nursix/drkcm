"""
    Beneficiary ID Card Layouts for Village

    License: MIT
"""

import os

from reportlab.lib.pagesizes import A4
from reportlab.lib.colors import Color, HexColor
from reportlab.platypus import Paragraph
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.enums import TA_CENTER, TA_RIGHT, TA_LEFT

from gluon import current

from core.resource.codecs.card import S3PDFCardLayout
from core import s3_format_fullname, s3_str

# Fonts we use in this layout
NORMAL = "Helvetica"
BOLD = "Helvetica-Bold"

# =============================================================================
class IDCardLayout(S3PDFCardLayout):
    """
        Layout for printable beneficiary ID cards
    """

    cardsize = A4
    orientation = "Portrait"
    doublesided = False

    # -------------------------------------------------------------------------
    @classmethod
    def fields(cls, resource):
        """
            The layout-specific list of fields to look up from the resource

            Args:
                resource: the resource

            Returns:
                list of field selectors
        """

        return ["id",
                "pe_id",
                "pe_label",
                "first_name",
                "middle_name",
                "last_name",
                "date_of_birth",
                "person_details.nationality",
                "dvr_case.organisation_id$root_organisation",
                "shelter_registration.shelter_id",
                "shelter_registration.shelter_unit_id",
                ]

    # -------------------------------------------------------------------------
    @classmethod
    def lookup(cls, resource, items):
        """
            Look up layout-specific common data for all cards

            Args:
                resource: the resource
                items: the items

            Returns:
                a dict with common data
        """

        db = current.db
        s3db = current.s3db

        defaultpath = os.path.join(current.request.folder, 'uploads')

        # Get all root organisations
        root_orgs = set(item["_row"]["org_organisation.root_organisation"] for item in items)

        # Look up all logos
        otable = s3db.org_organisation
        query = (otable.id.belongs(root_orgs))
        rows = db(query).select(otable.id, otable.name, otable.logo)

        field = otable.logo
        path = field.uploadfolder if field.uploadfolder else defaultpath
        logos = {row.id: os.path.join(path, row.logo) for row in rows if row.logo}

        # Get root organisation names
        ctable = s3db.dvr_case
        represent = ctable.organisation_id.represent
        if represent.bulk:
            root_org_names = represent.bulk(list(root_orgs), show_link=False)
        else:
            root_org_names = None

        # Get all PE IDs
        pe_ids = set(item["_row"]["pr_person.pe_id"] for item in items)

        # Look up all profile pictures
        itable = s3db.pr_image
        query = (itable.pe_id.belongs(pe_ids)) & \
                (itable.profile == True) & \
                (itable.deleted == False)
        rows = db(query).select(itable.pe_id, itable.image)

        field = itable.image
        path = field.uploadfolder if field.uploadfolder else defaultpath
        pictures = {row.pe_id: os.path.join(path, row.image) for row in rows if row.image}

        return {"pictures": pictures,
                "root_org_names": root_org_names,
                "logos": logos,
                }

    # -------------------------------------------------------------------------
    def draw(self):
        """
            Draw the card (one side)

            Instance attributes (NB draw-function should not modify them):
            - self.canv...............the canvas (provides the drawing methods)
            - self.resource...........the resource
            - self.item...............the data item (dict)
            - self.labels.............the field labels (dict)
            - self.backside...........this instance should render the backside
                                      of a card
            - self.multiple...........there are multiple cards per page
            - self.width..............the width of the card (in points)
            - self.height.............the height of the card (in points)

            NB Canvas coordinates are relative to the lower left corner of the
               card's frame, drawing must not overshoot self.width/self.height
        """

        T = current.T

        c = self.canv
        w = self.width
        h = self.height
        common = self.common

        blue = HexColor(0x27548F)

        item = self.item
        raw = item["_row"]

        root_org = raw["org_organisation.root_organisation"]

        # Get the localized root org name
        org_names = common.get("root_org_names")
        if org_names:
            root_org_name = org_names.get(root_org)

        draw_string = self.draw_string

        if not self.backside:
            # -------- Top ---------

            # Organisation Logo
            logos = common.get("logos")
            logo = logos.get(root_org) if logos else None
            if logo:
                self.draw_image(logo, 80, h-80, width=80, height=80, halign="center", valign="middle")

            # Organisation Name
            if root_org_name:
                draw_string(140, h-60, root_org_name, width=200, height=20, size=12, bold=True)

            # Shelter Name
            shelter = item["cr_shelter_registration.shelter_id"]
            if shelter:
                draw_string(140, h-75, shelter, width=200, height=20, size=10)

            # Document Title
            draw_string(140, h-110, "Registrierungsausweis", width=200, height=20, size=14, bold=True)

            # -------- Left ---------

            x = 60

            # Names
            y = h * 13/16 - 20
            name = s3_format_fullname(fname = raw["pr_person.first_name"],
                                      mname = raw["pr_person.middle_name"],
                                      lname = raw["pr_person.last_name"],
                                      truncate = False,
                                      )
            draw_string(x, y + 14, T("Name"))
            draw_string(x, y, name, size=12, width=w/2)

            # Date of birth
            y = y - 30
            dob = item["pr_person.date_of_birth"]
            draw_string(x, y + 14, T("Date of Birth"))
            draw_string(x, y, dob, size=12, width=w/2)

            # Nationality
            y = y - 30
            nationality = item["pr_person_details.nationality"]
            draw_string(x, y + 14, T("Nationality"))
            draw_string(x, y, nationality, size=12, width=w/2)

            # Lodging
            y = y - 30
            lodging = item["cr_shelter_registration.shelter_unit_id"]
            draw_string(x, y + 14, T("Lodging"))
            draw_string(x, y, lodging, size=12, width=w/2)

            # ID Number and barcode
            y = y - 60
            code = raw["pr_person.pe_label"]
            if code:
                draw_string(20, y, s3_str(code), size=28, bold=True, width=w/2-40, halign="center")
                self.draw_barcode(s3_str(code), w / 4, y - 60, height=36, halign="center", maxwidth=w/2-40)

            # -------- Right ---------

            # Profile picture
            pictures = common.get("pictures")
            picture = pictures.get(raw["pr_person.pe_id"]) if pictures else None
            if picture:
                self.draw_image(picture, w * 3/4, h * 7/8, height=h/4 - 80, width=w/2 - 60, halign="center", valign="middle")

            # QR-Code
            signature = "##".join(map(s3_str, (code, name, dob, nationality)))
            self.draw_qrcode(signature,
                             w * 3/4,
                             h * 5/8,
                             size = w/2 - 120,
                             halign = "center",
                             valign = "middle",
                             level = "M",
                             )

            # Graphics
            c.setFillColor(blue)
            c.rect(0, h-12, w, 12, fill=1, stroke=0)
            c.rect(0, h/2, 12, h/2, fill=1, stroke=0)
            c.rect(0, h/2, w, 12, fill=1, stroke=0)

        else:
            # No backside
            pass

    # -------------------------------------------------------------------------
    def draw_string(self, x, y, value, width=120, height=40, size=7, bold=False, halign=None, box=False):
        """
            Draw a string (label, value)

            Args:
                x: the horizontal position (from left)
                y: the vertical position (from bottom)
                value: the string to render
                width: the width of the text box
                height: the height of the text box
                size: the font size
                bold: use boldface font
                halign: horizonal alignment, "left" (or None, the default)|"right"|"center"
                box: render the box (with border and grey background)

            Returns:
                the actual height of the box
        """

        return self.draw_value(x + width/2.0,
                               y,
                               value,
                               width = width,
                               height = height,
                               size = size,
                               bold = bold,
                               halign = halign,
                               box = box,
                               )

    # -------------------------------------------------------------------------
    def draw_value(self, x, y, value, width=120, height=40, size=7, bold=True, valign=None, halign=None, box=False):
        """
            Helper function to draw a centered text above position (x, y);
            allows the text to wrap if it would otherwise exceed the given
            width

            Args:
                x: drawing position
                y: drawing position
                value: the text to render
                width: the maximum available width (points)
                height: the maximum available height (points)
                size: the font size (points)
                bold: use bold font
                valign: vertical alignment ("top"|"middle"|"bottom"),
                        default "bottom"
                halign: horizontal alignment ("left"|"center")

            Returns:
                the actual height of the text element drawn
        """

        # Preserve line breaks by replacing them with <br/> tags
        value = s3_str(value).strip("\n").replace('\n','<br />\n')

        stylesheet = getSampleStyleSheet()
        style = stylesheet["Normal"]
        style.fontName = BOLD if bold else NORMAL
        style.fontSize = size
        style.leading = size + 2
        style.splitLongWords = False
        style.alignment = TA_CENTER if halign=="center" else \
                          TA_RIGHT if halign == "right" else TA_LEFT

        if box:
            style.borderWidth = 0.5
            style.borderPadding = 3
            style.borderColor = Color(0, 0, 0)
            style.backColor = Color(0.7, 0.7, 0.7)

        para = Paragraph(value, style)
        aw, ah = para.wrap(width, height)

        while((ah > height or aw > width) and style.fontSize > 4):
            # Reduce font size to make fit
            style.fontSize -= 1
            style.leading = style.fontSize + 2
            para = Paragraph(value, style)
            aw, ah = para.wrap(width, height)

        if valign == "top":
            vshift = ah
        elif valign == "middle":
            vshift = ah / 2.0
        else:
            vshift = 0

        para.drawOn(self.canv, x - para.width / 2, y - vshift)

        return ah

# END =========================================================================
