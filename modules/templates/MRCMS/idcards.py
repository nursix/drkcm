"""
    ID Card Generator and Layout for MRCMS

    License: MIT
"""

import hashlib
import os
import secrets
import uuid

from reportlab.lib.pagesizes import A4
from reportlab.lib.colors import Color, HexColor
from reportlab.platypus import Paragraph
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.enums import TA_CENTER, TA_RIGHT, TA_LEFT

from gluon import current
from gluon.contenttype import contenttype

from core import CRUDMethod, PDFCardLayout, s3_format_fullname, s3_str

# Fonts we use in this layout
NORMAL = "Helvetica"
BOLD = "Helvetica-Bold"

# =============================================================================
class GenerateIDCard(CRUDMethod):

    # -------------------------------------------------------------------------
    def apply_method(self, r, **attr):
        # TODO docstring

        # Must be person+identity
        resource, component = r.resource, r.component
        if resource.tablename != "pr_person" or \
           not component or component.tablename != "pr_identity":
            r.error(400, current.ERROR.BAD_METHOD)

        # Context (person) record is required
        if not r.record:
            r.error(400, current.ERROR.BAD_METHOD)

        if r.interactive:
            # ID document generation
            if r.http in ("GET", "POST"):
                output = self.generate(r, **attr)
            else:
                r.error(405, current.ERROR.BAD_METHOD)
        elif r.representation == "pdf":
            # ID document download
            if r.http == "GET":
                output = self.download(r, **attr)
            else:
                r.error(405, current.ERROR.BAD_METHOD)
        else:
            r.error(415, current.ERROR.BAD_FORMAT)

        return output

    # -------------------------------------------------------------------------
    def generate(self, r, **attr):
        # TODO docstring

        # User must have permission to create identity records
        if not current.auth.s3_has_permission("create", "pr_identity"):
            r.unauthorised()

        T = current.T

        s3db = current.s3db

        resource = r.resource
        record = r.record
        person_id = record.id

        # TODO Proper form with info, confirmation + cancel-option
        #      - include warning about existing IDs (if any)
        #      - require explicit confirmation to invalidate them (mandatory)
        #      - allow user to select an expiry date?
        from gluon import FORM, BUTTON, DIV, P
        form = FORM(DIV(P("Alle bisherigen IDs werden durch die Aktion ungültig!"),
                        ),
                    BUTTON(T("Generate ID"),
                           _type="submit",
                           _class="small primary button",
                           ),
                    )

        # TODO include person_id in form name
        formname = "generate_id_cards"

        if form.accepts(r.post_vars,
                        current.session,
                        formname = formname,
                        #onvalidation = onvalidation, # TODO validate what?
                        keepvalues = False,
                        hideerror = False
                        ):

            idcard = IDCard(person_id)
            if r.controller == "hrm":
                layout = StaffIDCardLayout
            else:
                layout = IDCardLayout

            # Generate PDF document (with registration callback)
            # TODO choose alternative IDCardLayout depending on controller (dvr/hrm - default dvr)
            resource.configure(id_card_callback = idcard.register)
            from core import DataExporter
            document = DataExporter.pdfcard(resource,
                                            layout = layout,
                                            pagesize = "A4",
                                            )
            resource.clear_config("id_card_callback")

            identity_id = idcard.identity_id
            if not identity_id:
                r.error(503, "ID Registration failed", next=r.url())

            # Store the PDF document
            dtable = s3db.pr_identity_document
            entry = {"identity_id": idcard.identity_id,
                     "file": dtable.file.store(document, filename="%s_card_%s.pdf" % (resource.name, person_id)),
                     }
            entry_id = dtable.insert(**entry) # TODO postprocess create?

            # Invalidate previously generated IDs
            if entry_id:
                IDCard.invalidate_ids(person_id, keep=idcard.identity_id)

            # TODO Diplay details of the newly generated ID
            #      - including name, ID number and signature (fingerprint), possibly picture or QR code
            # TODO Include button to go back to ID list / person details
            from gluon import A
            form = A(T("Download PDF"), # TODO Include PDF-Icon
                     data = {"url": r.url(component_id = idcard.identity_id,
                                          method = "generate",
                                          representation = "pdf",
                                          ),
                             },
                     _class = "action-btn s3-download-button",
                     )

        output = {"form": form,
                  "title": T("Generate ID card"),
                  }
        current.response.view = self._view(r, "update.html")

        return output

    # -------------------------------------------------------------------------
    @staticmethod
    def download(r, **attr):
        # TODO docstring

        # Request must specify a particular identity record
        component_id = r.component_id
        if not component_id:
            r.error(400, current.ERROR.BAD_METHOD)

        # User must have permission to read this particular identity record
        if not current.auth.s3_has_permission("read", "pr_identity", record_id=component_id):
            r.unauthorised()

        db = current.db
        s3db = current.s3db

        itable = s3db.pr_identity
        dtable = s3db.pr_identity_document

        join = itable.on((itable.id == dtable.identity_id) & \
                         (itable.person_id == r.record.id) & \
                         (itable.id == r.component_id) & \
                         (itable.deleted == False))
        query = (dtable.file != None) & (dtable.deleted == False)
        row = db(query).select(dtable.id,
                               dtable.file,
                               join = join,
                               limitby = (0, 1),
                               orderby = ~dtable.id,
                               ).first()

        if row:
            filename, stream = dtable.file.retrieve(row.file)

            # Add file name and content headers
            response = current.response
            disposition = "attachment; filename=\"%s\"" % filename
            response.headers["Content-Type"] = contenttype(".pdf")
            response.headers["Content-disposition"] = disposition

            output = stream
        else:
            r.error(404, current.ERROR.BAD_RECORD)

        return output

# =============================================================================
class IDCard:
    """ Toolkit for registered, system-generated ID cards """

    def __init__(self, person_id):

        self.person_id = person_id
        self.identity_id = None

    # -------------------------------------------------------------------------
    def register(self, item):
        """
            Registers a system-generated ID card; as draw-callback for
            IDCardLayout (i.e. called per ID)
        """
        # TODO complete docstring

        db = current.db
        s3db = current.s3db

        # Check for existing registration details
        registration = item.get("_reg")
        if registration and \
           all(registration.get(k) for k in ("token", "vhash", "vcode")):
            # Already processed
            return item

        # Validate the person ID in the item
        person_id = item["_row"]["pr_person.id"]
        if person_id != self.person_id:
            return item

        # Look up the PE label and the person record UID
        ptable = s3db.pr_person
        query = (ptable.id == person_id)
        person = db(query).select(ptable.id,
                                  ptable.uuid,
                                  ptable.pe_label,
                                  ptable.pe_id,
                                  limitby = (0, 1),
                                  ).first()
        if not person:
            return item

        # Get the hex representation of the person UID
        person_uuid = person.uuid
        try:
            # Decode the person UID
            person_uuid = uuid.UUID(person_uuid).hex.upper()
        except ValueError:
            pass

        # Get the pe_label
        pe_label = person.pe_label

        # Produce the ID card token
        token = secrets.token_hex(16).upper()

        # Produce the verification hash for the ID record
        # - from pe_label, ID token and person UID
        record_vhash = self.get_vhash(pe_label, person_uuid, token)

        # Produce the UID for the ID record
        record_uid = uuid.uuid4()

        # Generate the verification hash for the ID card
        card_vhash = self.get_chash(pe_label, record_uid.hex.upper(), record_vhash)

        # Generate the card hash fingerprint
        check = self._fingerprint(card_vhash)

        # Generate or update ID record (=register the ID card)
        itable = s3db.pr_identity
        id_data = {"uuid": record_uid.urn,
                   "person_id": person.id,
                   "value": "%s-%s" % (pe_label, check),
                   "type": 999,
                   "system": True,
                   "invalid": False,
                   "valid_from": current.request.utcnow.date(),
                   "vhash": record_vhash,
                   }
        # TODO postprocess create?
        self.identity_id = itable.insert(**id_data)

        # Update and return the item
        item["_reg"] = {"token": token,
                        "vhash": card_vhash,
                        "vcode": check,
                        }
        return item

    # -------------------------------------------------------------------------
    @staticmethod
    def invalidate_ids(person_id, keep=None, expire_only=False):
        """
            Invalidate all system-generated IDs for a person

            Args:
                person_id: the person record ID
                keep: the ID of one identity record to keep
        """

        db = current.db
        s3db = current.s3db

        itable = s3db.pr_identity
        dtable = s3db.pr_identity_document

        query = (itable.person_id == person_id)
        if keep:
            query &= (itable.id != keep)
        query &= (itable.system == True) & \
                 (itable.invalid == False) & \
                 (itable.deleted == False)
        rows = db(query).select(itable.id, itable.valid_until)
        today = current.request.utcnow.date()
        for row in rows:
            # Remove all stored PDF documents for this identity
            db(dtable.identity_id == row.id).update(file=None)

            # Mark the identity record as invalid
            update = {} if expire_only else {"invalid": True}
            if not row.valid_until or row.valid_until > today:
                update["valid_until"] = today
            if update:
                row.update_record(**update)
                s3db.onaccept(itable, row, method="update")

    # -------------------------------------------------------------------------
    @staticmethod
    def get_vhash(label, uid, token):
        # TODO docstring
        # TODO rename as generate_*

        s = "##".join((label, token, uid))
        return hashlib.sha512(s.encode("ascii")).hexdigest().upper()

    # -------------------------------------------------------------------------
    @staticmethod
    def get_chash(label, uid, vhash):
        # TODO docstring
        # TODO rename as generate_*

        s = "##".join((label, uid, vhash))
        return hashlib.sha256(s.encode("ascii")).hexdigest().upper()

    # -------------------------------------------------------------------------
    @classmethod
    def get_id_fingerprint(cls, pe_label):
        # TODO docstring

        db = current.db
        s3db = current.s3db

        # Find the relevant identity record
        itable = s3db.pr_identity
        ptable = s3db.pr_person

        today = current.request.utcnow.date()
        join = ptable.on((ptable.id == itable.person_id) & \
                         (ptable.pe_label == pe_label) & \
                         (ptable.deleted == False))
        query = (itable.system == True) & \
                (itable.invalid == False) & \
                ((itable.valid_until == None) | (itable.valid_until >= today)) & \
                (itable.vhash != None)
        row = db(query).select(ptable.pe_label,
                               itable.uuid,
                               itable.vhash,
                               join = join,
                               limitby = (0, 1),
                               ).first()

        # Compute the card hash
        card_hash = None
        if row:
            pe_label = row.pr_person.pe_label
            record = row.pr_identity
            try:
                uid = uuid.UUID(record.uuid).hex.upper()
            except ValueError:
                pass
            else:
                card_hash = cls.get_chash(pe_label, uid, record.vhash)

        return cls._fingerprint(card_hash) if card_hash else None

    # -------------------------------------------------------------------------
    @staticmethod
    def _fingerprint(vhash):
        # TODO docstring
        # TODO rename as signature?

        d = len(vhash) // 8
        marks = [vhash[d*i:d*i+2] for i in range(d)]

        return "".join(vhash[int(m, 16) % len(vhash)] for m in marks)

    # -------------------------------------------------------------------------
    @classmethod
    def identify(cls, label, verify=True):
        # TODO docstring
        # TODO cleanup

        db = current.db
        s3db = current.s3db

        data = label.strip().split("##") if label else None
        if not data:
            raise SyntaxError("No data for identification")
        elif len(data) == 3:
            label, token, chash = data
        else:
            label, token, chash = data[0], None, None
            verify = False

        # Find the person record
        ptable = s3db.pr_person
        query = (ptable.pe_label == label) & (ptable.deleted == False)
        person = db(query).select(ptable.id,
                                  ptable.uuid,
                                  ptable.pe_label,
                                  limitby = (0, 1),
                                  ).first()
        if not person:
            # Label does not match any registered person
            return None, False

        if verify:
            cls._verify(person, token, chash)

        return person.id, verify

    # -------------------------------------------------------------------------
    @classmethod
    def _verify(cls, person, token, chash):
        # TODO docstring
        # TODO cleanup

        db = current.db
        s3db = current.s3db

        if not token or not hash:
            raise SyntaxError("Insufficient data for ID verification")

        # Compute ID record verification hash
        try:
            uid = uuid.UUID(person.uuid).hex.upper()
        except ValueError:
            uid = person.uuid
        vhash = cls.get_vhash(person.pe_label, uid, token)

        # Find a valid system ID record for this person that matches the vhash
        today = current.request.utcnow.date()
        itable = s3db.pr_identity
        query = (itable.person_id == person.id) & \
                (itable.vhash == vhash) & \
                (itable.system == True) & \
                (itable.invalid == False) & \
                ((itable.valid_until == None) | (itable.valid_until >= today)) & \
                (itable.deleted == False)
        rows = db(query).select(itable.id,
                                itable.uuid,
                                itable.vhash,
                                limitby = (0, 2),
                                )
        if len(rows) != 1:
            raise ValueError("ID record not found")
        record = rows.first()

        # Compute the ID card verification hash
        try:
            uid = uuid.UUID(record.uuid).hex.upper()
        except ValueError:
            # Malformed ID record UID (invalid ID record)
            raise ValueError("Invalid ID record")
        chash_v = cls.get_chash(person.pe_label, uid, vhash)

        if chash_v != chash:
            # ID card verification hash does not match the ID record
            raise ValueError("Verification hash mismatch")

        return person.id

    # -------------------------------------------------------------------------
    def auto_expire(self):
        """
            Auto-expire all ID cards unless holder is still registered
            at a shelter, or an active staff member
        """

        db = current.db
        s3db = current.s3db

        person_id = self.person_id

        rtable = s3db.cr_shelter_registration
        query = (rtable.person_id == person_id) & \
                (rtable.shelter_id != None) & \
                (rtable.registration_status != 3) & \
                (rtable.deleted == False)
        row = db(query).select(rtable.id, limitby=(0, 1)).first()
        if row:
            # ...still planned or checked-in to a shelter
            return

        htable = s3db.hrm_human_resource
        query = (htable.person_id == person_id) & \
                (htable.organisation_id != None) & \
                (htable.status == 1) & \
                (htable.deleted == False)
        row = db(query).select(htable.id, limitby=(0, 1)).first()
        if row:
            # ...still an active staff member
            return

        self.invalidate_ids(person_id, expire_only=True)

# =============================================================================
class IDCardLayout(PDFCardLayout):
    """
        Layout for printable beneficiary ID cards
    """

    cardsize = A4
    orientation = "Portrait"
    doublesided = False

    border_color = HexColor(0x6084bf) # HexColor(0x27548F)

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
                #"middle_name",
                "last_name",
                "date_of_birth",
                "dvr_case.organisation_id$root_organisation",
                "shelter_registration.shelter_id",
                "shelter_registration.shelter_unit_id",
                "shelter_registration.shelter_id$location_id$L2",
                "shelter_registration.shelter_id$location_id$L3",
                "shelter_registration.shelter_id$location_id$L4",
                "shelter_registration.shelter_id$location_id$addr_postcode",
                "shelter_registration.shelter_id$location_id$addr_street",
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

        # Enforce current default language
        T = current.T
        default_language = current.deployment_settings.get_L10n_default_language()
        if default_language:
            translate = lambda s: T(s, language=default_language)
        else:
            translate = T

        item = self.item

        # Invoke draw callback
        callback = self.resource.get_config("id_card_callback")
        if callable(callback):
            callback(item)

        if not self.backside:

            self.draw_base_layout(item, translate)
            self.draw_organisation_details(item, translate)
            self.draw_person_details(item, translate)
            self.draw_registration_details(item, translate)

        else:
            # No backside
            pass

    # -------------------------------------------------------------------------
    def draw_base_layout(self, item, t_):
        # TODO docstring
        # TODO i18n

        w = self.width
        h = self.height

        # Document Title
        title = "Registrierungskarte"
        self.draw_string(20, h-165, title, width=w/2-40, height=20, size=14, bold=True, halign="center")

        # Advice
        advice = "Kein Identitätsnachweis! Nur gültig in Verbindung mit dem digitalen Register."
        self.draw_vertical_string(h/2+20, 25, advice, width=h/2-50)

        # Border
        self.draw_border()

    # -------------------------------------------------------------------------
    def draw_organisation_details(self, item, t_):
        # TODO docstring

        w = self.width
        h = self.height

        raw = item["_row"]
        common = self.common

        # Get the root organisation ID
        root_org = raw["org_organisation.root_organisation"]

        # Get the localized root org name
        org_names = common.get("root_org_names")
        if org_names:
            root_org_name = org_names.get(root_org)

        # Get the organisation logo
        logos = common.get("logos")
        logo = logos.get(root_org) if logos else None
        if not logo:
            # TODO Make the default logo a setting
            default_logo = os.path.join("static", "themes", "JUH", "img", "logo_small.png")
            logo = os.path.join(current.request.folder, default_logo)

        # Logo
        if logo:
            self.draw_image(logo, w//4, h-80, width=80, height=80, halign="center", valign="middle")

        # Organisation Name
        if root_org_name:
            self.draw_string(20, h-140, root_org_name, width=w/2-40, height=20, size=12, bold=True, halign="center")

    # -------------------------------------------------------------------------
    def draw_person_details(self, item, t_):
        # TODO docstring

        w = self.width
        h = self.height

        raw = item["_row"]
        common = self.common

        draw_string = self.draw_string

        # ----- Left -----

        # Draw box around person data
        left = 30
        bottom = h/2 + 95
        self.draw_box(left, bottom, w/2-60, 150)

        x = left + 10
        y = bottom + 120
        wt = w/2-2*x # text width

        # Shelter
        shelter_id = raw["cr_shelter_registration.shelter_id"]
        if shelter_id:
            # Shelter Name
            shelter = item["cr_shelter_registration.shelter_id"]
            draw_string(x, y + 14, t_("Shelter"))
            draw_string(x, y, shelter, width=wt, height=20, size=10)

            # Shelter address
            location = raw["gis_location.L4"] or raw["gis_location.L3"]
            if location:
                address = item["gis_location.addr_street"]
                if address:
                    draw_string(x, y-13, address, width=wt, height=20, size=9)
                postcode = item["gis_location.addr_postcode"]
                place = "%s %s" % (postcode, location) if postcode else location
                if place:
                    draw_string(x, y-26, place, width=wt, height=20, size=9)

        y = bottom + 55

        # Names
        name = s3_format_fullname(fname = raw["pr_person.first_name"],
                                  #mname = raw["pr_person.middle_name"],
                                  lname = raw["pr_person.last_name"],
                                  truncate = False,
                                  )
        draw_string(x, y + 13, t_("Name"))
        draw_string(x, y, name, width=wt, height=20, size=12)

        # Date of birth
        y = y - 26
        dob = raw["pr_person.date_of_birth"]
        if dob:
            dob = item["pr_person.date_of_birth"]
            draw_string(x, y + 13, t_("Date of Birth"))
            draw_string(x, y, dob, size=12, width=wt)

        # Shelter Unit
        # TODO not for staff
        # TODO do not draw if transitory unit?
        y = y - 26
        unit = raw["cr_shelter_registration.shelter_unit_id"]
        if unit:
            unit = item["cr_shelter_registration.shelter_unit_id"]
            draw_string(x, y + 13, t_("Housing Unit"))
            draw_string(x, y, unit, size=12, width=wt)

        # ----- Right -----

        # Profile picture
        pictures = common.get("pictures")
        picture = pictures.get(raw["pr_person.pe_id"]) if pictures else None
        if picture:
            self.draw_image(picture,
                            w * 3/4, h * 7/8, height=h/4 - 80, width=w/2 - 60,
                            halign = "center",
                            valign = "middle",
                            )

    # -------------------------------------------------------------------------
    def draw_registration_details(self, item, t_):
        # TODO docstring

        h = self.height
        w = self.width

        raw = item["_row"]

        # ----- Left -----

        # ID Number and barcode
        y = h/2 + 60
        code = raw["pr_person.pe_label"]
        if code:
            self.draw_string(30, y, s3_str(code), width=w/2-60, size=28, bold=True, halign="center")
            self.draw_barcode(s3_str(code), w / 4, y - 40, height=28, halign="center", maxwidth=w/2-40)

        # ----- Right -----

        # QR-Code and Hash Signature
        registration = item.get("_reg")
        if not registration or \
            not all(registration.get(d) for d in ("token", "vhash", "vcode")):
            vcode = "%s##UNVERIFIED##UNREGISTERED" % code
            signature = None
        else:
            vcode = "##".join(map(s3_str, (code, registration["token"], registration["vhash"])))
            signature = registration["vcode"]

        if vcode:
            self.draw_qrcode(vcode,
                             w * 3/4,
                             h * 5/8,
                             size = w/2 - 120,
                             halign = "center",
                             valign = "middle",
                             level = "M",
                             )
        if signature:
            self.draw_string(w/2 + 20, h * 6/8 - 15, signature,
                             size = 9,
                             bold = False,
                             width = w/2-40,
                             halign = "center",
                             )

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
    def draw_vertical_string(self, x, y, value, width=120, height=40, size=7, bold=False, halign=None, box=False):
        # TODO docstring

        c = self.canv
        c.saveState()
        c.rotate(90)

        result = self.draw_value(x + width/2.0,
                                 y - self.width,
                                 value,
                                 width = width,
                                 height = height,
                                 size = size,
                                 bold = bold,
                                 halign = halign,
                                 box = box,
                                 )

        c.restoreState()
        return result

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

    # -------------------------------------------------------------------------
    def draw_border(self):
        """
            Draws a border around the contents
        """

        c = self.canv
        c.saveState()

        w = self.width
        h = self.height

        c.setFillColor(self.border_color)

        # Horizontal bars
        c.rect(18, h-22, w-36, 12, fill=1, stroke=0) # horizontal top
        c.rect(18, h/2, w-36, 12, fill=1, stroke=0) # horizontal bottom

        # Vertical bars
        c.rect(18, h/2, 4, h/2-22, fill=1, stroke=0) # vertical left
        c.rect(w-22, h/2, 4, h/2-22, fill=1, stroke=0) # vertical right

        c.restoreState()

    # -------------------------------------------------------------------------
    def draw_box(self, x, y, width, height):
        """
            Draws a (gray) box

            Args:
                x - bottom left corner, x coordinate (in points)
                y - bottom left corner, y coordinate (in points)
                width - the width in points
                height - the height in points
        """

        c = self.canv
        c.saveState()

        c.setLineWidth(1)
        c.setFillGray(0.95)
        c.setStrokeGray(0.9)

        c.rect(x, y, width, height, stroke=1, fill=0)

        c.restoreState()

# =============================================================================
class StaffIDCardLayout(IDCardLayout):
    """
        Variant of IDCardLayout for staff members
    """

    border_color = HexColor(0xeb003c)

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
                #"middle_name",
                "last_name",
                "date_of_birth",
                "human_resource.organisation_id$root_organisation",
                "human_resource.job_title_id",
                ]

    # -------------------------------------------------------------------------
    def draw_person_details(self, item, t_):
        # TODO docstring

        w = self.width
        h = self.height

        raw = item["_row"]
        common = self.common

        draw_string = self.draw_string

        # ----- Left -----

        # Draw box around person data
        left = 30
        bottom = h/2 + 95
        self.draw_box(left, bottom, w/2-60, 130)

        x = left + 10
        y = bottom + 105
        wt = w/2-2*x # text width

        # Staff Role
        draw_string(x, y, t_("Staff"), width=wt, height=20, size=18, bold=True)
        job_title_id = raw["hrm_human_resource.job_title_id"]
        if job_title_id:
            job_title = item["hrm_human_resource.job_title_id"]
            draw_string(x, y-16, job_title, width=wt, height=20, size=12)

        y = bottom + 55

        # Names
        name = s3_format_fullname(fname = raw["pr_person.first_name"],
                                  #mname = raw["pr_person.middle_name"],
                                  lname = raw["pr_person.last_name"],
                                  truncate = False,
                                  )
        draw_string(x, y + 13, t_("Name"))
        draw_string(x, y, name, width=wt, height=20, size=12)

        # Date of birth
        y = y - 26
        dob = raw["pr_person.date_of_birth"]
        if dob:
            dob = item["pr_person.date_of_birth"]
            draw_string(x, y + 13, t_("Date of Birth"))
            draw_string(x, y, dob, size=12, width=wt)

        # ----- Right -----

        # Profile picture
        pictures = common.get("pictures")
        picture = pictures.get(raw["pr_person.pe_id"]) if pictures else None
        if picture:
            self.draw_image(picture,
                            w * 3/4, h * 7/8, height=h/4 - 80, width=w/2 - 60,
                            halign = "center",
                            valign = "middle",
                            )

# END =========================================================================
