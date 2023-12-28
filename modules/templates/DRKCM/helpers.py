"""
    Helper functions and classes for DRKCM

    License: MIT
"""

import os

from gluon import current, A, DIV, I, SPAN, TAG, URL

from core import ICON, s3_fullname

# =============================================================================
def case_read_multiple_orgs():
    """
        Check if the user has read access to cases of more than one org

        Returns:
            tuple (multiple_orgs, org_ids)
    """

    realms = current.auth.permission.permitted_realms("dvr_case", "read")
    if realms is None:
        multiple_orgs = True
        org_ids = []
    else:
        otable = current.s3db.org_organisation
        query = (otable.pe_id.belongs(realms)) & \
                (otable.deleted == False)
        rows = current.db(query).select(otable.id)
        multiple_orgs = len(rows) > 1
        org_ids = [row.id for row in rows]

    return multiple_orgs, org_ids

# =============================================================================
def case_default_org():
    """
        Determine the default organisation for new cases
    """

    auth = current.auth
    realms = auth.permission.permitted_realms("dvr_case", "create")

    default_org = None

    if realms is None:
        # User can create cases for any org
        orgs = []
        multiple_orgs = True
    else:
        otable = current.s3db.org_organisation
        query = (otable.pe_id.belongs(realms)) & \
                (otable.deleted == False)
        rows = current.db(query).select(otable.id)
        orgs = [row.id for row in rows]
        multiple_orgs = len(rows) > 1

    if multiple_orgs:
        # User can create cases for multiple orgs
        user_org = auth.user.organisation_id if auth.user else None
        if user_org and user_org in orgs:
            default_org = user_org
    elif orgs:
        # User can create cases for exactly one org
        default_org = orgs[0]

    return default_org, multiple_orgs

# =============================================================================
def get_total_consultations(person):
    """
        Get number of consultations for person

        Args:
            person: the beneficiary record (pr_person Row)

        Returns:
            number of consultations
    """

    s3db = current.s3db

    rtable = s3db.dvr_response_action

    from .uioptions import get_ui_options
    ui_options = get_ui_options()
    if ui_options.get("response_types"):
        # Filter by response type
        ttable = s3db.dvr_response_type
        join = ttable.on((ttable.id == rtable.response_type_id) & \
                         (ttable.is_consultation == True))
    else:
        # Count all responses as consultations
        join = None

    query = (rtable.person_id == person.id) & \
            (rtable.deleted == False)
    count = rtable.id.count()

    row = current.db(query).select(count, join=join).first()
    return row[count]

# =============================================================================
def get_protection_themes(person):
    """
        Get response themes of a case that are linked to protection needs

        Args:
            person: the beneficiary record (pr_person Row)

        Returns:
            list-representation of response themes
    """

    db = current.db
    s3db = current.s3db

    # Get all theme_ids that are linked to protection needs
    ntable = s3db.dvr_need
    ttable = s3db.dvr_response_theme

    query = (ntable.protection == True) & \
            (ntable.id == ttable.need_id) & \
            (ttable.deleted == False)
    themes = db(query).select(ttable.id,
                              cache = s3db.cache,
                              )
    theme_ids = set(theme.id for theme in themes)

    # Find out which of these themes are linked to the person
    rtable = s3db.dvr_response_action
    ltable = s3db.dvr_response_action_theme

    query = (ltable.theme_id.belongs(theme_ids)) & \
            (ltable.action_id == rtable.id) & \
            (ltable.deleted == False) & \
            (rtable.person_id == person.id) & \
            (rtable.deleted == False)
    rows = db(query).select(ltable.theme_id,
                            groupby = ltable.theme_id,
                            )
    theme_list = [row.theme_id for row in rows]

    # Return presented as list
    represent = rtable.response_theme_ids.represent
    return represent(theme_list)

# =============================================================================
def get_vulnerabilities(person):
    """
        Get current vulnerabilities of a client

        Args:
            person: the client record (pr_person Row)

        Returns:
            list-representation of vulnerabilities
    """

    db = current.db
    s3db = current.s3db

    vtable = s3db.dvr_vulnerability
    ttable = s3db.dvr_vulnerability_type

    today = current.request.utcnow.date()
    join = vtable.on((vtable.person_id == person.id) & \
                     (vtable.vulnerability_type_id == ttable.id) & \
                     ((vtable.date == None) | (vtable.date <= today)) & \
                     ((vtable.end_date == None) | (vtable.end_date >= today)) & \
                     (vtable.deleted == False))
    query = (ttable.obsolete == False) & (ttable.deleted == False)
    rows = db(query).select(ttable.id,
                            ttable.name,
                            distinct = True,
                            join = join,
                            )
    from core import s3_truncate
    names = [SPAN(s3_truncate(row.name), _class="vuln-tag") for row in rows]

    return TAG[""](*names) if len(names) else None

# =============================================================================
def client_name_age(record):
    """
        Represent a client as name, gender and age; for case file rheader

        Args:
            record: the client record (pr_person)

        Returns:
            HTML
    """

    T = current.T

    pr_age = current.s3db.pr_age

    age = pr_age(record)
    if age is None:
        age = "?"
        unit = T("years")
    elif age == 0:
        age = pr_age(record, months=True)
        unit = T("months") if age != 1 else T("month")
    else:
        unit = T("years") if age != 1 else T("year")

    icons = {2: "fa fa-venus",
             3: "fa fa-mars",
             4: "fa fa-transgender-alt",
             }
    icon = icons.get(record.gender)
    if not icon:
        icon = "fa fa-question-circle-o incomplete-data"
        title = T("Gender unknown")
    else:
        table = current.s3db.pr_person
        title = table.gender.represent(record.gender)

    client = TAG[""](s3_fullname(record, truncate=False),
                     SPAN(I(_class=icon, _title=title),
                          "%s %s" % (age, unit),
                          _class = "client-gender-age",
                          ),
                     )
    return client

# =============================================================================
def warn_if_missing(row, colname):
    """
        Returns a rheader field function that renders a warning instead
        of a null-value if the field value is empty (None or "")

        Args:
            row: the Resource row, including raw data
            colname: the column name
        Returns:
            function
    """

    def rheader_field(record):
        raw = row["_row"]
        if raw.get(colname) in ("", None):
            # Indicate missing value
            return SPAN(current.T("missing##data"),
                        ICON("fa fa-exclamation-circle"),
                        _class = "incomplete-data",
                        )
        else:
            return row[colname]

    return rheader_field

# =============================================================================
def user_mailmerge_fields(resource, record):
    """
        Lookup mailmerge-data about the current user

        Args:
            resource: the context resource (pr_person)
            record: the context record (beneficiary)
    """

    user = current.auth.user
    if not user:
        return {}

    fname = user.first_name
    lname = user.last_name

    data = {"Unterschrift": " ".join(n for n in (fname, lname) if n)
            }
    if fname:
        data["Vorname"] = fname
    if lname:
        data["Nachname"] = lname

    db = current.db
    s3db = current.s3db

    # Look up the user organisation
    otable = s3db.org_organisation
    query = (otable.id == user.organisation_id)
    org = db(query).select(otable.id,
                            otable.name,
                            limitby = (0, 1),
                            ).first()
    if org:
        data["Organisation"] = org.name

        # Look up the team the user belongs to
        ltable = s3db.org_organisation_team
        gtable = s3db.pr_group
        mtable = s3db.pr_group_membership
        ptable = s3db.pr_person
        join = [gtable.on(gtable.id == ltable.group_id),
                mtable.on((mtable.group_id == gtable.id) & (mtable.deleted == False)),
                ptable.on(ptable.id == mtable.person_id),
                ]
        query = (ltable.organisation_id == org.id) & \
                (ltable.deleted == False) & \
                (ptable.pe_id == user.pe_id)
        row = db(query).select(gtable.name,
                                join = join,
                                limitby = (0, 1),
                                orderby = ~(mtable.modified_on),
                                ).first()
        if row:
            data["Team"] = row.name

    # Look up contact information
    ctable = s3db.pr_contact
    query = (ctable.pe_id == user.pe_id) & \
            (ctable.contact_method == "EMAIL") & \
            (ctable.deleted == False)
    row = db(query).select(ctable.value,
                            limitby = (0, 1),
                            orderby = (ctable.priority, ~(ctable.modified_on)),
                            ).first()
    if row:
        data["Email"] = row.value

    priority = {"SMS": 1, "WORK_PHONE": 2, "HOME_PHONE": 3}
    query = (ctable.pe_id == user.pe_id) & \
            (ctable.contact_method.belongs(list(priority.keys()))) & \
            (ctable.deleted == False)
    rows = db(query).select(ctable.priority,
                            ctable.contact_method,
                            ctable.value,
                            orderby = (ctable.priority, ~(ctable.modified_on)),
                            )
    if rows:
        rank = lambda row: row.priority * 10 + priority[row.contact_method]
        numbers = sorted(((row.value, rank(row)) for row in rows), key = lambda i: i[1])
        data["Telefon"] = numbers[0][0]

    return data

# =============================================================================
# Uploaded file representation
#
FILE_ICONS = {
    ".pdf": "file-pdf",
    ".xls": "file-xls",
    ".xlsx": "file-xls",
    ".doc": "file-doc",
    ".docx": "file-doc",
    ".odt": "file-text",
    ".txt": "file-text",
    ".png": "file-image",
    ".jpg": "file-image",
    ".jpeg": "file-image",
    ".bmp": "file-image",
    }

def file_represent(value, row=None):
    """
        Represent an upload-field (file)

        Args:
            value: the uploaded file name
            row: unused, for API compatibility

        Returns:
            representation (DIV-type)
    """

    if not value:
        return current.messages["NONE"]

    try:
        # Check whether file exists and extract the original
        # file name from the stored file name
        name, f = current.db.doc_document.file.retrieve(value)
    except IOError:
        return current.T("File not found")

    # Get the file extension and generate corresponding icon
    ext = os.path.splitext(name)[1].lower()
    icon_type = FILE_ICONS.get(ext)
    if not icon_type:
        icon_type = "file-generic"
    icon = ICON(icon_type)

    output = A(icon,
               _href = URL(c="default", f="download", args=[value]),
               _title = name,
               _class = "file-repr",
               )

    # Determine the file size
    fsize = f.seek(0, 2) if f else None
    if fsize is not None:
        for u in ("B", "kB", "MB", "GB"):
            unit = u
            if fsize < 1024:
                break
            fsize /= 1024
        fsize = "%s %s" % (round(fsize), unit)
        output.append(SPAN(fsize, _class="file-size"))

    return output

# =============================================================================
class PriorityRepresent:
    """
        Color-coded representation of priorities

        @todo: generalize/move to core/tools?
    """

    def __init__(self, options, classes=None):
        """
            Args:
                options: the options (as dict or anything that can be
                         converted into a dict)
                classes: a dict mapping keys to CSS class suffixes
        """

        self.options = dict(options)
        self.classes = classes

    def represent(self, value, row=None):
        """
            Representation function

            Args:
                value: the value to represent
        """

        css_class = base_class = "prio"

        classes = self.classes
        if classes:
            suffix = classes.get(value)
            if suffix:
                css_class = "%s %s-%s" % (css_class, base_class, suffix)

        label = self.options.get(value)

        return DIV(label, _class=css_class)

# END =========================================================================
