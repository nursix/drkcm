"""
    DISEASE module customisations for RLPPTM

    License: MIT
"""

from collections import OrderedDict
from dateutil.relativedelta import relativedelta

from gluon import current, IS_EMPTY_OR, IS_IN_SET
from gluon.storage import Storage

from core import IS_ONE_OF, IS_UTC_DATE, S3CRUD, S3Represent, \
                 get_form_record_id

# -------------------------------------------------------------------------
def case_diagnostics_onaccept(form):
    """
        Custom onaccept routine for disease_case_diagnostics
        - auto-generate/update corresponding daily testing report
    """

    record_id = get_form_record_id(form)
    if not record_id:
        return

    settings = current.deployment_settings

    db = current.db
    s3db = current.s3db

    # Get the record
    table = s3db.disease_case_diagnostics
    query = (table.id == record_id)
    record = db(query).select(table.site_id,
                              table.result_date,
                              table.disease_id,
                              limitby = (0, 1),
                              ).first()
    if not record:
        return

    site_id = record.site_id
    result_date = record.result_date
    disease_id = record.disease_id

    if site_id and disease_id and result_date:
        # Update daily testing report
        if settings.get_disease_testing_report_by_demographic():
            from ..helpers import update_daily_report_by_demographic as update_daily_report
        else:
            from ..helpers import update_daily_report
        update_daily_report(site_id, result_date, disease_id)

# -------------------------------------------------------------------------
def disease_case_diagnostics_resource(r, tablename):

    T = current.T
    db = current.db
    s3db = current.s3db
    settings = current.deployment_settings

    table = s3db.disease_case_diagnostics

    single_disease = single_site = False

    # Make site link visible + limit to managed+approved+active sites
    field = table.site_id
    field.readable = field.writable = True

    from ..helpers import get_managed_facilities
    site_ids = get_managed_facilities("TEST_PROVIDER")
    if site_ids is not None:
        if len(site_ids) == 1:
            single_site = True
            # Default + make r/o
            field.default = site_ids[0]
            field.writable = False
        else:
            # Limit to managed sites
            dbset = db(s3db.org_site.site_id.belongs(site_ids))
            field.requires = IS_ONE_OF(dbset, "org_site.site_id",
                                       field.represent,
                                       )
    else:
        # Site is required
        requires = field.requires
        if isinstance(requires, IS_EMPTY_OR):
            requires = requires.other

    # Enable disease link and make it mandatory
    field = table.disease_id
    field.readable = field.writable = True
    field.comment = None
    requires = field.requires
    if isinstance(requires, (list, tuple)):
        requires = requires[0]
    if isinstance(requires, IS_EMPTY_OR):
        field.requires = requires.other

    # If there is only one disease, default the selector + make r/o
    dtable = s3db.disease_disease
    rows = db(dtable.deleted == False).select(dtable.id,
                                              cache = s3db.cache,
                                              limitby = (0, 2),
                                              )
    if len(rows) == 1:
        single_disease = True
        field.default = rows[0].id
        field.writable = False

    # Default probe details
    field = table.probe_status
    field.default = "PROCESSED"

    now = current.request.utcnow

    # Probe date/time is mandatory
    field = table.probe_date
    field.label = T("Test Date/Time")
    field.default = now
    requires = field.requires
    if isinstance(requires, IS_EMPTY_OR):
        requires = requires.other

    # Default result date
    field = table.result_date
    field.default = now.date()
    field.writable = False

    # Formal test types
    # TODO move to lookup table?
    type_options = (("LFD", T("LFD Antigen Test")),
                    ("PCR", T("PCR Test")),
                    ("SER", T("Serum Antibody Test")),
                    ("OTH", T("Other")),
                    )
    field = table.test_type
    field.default = "LFD"
    field.writable = False # fixed for now
    field.requires = IS_IN_SET(type_options,
                               zero = "",
                               sort = False,
                               )
    field.represent = S3Represent(options=dict(type_options))

    # Formal results
    result_options = (("NEG", T("Negative")),
                      ("POS", T("Positive")),
                      # Only relevant for two-step workflow:
                      #("INC", T("Inconclusive")),
                      )
    field = table.result
    field.requires = IS_IN_SET(result_options,
                               zero = "",
                               sort = False,
                               error_message = T("Please select a value"),
                               )
    field.represent = S3Represent(options=dict(result_options))

    if not single_site or current.auth.s3_has_role("DISEASE_TEST_READER"):
        site_id = "site_id"
    else:
        site_id = None
    disease_id = "disease_id" if not single_disease else None

    if settings.get_disease_testing_report_by_demographic():
        table.demographic_id.readable = True
        demographic_id = "demographic_id"
    else:
        demographic_id = None

    # Custom list_fields
    list_fields = [site_id,
                   disease_id,
                   "probe_date",
                   demographic_id,
                   "result",
                   "device_id",
                   ]

    # Custom form (for read)
    from core import S3SQLCustomForm
    crud_form = S3SQLCustomForm(disease_id,
                                site_id,
                                "probe_date",
                                demographic_id,
                                "result",
                                "result_date",
                                )

    # Filters

    # Limit report by setting date filter default start date
    if r.method == "report":
        start = current.request.utcnow.date() - relativedelta(weeks=1)
        default = {"~.probe_date__ge": {"ge": start}}
    else:
        default = None

    from core import S3DateFilter, S3OptionsFilter, s3_get_filter_opts
    filter_widgets = [S3DateFilter("probe_date",
                                   label = T("Date"),
                                   hide_time = True,
                                   default = default,
                                   ),
                      S3OptionsFilter("result",
                                      options = OrderedDict(result_options),
                                      hidden = True,
                                      ),
                      S3DateFilter("result_date",
                                   label = T("Result Date"),
                                   hidden = True,
                                   ),
                      ]
    if site_id:
        # Better to use text filter for site name?
        # - better scalability, but cannot select multiple
        filter_widgets.append(
            S3OptionsFilter("site_id", hidden=True))
    if disease_id:
        filter_widgets.append(
            S3OptionsFilter("disease_id",
                            options = lambda: s3_get_filter_opts("disease_disease"),
                            hidden=True,
                            ))
    if demographic_id:
        filter_widgets.append(
            S3OptionsFilter("demographic_id",
                            options = lambda: s3_get_filter_opts("disease_demographic"),
                            hidden=True,
                            ))

    # Report options
    facts = ((T("Number of Tests"), "count(id)"),
             )
    axes = ["result",
            "site_id",
            #"disease_id",
            ]
    if disease_id:
        axes.append(disease_id)
    report_options = {
        "rows": axes,
        "cols": axes,
        "fact": facts,
        "defaults": {"rows": axes[1],
                     "cols": axes[0],
                     "fact": facts[0],
                     "totals": True,
                     },
        }

    s3db.configure("disease_case_diagnostics",
                   insertable = False,
                   editable = False,
                   deletable = False,
                   crud_form = crud_form,
                   filter_widgets = filter_widgets,
                   list_fields = list_fields,
                   report_options = report_options,
                   orderby = "disease_case_diagnostics.probe_date desc",
                   )

    # Custom callback to auto-update test station daily reports
    s3db.add_custom_callback("disease_case_diagnostics",
                             "onaccept",
                             case_diagnostics_onaccept,
                             )

    # Custom REST methods
    from ..cwa import TestResultRegistration
    s3db.set_method("disease_case_diagnostics",
                    method = "register",
                    action = TestResultRegistration,
                    )
    s3db.set_method("disease_case_diagnostics",
                    method = "certify",
                    action = TestResultRegistration,
                    )
    s3db.set_method("disease_case_diagnostics",
                    method = "cwaretry",
                    action = TestResultRegistration,
                    )

    crud_strings = current.response.s3.crud_strings
    crud_strings["disease_case_diagnostics"] = Storage(
        label_create = T("Register Test Result"),
        title_display = T("Test Result"),
        title_list = T("Test Results"),
        title_update = T("Edit Test Result"),
        title_upload = T("Import Test Results"),
        label_list_button = T("List Test Results"),
        label_delete_button = T("Delete Test Result"),
        msg_record_created = T("Test Result added"),
        msg_record_modified = T("Test Result updated"),
        msg_record_deleted = T("Test Result deleted"),
        msg_list_empty = T("No Test Results currently registered"),
        )

# -------------------------------------------------------------------------
def disease_case_diagnostics_controller(**attr):

    T = current.T
    s3 = current.response.s3

    # Enable bigtable features
    current.deployment_settings.base.bigtable = True

    ## Custom prep
    #standard_prep = s3.prep
    #def prep(r):
    #    # Call standard prep
    #    result = standard_prep(r) if callable(standard_prep) else True
    #
    #    return result
    #s3.prep = prep

    standard_postp = s3.postp
    def custom_postp(r, output):

        # Call standard postp
        if callable(standard_postp):
            output = standard_postp(r, output)

        if isinstance(output, dict):

            record = r.record
            method = r.method

            # Add Register-button in list and read views
            key, label = None, None
            permitted = current.auth.s3_has_permission("create", r.table)
            if permitted:
                if not record and not method:
                    key, label = "add_btn", T("Register Test Result")
                elif record and method in (None, "read"):
                    key, label = "list_btn", T("Register another test result")
            if key:
                regbtn = S3CRUD.crud_button(label = label,
                                            _href = r.url(id="", method="register"),
                                            )
                output["buttons"] = {key: regbtn}

        return output
    s3.postp = custom_postp

    return attr

# -------------------------------------------------------------------------
def disease_testing_report_resource(r, tablename):

    from core import S3CalendarWidget, S3DateFilter, S3TextFilter

    T = current.T
    settings = current.deployment_settings

    db = current.db
    s3db = current.s3db

    table = s3db.disease_testing_report

    list_fields = ["date",
                   (T("Test Station ID"), "site_id$org_facility.code"),
                   "site_id",
                   #"disease_id",
                   "tests_total",
                   "tests_positive",
                   "comments",
                   ]

    # No add-link on disease_id
    field = table.disease_id
    field.comment = None

    # If there is only one disease, set as default + hide field
    dtable = s3db.disease_disease
    rows = db(dtable.deleted == False).select(dtable.id,
                                              cache = s3db.cache,
                                              limitby = (0, 2),
                                              )
    if len(rows) == 1:
        field.default = rows[0].id
        field.readable = field.writable = False
    else:
        list_fields.insert(1, "disease_id")

    if r.tablename == "disease_testing_report" and r.record and \
       settings.get_disease_testing_report_by_demographic() and \
       r.method != "read" and \
       current.auth.s3_has_permission("update", table, record_id=r.record.id):
        # Hide totals in create/update form
        table.tests_total.readable = False
        table.tests_positive.readable = False

    # Order testing sites selector by obsolete-flag
    field = table.site_id
    stable = current.s3db.org_site
    field.requires = IS_ONE_OF(db, "org_site.site_id",
                               field.represent,
                               instance_types = ["org_facility"],
                               orderby = (stable.obsolete, stable.name),
                               sort = False,
                               )

    # Check how many sites are selectable
    selectable = [o[0] for o in field.requires.options() if o[0]]
    if len(selectable) == 1:
        # If only one selectable site, set as default + make r/o
        field.default = selectable[0]
        field.writable = False
    else:
        # If one active site, set it as default, but leave selectable
        query = (stable.site_id.belongs(selectable)) & \
                (stable.obsolete == False)
        active = db(query).select(stable.site_id, limitby = (0, 2))
        if len(active) == 1:
            field.default = active.first().site_id

    # Allow daily reports up to 3 months back in time (1st of month)
    today = current.request.utcnow.date()
    earliest = today - relativedelta(months=3, day=1)
    field = table.date
    field.requires = IS_UTC_DATE(minimum = earliest,
                                 maximum = today,
                                 )
    field.widget = S3CalendarWidget(minimum = earliest,
                                    maximum = today,
                                    month_selector = True,
                                    )

    # Limit report by setting date filter default start date
    if r.method == "report":
        start = current.request.utcnow.date() - relativedelta(weeks=1)
        default = {"~.date__ge": {"ge": start}}
    else:
        default = None

    filter_widgets = [S3TextFilter(["site_id$name",
                                    "site_id$org_facility.code",
                                    "comments",
                                    ],
                                   label = T("Search"),
                                   ),
                      S3DateFilter("date",
                                   default = default,
                                   ),
                      ]

    # Daily reports only writable for ORG_ADMINs of test stations
    writable = current.auth.s3_has_roles(["ORG_ADMIN", "TEST_PROVIDER"], all=True)

    s3db.configure("disease_testing_report",
                   filter_widgets = filter_widgets,
                   list_fields = list_fields,
                   insertable = writable,
                   editable = writable,
                   deletable = writable,
                   )

# -------------------------------------------------------------------------
def disease_testing_report_controller(**attr):

    # Enable bigtable features
    current.deployment_settings.base.bigtable = True

    return attr

# -------------------------------------------------------------------------
def disease_testing_demographic_resource(r, tablename):

    # Limit report by setting date filter default start date
    if r.method == "report":
        start = current.request.utcnow.date() - relativedelta(weeks=1)
        default = {"~.report_id$date__ge": {"ge": start}}
    else:
        default = None

    from core import S3DateFilter, \
                     S3OptionsFilter, \
                     S3TextFilter, \
                     s3_get_filter_opts

    filter_widgets = [S3TextFilter(["report_id$site_id$name",
                                    "report_id$comments",
                                    ],
                                   label = current.T("Search"),
                                   ),
                      S3DateFilter("report_id$date",
                                   default = default,
                                   ),
                      S3OptionsFilter("demographic_id",
                                      options = s3_get_filter_opts("disease_demographic"),
                                      hidden = True,
                                      ),
                      ]

    current.s3db.configure("disease_testing_demographic",
                           filter_widgets = filter_widgets,
                           )

# -------------------------------------------------------------------------
def disease_testing_demographic_controller(**attr):

    # Enable bigtable features
    current.deployment_settings.base.bigtable = True

    return attr

# END =========================================================================
