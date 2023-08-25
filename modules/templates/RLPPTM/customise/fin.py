"""
    FIN module customisations for RLPPTM

    License: MIT
"""

from collections import OrderedDict

from gluon import current, A, DIV, IS_EMPTY_OR, IS_INT_IN_RANGE, TAG

from core import FS, IS_ONE_OF, s3_str

ISSUER_ORG_TYPE = "pe_id$pe_id:org_organisation.org_organisation_organisation_type.organisation_type_id"

# -------------------------------------------------------------------------
def fin_voucher_resource(r, tablename):

    T = current.T

    auth = current.auth
    has_role = auth.s3_has_role

    s3db = current.s3db
    table = s3db.fin_voucher

    # Determine form mode
    resource = r.resource
    group_voucher = resource.tablename == "fin_voucher" and \
                    r.get_vars.get("g") == "1"

    # Customise fields
    field = table.pe_id
    field.label = T("Issuer##fin")

    from core import WithAdvice
    field = table.bearer_dob
    if group_voucher:
        label = T("Group Representative Date of Birth")
        intro = "GroupDoBIntro"
    else:
        label = T("Beneficiary Date of Birth")
        intro = "BearerDoBIntro"
    field.label = label
    field.widget = WithAdvice(field.widget,
                              text = ("fin", "voucher", intro),
                              )
    if not has_role("VOUCHER_ISSUER"):
        field.readable = field.writable = False

    field = table.initial_credit
    field.label = T("Number of Beneficiaries")
    if group_voucher:
        field.default = None
        field.requires = IS_INT_IN_RANGE(1, 51,
                            error_message = T("Enter the number of beneficiaries (max %(max)s)"),
                            )
        field.readable = field.writable = True

    field = table.comments
    field.label = T("Memoranda")
    field.comment = DIV(_class="tooltip",
                        _title="%s|%s" % (T("Memoranda"),
                                          T("Notes of the Issuer"),
                                          ),
                        )
    if not has_role("VOUCHER_PROVIDER"):
        field.readable = field.writable = False

    # Custom list fields
    if has_role("VOUCHER_ISSUER"):
        list_fields = ["program_id",
                       "signature",
                       (T("Beneficiary/Representative Date of Birth"), "bearer_dob"),
                       "initial_credit",
                       "credit_spent",
                       (T("Status"), "status"),
                       "date",
                       #"valid_until",
                       "comments",
                       ]
    else:
        list_fields = ["program_id",
                       "signature",
                       (T("Status"), "status"),
                       "pe_id",
                       #(T("Issuer Type"), ISSUER_ORG_TYPE),
                       "eligibility_type_id",
                       "initial_credit",
                       "credit_spent",
                       "date",
                       #"valid_until",
                       ]

    # Report Options
    if r.method == "report":
        facts = ((T("Credit Redeemed"), "sum(credit_spent)"),
                 (T("Credit Issued"), "sum(initial_credit)"),
                 (T("Remaining Credit"), "sum(balance)"),
                 (T("Number of Vouchers"), "count(id)"),
                 )
        axes = [ISSUER_ORG_TYPE,
                "eligibility_type_id",
                "program_id",
                "status",
                "pe_id",
                ]
        report_options = {
            "rows": axes,
            "cols": axes,
            "fact": facts,
            "defaults": {"rows": axes[0],
                         "cols": axes[1],
                         "fact": facts[0],
                         "totals": True,
                         },
            }
        s3db.configure("fin_voucher",
                       report_options = report_options,
                       )

    s3db.configure("fin_voucher",
                   list_fields = list_fields,
                   orderby = "fin_voucher.date desc",
                   )

# -------------------------------------------------------------------------
def fin_voucher_controller(**attr):

    T = current.T
    s3 = current.response.s3
    settings = current.deployment_settings

    # Enable bigtable features
    settings.base.bigtable = True

    # Custom prep
    standard_prep = s3.prep
    def prep(r):
        # Call standard prep
        result = standard_prep(r) if callable(standard_prep) else True

        # Restrict data formats
        settings.ui.export_formats = None
        representation = r.representation
        ALLOWED_FORMATS = ("html", "iframe", "popup", "aadata", "json")
        if representation not in ALLOWED_FORMATS and \
            not(r.record and representation == "card"):
            r.error(403, current.ERROR.NOT_PERMITTED)

        is_program_manager = current.auth.s3_has_role("PROGRAM_MANAGER")

        db = current.db
        s3db = current.s3db

        # Check which programs and organisations the user can issue vouchers for
        program_ids, org_ids, pe_ids = s3db.fin_voucher_permitted_programs(mode="issuer")

        resource = r.resource
        table = resource.table

        if program_ids and org_ids:
            etypes = s3db.fin_voucher_eligibility_types(program_ids, org_ids)
            program_ids = list(etypes.keys())

        if not program_ids or not org_ids:
            # User is not permitted to issue vouchers for any programs/issuers
            resource.configure(insertable = False)

        else:
            # Limit the program selector to permitted+active programs
            field = table.program_id
            ptable = s3db.fin_voucher_program
            dbset = db(ptable.id.belongs(program_ids))
            field.requires = IS_ONE_OF(dbset, "fin_voucher_program.id",
                                       field.represent,
                                       sort = True,
                                       )
            # Default the program selector if only one program can be chosen
            if len(program_ids) == 1:
                program_id = program_ids[0]
                field.default = program_id
                field.writable = False

            # Limit the eligibility type selector to applicable types
            allow_empty = False
            if len(program_ids) == 1:
                etype_ids = etypes[program_ids[0]]
            else:
                etype_ids = []
                for item in etypes.values():
                    if item:
                        etype_ids += item
                    else:
                        allow_empty = True
                etype_ids = list(set(etype_ids)) if etype_ids else None

            field = table.eligibility_type_id
            if etype_ids is None:
                # No selectable eligibility types => hide selector
                field.readable = field.writable = False
            elif len(etype_ids) == 1 and not allow_empty:
                # Only one type selectable => default
                field.default = etype_ids[0]
                field.writable = False
            else:
                # Multiple types selectable
                ttable = s3db.fin_voucher_eligibility_type
                etset = db(ttable.id.belongs(etype_ids))
                field.requires = IS_ONE_OF(etset, "fin_voucher_eligibility_type.id",
                                           field.represent,
                                           sort = True,
                                           )
                if allow_empty:
                    field.requires = IS_EMPTY_OR(field.requires)

            # Limit the issuer selector to permitted entities
            etable = s3db.pr_pentity
            field = table.pe_id
            dbset = db(etable.pe_id.belongs(pe_ids))
            field.requires = IS_ONE_OF(dbset, "pr_pentity.pe_id",
                                       field.represent,
                                       )
            # Hide the issuer selector if only one entity can be chosen
            if len(pe_ids) == 1:
                field.default = pe_ids[0]
                field.readable = field.writable = False

        if r.interactive:

            if r.get_vars.get("g") == "1":
                s3.crud_strings["fin_voucher"]["label_create"] = T("Create Group Voucher")

            # Hide valid_until from create-form (will be set onaccept)
            field = table.valid_until
            field.readable = bool(r.record)
            field.writable = False

            # Always show number of beneficiaries
            if r.record:
                field = table.initial_credit
                field.readable = True

            # Filter Widgets
            from core import DateFilter, TextFilter
            text_fields = ["signature", "comments", "program_id$name"]
            if is_program_manager:
                text_fields.append("pe_id$pe_id:org_organisation.name")
            filter_widgets = [
                TextFilter(text_fields,
                           label = T("Search"),
                           ),
                DateFilter("date",
                           ),
                ]
            if is_program_manager:
                from core import OptionsFilter, get_filter_options
                filter_widgets.extend([
                    OptionsFilter("eligibility_type_id",
                                  hidden = True,
                                  label = T("Type of Eligibility"),
                                  ),
                    OptionsFilter(ISSUER_ORG_TYPE,
                                  hidden = True,
                                  label = T("Issuer Type"),
                                  options = lambda: get_filter_options("org_organisation_type"),
                                  ),
                    ])
            resource.configure(filter_widgets = filter_widgets,
                                )

        elif r.representation == "card":
            # Configure ID card layout
            from ..vouchers import VoucherCardLayout
            resource.configure(pdf_card_layout = VoucherCardLayout,
                               pdf_card_suffix = lambda record: \
                                    s3_str(record.signature) \
                                    if record and record.signature else None,
                               )
        return result
    s3.prep = prep

    standard_postp = s3.postp
    def custom_postp(r, output):

        # Call standard postp
        if callable(standard_postp):
            output = standard_postp(r, output)

        if not r.component and isinstance(output, dict):
            if r.record and r.method in (None, "update", "read"):

                # Custom CRUD buttons
                if "buttons" not in output:
                    buttons = output["buttons"] = {}
                else:
                    buttons = output["buttons"]

                # PDF-button
                pdf_download = A(T("Download PDF"),
                                 _href = "/%s/fin/voucher/%s.card" % (r.application, r.record.id),
                                 _class="action-btn",
                                 )

                # Render in place of the delete-button
                buttons["delete_btn"] = TAG[""](pdf_download,
                                                )
        return output
    s3.postp = custom_postp

    # Custom rheader
    from ..rheaders import rlpptm_fin_rheader
    attr["rheader"] = rlpptm_fin_rheader

    return attr


# -------------------------------------------------------------------------
def fin_voucher_debit_resource(r, tablename):

    T = current.T
    auth = current.auth
    has_role = auth.s3_has_role

    s3db = current.s3db
    table = s3db.fin_voucher_debit

    # Determine form mode
    resource = r.resource
    group_voucher = resource.tablename == "fin_voucher_debit" and \
                    r.get_vars.get("g") == "1"

    # Customise fields
    field = table.comments
    field.label = T("Memoranda")
    field.comment = DIV(_class="tooltip",
                        _title="%s|%s" % (T("Memoranda"),
                                          T("Notes of the Provider"),
                                          ),
                        )
    if not has_role("VOUCHER_PROVIDER"):
        field.readable = field.writable = False

    field = table.bearer_dob
    if group_voucher:
        label = T("Group Representative Date of Birth")
    else:
        label = T("Beneficiary Date of Birth")
    field.label = label
    if not has_role("VOUCHER_PROVIDER"):
        field.readable = field.writable = False

    field = table.quantity
    if group_voucher:
        field.default = None
        field.requires = IS_INT_IN_RANGE(1,
                            error_message = T("Enter the service quantity"),
                            )
        field.readable = field.writable = True

    field = table.balance
    field.label = T("Remaining Compensation Claims")

    # Custom list_fields
    list_fields = [(T("Date"), "date"),
                   "program_id",
                   "voucher_id$signature",
                   "quantity",
                   "status",
                   ]
    if current.auth.s3_has_roles(("PROGRAM_MANAGER", "PROGRAM_ACCOUNTANT")):
        # Include issuer and provider
        list_fields[3:3] = ["voucher_id$pe_id",
                            "pe_id",
                            ]
    if has_role("VOUCHER_PROVIDER"):
        # Include provider notes
        list_fields.append("comments")

    s3db.configure("fin_voucher_debit",
                   list_fields = list_fields,
                   )

    # Filters
    if r.interactive:
        from core import DateFilter, TextFilter
        filter_widgets = [TextFilter(["program_id$name",
                                      "signature",
                                      ],
                                     label = T("Search"),
                                     ),
                          DateFilter("date",
                                     label = T("Date"),
                                     ),
                          ]
        s3db.configure("fin_voucher_debit",
                       filter_widgets = filter_widgets,
                       )

    # Report options
    if r.method == "report":
        field = table.created_by
        field.represent = s3db.auth_UserRepresent(show_name = True,
                                                  show_email = False,
                                                  )
        facts = ((T("Total Services Rendered"), "sum(quantity)"),
                 (T("Number of Accepted Vouchers"), "count(id)"),
                 (T("Remaining Compensation Claims"), "sum(balance)"),
                 )
        axes = ["program_id",
                "status",
                ]
        has_role = auth.s3_has_role
        if has_role("PROGRAM_MANAGER"):
            axes.insert(0, "pe_id")
        if has_role("VOUCHER_PROVIDER"):
            axes.append((T("User"), "created_by"))
        report_options = {
            "rows": axes,
            "cols": axes,
            "fact": facts,
            "defaults": {"rows": axes[0],
                         "cols": None,
                         "fact": facts[0],
                         "totals": True,
                         },
            }
        s3db.configure("fin_voucher_debit",
                       report_options = report_options,
                       )

# -------------------------------------------------------------------------
def fin_voucher_debit_controller(**attr):

    T = current.T
    s3 = current.response.s3

    # Enable bigtable features
    current.deployment_settings.base.bigtable = True

    # Custom prep
    standard_prep = s3.prep
    def prep(r):
        # Call standard prep
        result = standard_prep(r) if callable(standard_prep) else True

        db = current.db
        s3db = current.s3db

        resource = r.resource

        # Catch inappropriate cancel-attempts
        record = r.record
        if record and not r.component and r.method == "cancel":
            from ..helpers import can_cancel_debit
            if not can_cancel_debit(record):
                r.unauthorised()

        has_role = current.auth.s3_has_role
        if has_role("PROGRAM_ACCOUNTANT") and not has_role("PROGRAM_MANAGER"):

            # PROGRAM_ACCOUNTANT can only see debits where they are assigned
            # for the billing process
            from ..helpers import get_role_realms
            role_realms = get_role_realms("PROGRAM_ACCOUNTANT")
            if role_realms is not None:
                query = FS("billing_id$organisation_id$pe_id").belongs(role_realms)
                resource.add_filter(query)

            # PROGRAM_ACCOUNTANT does not (need to) see cancelled debits
            resource.add_filter(FS("cancelled") == False)

        # Check which programs and organisations the user can accept vouchers for
        program_ids, org_ids, pe_ids = s3db.fin_voucher_permitted_programs(
                                                    mode = "provider",
                                                    partners_only = True,
                                                    )
        table = resource.table

        if not program_ids or not org_ids:
            # User is not permitted to accept vouchers for any programs/providers
            resource.configure(insertable = False)

        else:
            # Limit the program selector to permitted programs
            field = table.program_id
            ptable = s3db.fin_voucher_program
            dbset = db(ptable.id.belongs(program_ids))
            field.requires = IS_ONE_OF(dbset, "fin_voucher_program.id",
                                       field.represent,
                                       sort = True,
                                       )
            # Hide the program selector if only one program can be chosen
            rows = dbset.select(ptable.id, limitby=(0, 2))
            if len(rows) == 1:
                field.default = rows.first().id
                field.writable = False

            # Limit the provider selector to permitted entities
            etable = s3db.pr_pentity
            field = table.pe_id
            dbset = db(etable.pe_id.belongs(pe_ids))
            field.requires = IS_ONE_OF(dbset, "pr_pentity.pe_id",
                                       field.represent,
                                       )
            # Hide the provider selector if only one entity can be chosen
            rows = dbset.select(etable.pe_id, limitby=(0, 2))
            if len(rows) == 1:
                field.default = rows.first().pe_id
                field.readable = field.writable = False

            # Always show quantity
            if record:
                field = table.quantity
                field.readable = True

        if r.interactive:

            if r.get_vars.get("g") == "1":
                s3.crud_strings["fin_voucher_debit"]["label_create"] = T("Accept Group Voucher")

        return result
    s3.prep = prep

    # Custom rheader
    from ..rheaders import rlpptm_fin_rheader
    attr["rheader"] = rlpptm_fin_rheader

    return attr


# -------------------------------------------------------------------------
def fin_voucher_program_resource(r, tablename):

    T = current.T
    table = current.s3db.fin_voucher_program

    represent = lambda v, row=None: -v if v else current.messages["NONE"]

    field = table.credit
    field.label = T("Pending Credits")
    field.represent = represent

    field = table.compensation
    field.label = T("Pending Compensation Claims")
    field.represent = represent


# -------------------------------------------------------------------------
def fin_voucher_program_controller(**attr):

    s3 = current.response.s3

    # Enable bigtable features
    current.deployment_settings.base.bigtable = True

    # Custom prep
    standard_prep = s3.prep
    def prep(r):
        # Call standard prep
        result = standard_prep(r) if callable(standard_prep) else True

        resource = r.resource

        has_role = current.auth.s3_has_role
        if has_role("PROGRAM_ACCOUNTANT") and not has_role("PROGRAM_MANAGER"):

            # PROGRAM_ACCOUNTANT can only see programs where they are
            # assigned for a billing process
            from ..helpers import get_role_realms
            role_realms = get_role_realms("PROGRAM_ACCOUNTANT")
            if role_realms is not None:
                query = FS("voucher_billing.organisation_id$pe_id").belongs(role_realms)
                resource.add_filter(query)

        return result
    s3.prep = prep

    return attr


# -------------------------------------------------------------------------
def billing_onaccept(form):
    """
        Custom onaccept of billing:
        - make sure all invoices are owned by the accountant
            organisation (as long as they are the accountants in charge)
    """

    # Get record ID
    form_vars = form.vars
    if "id" in form_vars:
        record_id = form_vars.id
    elif hasattr(form, "record_id"):
        record_id = form.record_id
    else:
        return

    db = current.db
    s3db = current.s3db

    # Get the billing/program organisations
    table = s3db.fin_voucher_billing
    ptable = s3db.fin_voucher_program
    left = ptable.on((ptable.id == table.program_id) & \
                     (ptable.deleted == False))
    query = (table.id == record_id)
    row = db(query).select(table.id,
                           table.organisation_id,
                           ptable.organisation_id,
                           left = left,
                           limitby = (0, 1),
                           ).first()
    if not row:
        return

    # Identify the organisation to own the invoices under this process
    billing = row.fin_voucher_billing
    organisation_id = billing.organisation_id
    if not organisation_id:
        organisation_id = row.fin_voucher_program.organisation_id

    # Update the realm entity as needed
    if organisation_id:
        pe_id = s3db.pr_get_pe_id("org_organisation", organisation_id)
        itable = s3db.fin_voucher_invoice
        query = (itable.billing_id == billing.id) & \
                (itable.realm_entity != pe_id) & \
                (itable.deleted == False)
        current.auth.set_realm_entity(itable,
                                      query,
                                      entity = pe_id,
                                      force_update = True,
                                      )

        # Re-assign pending invoices
        from ..helpers import assign_pending_invoices
        assign_pending_invoices(billing.id,
                                organisation_id = organisation_id,
                                )

# -------------------------------------------------------------------------
def fin_voucher_billing_resource(r, tablename):

    s3db = current.s3db
    table = current.s3db.fin_voucher_billing

    # Color-coded representation of billing process status
    field = table.status

    from core import S3PriorityRepresent
    status_opts = s3db.fin_voucher_billing_status_opts
    field.represent = S3PriorityRepresent(status_opts,
                                          {"SCHEDULED": "lightblue",
                                           "IN PROGRESS": "amber",
                                           "ABORTED": "black",
                                           "COMPLETE": "green",
                                           }).represent

    # Custom onaccept to maintain realm-assignment of invoices
    # when accountant organisation changes
    s3db.add_custom_callback("fin_voucher_billing",
                             "onaccept",
                             billing_onaccept,
                             )


# -------------------------------------------------------------------------
def claim_create_onaccept(form):
    """
        Custom create-onaccept for claim to notify the provider
        accountant about the new claim
    """

    # Get record ID
    form_vars = form.vars
    if "id" in form_vars:
        record_id = form_vars.id
    elif hasattr(form, "record_id"):
        record_id = form.record_id
    else:
        return

    T = current.T
    db = current.db
    s3db = current.s3db

    table = s3db.fin_voucher_claim
    btable = s3db.fin_voucher_billing
    ptable = s3db.fin_voucher_program
    join = [ptable.on(ptable.id == table.program_id),
            btable.on(btable.id == table.billing_id),
            ]
    query = (table.id == record_id)
    row = db(query).select(table.id,
                           table.program_id,
                           table.billing_id,
                           table.pe_id,
                           table.status,
                           btable.date,
                           ptable.name,
                           ptable.organisation_id,
                           join = join,
                           limitby = (0, 1),
                           ).first()
    if not row:
        return
    program = row.fin_voucher_program
    billing = row.fin_voucher_billing
    claim = row.fin_voucher_claim

    if claim.status != "NEW":
        return

    error = None

    # Look up the provider organisation
    pe_id = claim.pe_id
    otable = s3db.org_organisation
    provider = db(otable.pe_id == pe_id).select(otable.id,
                                                otable.name,
                                                limitby = (0, 1),
                                                ).first()

    from ..helpers import get_role_emails
    provider_accountants = get_role_emails("PROVIDER_ACCOUNTANT", pe_id)
    if not provider_accountants:
        error = "No provider accountant found"

    if not error:
        # Lookup the template variables
        app_url = current.deployment_settings.get_base_app_url()
        data = {"program": program.name,
                "date": btable.date.represent(billing.date),
                "organisation": provider.name,
                "url": "%s/fin/voucher_claim/%s" % (app_url, claim.id),
                }

        # Send the email notification
        from ..notifications import CMSNotifications
        error = CMSNotifications.send(provider_accountants,
                                      "ClaimNotification",
                                      data,
                                      module = "fin",
                                      resource = "voucher_claim",
                                      )
    if error:
        # Inform the program manager that the provider could not be notified
        msg = T("%(name)s could not be notified of new compensation claim: %(error)s") % \
                {"name": provider.name, "error": error}
        program_managers = get_role_emails("PROGRAM_MANAGER",
                                           organisation_id = program.organisation_id,
                                           )
        if program_managers:
            current.msg.send_email(to = program_managers,
                                   subject = T("Provider Notification Failed"),
                                   message = msg,
                                   )
        current.log.error(msg)
    else:
        current.log.debug("Provider '%s' notified about new compensation claim" % provider.name)

# -------------------------------------------------------------------------
def fin_voucher_claim_resource(r, tablename):

    T = current.T
    auth = current.auth
    s3db = current.s3db

    table = s3db.fin_voucher_claim

    is_provider_accountant = auth.s3_has_role("PROVIDER_ACCOUNTANT")

    if not is_provider_accountant:
        # Hide comments
        field = table.comments
        field.readable = field.writable = False

    # Color-coded representation of claim status
    field = table.status

    from core import S3PriorityRepresent
    status_opts = s3db.fin_voucher_claim_status_opts
    field.represent = S3PriorityRepresent(status_opts,
                                          {"NEW": "lightblue",
                                           "CONFIRMED": "blue",
                                           "INVOICED": "amber",
                                           "PAID": "green",
                                           }).represent

    # Custom list fields
    list_fields = [#"refno",
                   "date",
                   "program_id",
                   #"pe_id",
                   "vouchers_total",
                   "quantity_total",
                   "amount_receivable",
                   "currency",
                   "status",
                   ]
    if is_provider_accountant:
        list_fields.insert(0, "refno")
        text_fields = ["refno",
                       "comments",
                       ]
    else:
        list_fields.insert(2, "pe_id")
        text_fields = ["pe_id$pe_id:org_organisation.name",
                       ]

    # Filter widgets
    from core import TextFilter, OptionsFilter, get_filter_options
    filter_widgets = [TextFilter(text_fields,
                                 label = T("Search"),
                                 ),
                      OptionsFilter("program_id",
                                    options = lambda: get_filter_options("fin_voucher_program"),
                                    ),
                      ]

    s3db.configure("fin_voucher_claim",
                   filter_widgets = filter_widgets,
                   list_fields = list_fields,
                   )

    # PDF export method
    from ..helpers import ClaimPDF
    s3db.set_method("fin_voucher_claim",
                    method = "record",
                    action = ClaimPDF,
                    )

    s3db.add_custom_callback("fin_voucher_claim",
                             "onaccept",
                             claim_create_onaccept,
                             method = "create",
                             )

# -------------------------------------------------------------------------
def fin_voucher_claim_controller(**attr):

    T = current.T
    s3 = current.response.s3

    s3db = current.s3db

    # Custom prep
    standard_prep = s3.prep
    def prep(r):

        # Block all non-interactive update attempts
        if not r.interactive and r.http != "GET":
            r.error(403, current.ERROR.NOT_PERMITTED)

        # Call standard prep
        result = standard_prep(r) if callable(standard_prep) else True

        # Check which programs and organisations the user can accept vouchers for
        program_ids, org_ids = s3db.fin_voucher_permitted_programs(mode = "provider",
                                                                   partners_only = True,
                                                                   c = "fin",
                                                                   f = "voucher_debit",
                                                                   )[:2]
        if not program_ids or not org_ids:
            s3db.configure("fin_voucher_debit",
                           insertable = False,
                           )
        return result
    s3.prep = prep

    standard_postp = s3.postp
    def custom_postp(r, output):

        # Call standard postp
        if callable(standard_postp):
            output = standard_postp(r, output)

        if not r.component and isinstance(output, dict):
            record = r.record
            if record and r.method in (None, "update", "read"):

                # Hint that the user need to confirm the claim
                if record.status == "NEW" and \
                    all(record[fn] for fn in ("account_holder", "account_number")):
                    current.response.warning = T('You must change the status to "confirmed" before an invoice can be issued')

                # Custom CRUD buttons
                if "buttons" not in output:
                    buttons = output["buttons"] = {}
                else:
                    buttons = output["buttons"]

                # PDF-button
                pdf_download = A(T("Download PDF"),
                                 _href = "/%s/fin/voucher_claim/%s/record.pdf" % \
                                         (r.application, record.id),
                                 _class="action-btn",
                                 )

                # Render in place of the delete-button
                buttons["delete_btn"] = TAG[""](pdf_download,
                                                )
        return output
    s3.postp = custom_postp

    return attr


# -------------------------------------------------------------------------
def invoice_onsettled(invoice):
    """
        Callback to notify the provider that an invoice has been settled

        Args:
            invoice: the invoice (Row)
    """

    db = current.db
    s3db = current.s3db

    # Look up claim, invoice number, program and billing
    btable = s3db.fin_voucher_billing
    ctable = s3db.fin_voucher_claim
    itable = s3db.fin_voucher_invoice
    ptable = s3db.fin_voucher_program
    join = [ptable.on(ptable.id == ctable.program_id),
            btable.on(btable.id == ctable.billing_id),
            itable.on(itable.id == ctable.invoice_id),
            ]
    query = (ctable.invoice_id == invoice.id) & \
            (ctable.deleted == False)
    row = db(query).select(ctable.id,
                           ctable.program_id,
                           ctable.billing_id,
                           ctable.pe_id,
                           btable.date,
                           itable.invoice_no,
                           ptable.name,
                           ptable.organisation_id,
                           join = join,
                           limitby = (0, 1),
                           ).first()
    if not row:
        return
    program = row.fin_voucher_program
    billing = row.fin_voucher_billing
    claim = row.fin_voucher_claim
    invoice_no = row.fin_voucher_invoice.invoice_no

    error = None

    # Look up the provider organisation
    pe_id = claim.pe_id
    otable = s3db.org_organisation
    provider = db(otable.pe_id == pe_id).select(otable.id,
                                                otable.name,
                                                limitby = (0, 1),
                                                ).first()

    from ..helpers import get_role_emails
    provider_accountants = get_role_emails("PROVIDER_ACCOUNTANT", pe_id)
    if not provider_accountants:
        error = "No provider accountant found"

    if not error:
        # Lookup the template variables
        app_url = current.deployment_settings.get_base_app_url()
        data = {"program": program.name,
                "date": btable.date.represent(billing.date),
                "invoice": invoice_no,
                "organisation": provider.name,
                "url": "%s/fin/voucher_claim/%s" % (app_url, claim.id),
                }

        # Send the email notification
        from ..notifications import CMSNotifications
        error = CMSNotifications.send(provider_accountants,
                                      "InvoiceSettled",
                                      data,
                                      module = "fin",
                                      resource = "voucher_invoice",
                                      )
    if error:
        msg = "%s could not be notified about invoice settlement: %s"
        current.log.error(msg % (provider.name, error))
    else:
        msg = "%s notified about invoice settlement"
        current.log.debug(msg % provider.name)

# -------------------------------------------------------------------------
def invoice_create_onaccept(form):
    """
        Custom create-onaccept to assign a new invoice to an
        accountant
    """

    # Get record ID
    form_vars = form.vars
    if "id" in form_vars:
        record_id = form_vars.id
    elif hasattr(form, "record_id"):
        record_id = form.record_id
    else:
        return

    # Look up the billing ID
    table = current.s3db.fin_voucher_invoice
    query = (table.id == record_id)
    invoice = current.db(query).select(table.billing_id,
                                       limitby = (0, 1),
                                       ).first()

    if invoice:
        # Assign the invoice
        from ..helpers import assign_pending_invoices
        assign_pending_invoices(invoice.billing_id,
                                invoice_id = record_id,
                                )

# -------------------------------------------------------------------------
def fin_voucher_invoice_resource(r, tablename):

    T = current.T
    auth = current.auth
    s3db = current.s3db

    table = s3db.fin_voucher_invoice

    # Color-coded representation of invoice status
    from core import S3PriorityRepresent
    field = table.status
    try:
        status_opts = field.requires.options()
    except AttributeError:
        status_opts = []
    else:
        field.represent = S3PriorityRepresent(status_opts,
                                              {"NEW": "lightblue",
                                               "APPROVED": "blue",
                                               "REJECTED": "red",
                                               "PAID": "green",
                                               })

    is_accountant = auth.s3_has_role("PROGRAM_ACCOUNTANT")

    # Personal work list?
    if is_accountant and r.get_vars.get("mine") == "1":
        title_list = T("My Work List")
        default_status = ["NEW", "REJECTED"]
        default_hr = current.auth.s3_logged_in_human_resource()
    else:
        title_list = T("All Invoices")
        default_status = default_hr = None
    current.response.s3.crud_strings["fin_voucher_invoice"].title_list = title_list

    # Lookup method for HR filter options
    if is_accountant:
        def hr_filter_opts():
            hresource = s3db.resource("hrm_human_resource")
            rows = hresource.select(["id", "person_id"], represent=True).rows
            return {row["hrm_human_resource.id"]:
                    row["hrm_human_resource.person_id"] for row in rows}
    else:
        hr_filter_opts = None

    # Filter widgets
    from core import DateFilter, OptionsFilter, TextFilter
    if r.interactive:
        filter_widgets = [TextFilter(["invoice_no",
                                      "refno",
                                      ],
                                     label = T("Search"),
                                     ),
                          OptionsFilter("status",
                                        default = default_status,
                                        options = OrderedDict(status_opts),
                                        sort = False,
                                        ),
                          OptionsFilter("human_resource_id",
                                        default = default_hr,
                                        options = hr_filter_opts,
                                        ),
                          DateFilter("date",
                                     hidden = True,
                                     ),
                          OptionsFilter("pe_id",
                                        hidden = True,
                                        ),
                          OptionsFilter("pe_id$pe_id:org_organisation.facility.location_id$L2",
                                        hidden = True,
                                        ),
                          ]
        s3db.configure("fin_voucher_invoice",
                       filter_widgets = filter_widgets,
                       )

    # Custom create-onaccept to assign the invoice
    s3db.add_custom_callback("fin_voucher_invoice",
                             "onaccept",
                             invoice_create_onaccept,
                             method = "create",
                             )

    # PDF export method
    from ..helpers import InvoicePDF
    s3db.set_method("fin_voucher_invoice",
                    method = "record",
                    action = InvoicePDF,
                    )

    # Callback when invoice is settled
    s3db.configure("fin_voucher_invoice",
                   onsettled = invoice_onsettled,
                   )

# -------------------------------------------------------------------------
def fin_voucher_invoice_controller(**attr):

    T = current.T
    s3 = current.response.s3

    # Enable bigtable features
    current.deployment_settings.base.bigtable = True

    standard_postp = s3.postp
    def custom_postp(r, output):

        # Call standard postp
        if callable(standard_postp):
            output = standard_postp(r, output)

        if not r.component and isinstance(output, dict):
            if r.record and r.method in (None, "update", "read"):

                # Custom CRUD buttons
                if "buttons" not in output:
                    buttons = output["buttons"] = {}
                else:
                    buttons = output["buttons"]

                # PDF-button
                pdf_download = A(T("Download PDF"),
                                 _href = "/%s/fin/voucher_invoice/%s/record.pdf" % \
                                         (r.application, r.record.id),
                                 _class="action-btn",
                                 )

                # Render in place of the delete-button
                buttons["delete_btn"] = TAG[""](pdf_download,
                                                )
        return output
    s3.postp = custom_postp

    # Custom rheader
    from ..rheaders import rlpptm_fin_rheader
    attr["rheader"] = rlpptm_fin_rheader

    return attr

# END =========================================================================
