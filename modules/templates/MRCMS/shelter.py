"""
    Shelter overview

    License: MIT
"""

from dateutil.relativedelta import relativedelta
from gluon import current, \
                  A, DIV, I, P, TABLE, TD, TR, TH, TAG
from gluon.storage import Storage

from core import CRUDMethod, PresenceRegistration

# =============================================================================
class ShelterOverview(CRUDMethod):
    """
        Overview page for a single shelter
    """

    def apply_method(self, r, **attr):
        # TODO docstring
        # TODO cleanup

        if not r.record:
            r.error(405, current.ERROR.BAD_METHOD)

        if r.http == "GET" and r.representation == "html":
            output = self.legacy_overview(r, **attr)
        else:
            r.error(405, current.ERROR.BAD_METHOD)

        return output

    # -------------------------------------------------------------------------
    def legacy_overview(self, r, **attr):
        # TODO docstring
        # TODO refactor

        T = current.T
        db = current.db
        s3db = current.s3db

        rtable = s3db.cr_shelter_registration
        utable = s3db.cr_shelter_unit
        ctable = s3db.dvr_case
        stable = s3db.dvr_case_status

        output = {}

        record = r.record
        if not record:
            return output

        shelter_id = record.id

        # Get nostats flags
        ftable = s3db.dvr_case_flag
        query = (ftable.nostats == True) & \
                (ftable.deleted == False)
        rows = db(query).select(ftable.id)
        nostats = set(row.id for row in rows)

        # Get person_ids with nostats-flags
        # (=persons who are registered as residents, but not BEA responsibility)
        if nostats:
            ltable = s3db.dvr_case_flag_case
            query = (ltable.flag_id.belongs(nostats)) & \
                    (ltable.deleted == False)
            rows = db(query).select(ltable.person_id)
            exclude = set(row.person_id for row in rows)
        else:
            exclude = set()

        # Count total shelter registrations for non-BEA persons
        query = (rtable.person_id.belongs(exclude)) & \
                (rtable.shelter_id == shelter_id) & \
                (rtable.deleted != True)
        other_total = db(query).count()

        # Count number of shelter registrations for this shelter,
        # grouped by transitory-status of the housing unit
        left = utable.on(utable.id == rtable.shelter_unit_id)
        query = (~(rtable.person_id.belongs(exclude))) & \
                (rtable.shelter_id == shelter_id) & \
                (rtable.deleted != True)
        count = rtable.id.count()
        rows = db(query).select(utable.transitory,
                                count,
                                groupby = utable.transitory,
                                left = left,
                                )
        transitory = 0
        regular = 0
        for row in rows:
            if row[utable.transitory]:
                transitory += row[count]
            else:
                regular += row[count]
        total = transitory + regular

        # Children
        EIGHTEEN = r.utcnow - relativedelta(years=18)
        ptable = s3db.pr_person
        query = (ptable.date_of_birth > EIGHTEEN) & \
                (~(ptable.id.belongs(exclude))) & \
                (ptable.id == rtable.person_id) & \
                (rtable.shelter_id == shelter_id)
        count = ptable.id.count()
        row = db(query).select(count).first()
        children = row[count]

        CHILDREN = TR(TD(T("Children")),
                    TD(children),
                    )

        # Families on-site
        gtable = s3db.pr_group
        mtable = s3db.pr_group_membership
        join = [mtable.on((~(mtable.person_id.belongs(exclude))) & \
                        (mtable.group_id == gtable.id) & \
                        (mtable.deleted != True)),
                rtable.on((rtable.person_id == mtable.person_id) & \
                        (rtable.shelter_id == shelter_id) & \
                        (rtable.deleted != True)),
                ]
        query = (gtable.group_type == 7) & \
                (gtable.deleted != True)

        rows = db(query).select(gtable.id,
                                having = (mtable.id.count() > 1),
                                groupby = gtable.id,
                                join = join,
                                )
        families = len(rows)
        FAMILIES = TR(TD(T("Families")),
                    TD(families),
                    )

        TOTAL = TR(TD(T("Current Population##shelter")),
                TD(total),
                _class="dbstats-total",
                )
        TRANSITORY = TR(TD(T("in staging area")),
                        TD(transitory),
                        _class="dbstats-sub",
                        )
        REGULAR = TR(TD(T("in housing units")),
                    TD(regular),
                    _class="dbstats-sub",
                    )

        #OTHER = TR(TD(T("Population Other")),
        #           TD(other_total),
        #           _class="dbstats-extra",
        #           )

        # Get the IDs of open case statuses
        query = (stable.is_closed == False) & (stable.deleted != True)
        rows = db(query).select(stable.id)
        OPEN = set(row.id for row in rows)

        # Count number of external persons
        ftable = s3db.dvr_case_flag
        ltable = s3db.dvr_case_flag_case
        left = [ltable.on((ltable.flag_id == ftable.id) & \
                        (ltable.deleted != True)),
                ctable.on((ctable.person_id == ltable.person_id) & \
                        (~(ctable.person_id.belongs(exclude))) & \
                        (ctable.status_id.belongs(OPEN)) & \
                        ((ctable.archived == False) | (ctable.archived == None)) & \
                        (ctable.deleted != True)),
                rtable.on((rtable.person_id == ltable.person_id) & \
                        (rtable.deleted != True)),
                ]
        query = (ftable.is_external == True) & \
                (ftable.deleted != True) & \
                (ltable.id != None) & \
                (ctable.id != None) & \
                (rtable.shelter_id == shelter_id)
        count = ctable.id.count()
        rows = db(query).select(count, left=left)
        external = rows.first()[count] if rows else 0

        EXTERNAL = TR(TD(T("External (Hospital / Police)")),
                    TD(external),
                    )

        # Get the number of free places in the BEA
        # => Non-BEA registrations do not occupy BEA capacity,
        #    so need to re-add the total here:
        free = (record.available_capacity or 0) + other_total
        FREE = TR(TD(T("Free places")),
                TD(free),
                _class="dbstats-total",
                )

        # Show Check-in/Check-out action only if user is permitted
        # to update shelter registrations (NB controllers may be
        # read-only, therefore checking against default here):
        if PresenceRegistration.permitted("cr_shelter", record.site_id):
            # Action button for presence registration
            cico = A(T("Presence Registration"),
                    _href=r.url(method="presence"),
                    _class="action-btn dashboard-action",
                    )
        else:
            cico = ""

        # Generate profile header HTML
        overview = DIV(P(record.comments or ""),
                       TABLE(TR(TD(TABLE(TOTAL,
                                         TRANSITORY,
                                         REGULAR,
                                         CHILDREN,
                                         FAMILIES,
                                         EXTERNAL,
                                         FREE,
                                         #OTHER,
                                         _class="dbstats",
                                         ),
                                    ),
                                ),
                            ),
                        cico,
                        _class="profile-header",
                        )

        occupancy = self.occupancy_data(record)
        overview = TAG[""](overview, occupancy)

        output["item"] = overview
        current.response.view = self._view(r, "display.html")

        return output

    # -------------------------------------------------------------------------
    def occupancy_data(self, shelter):

        db = current.db
        s3db = current.s3db

        shelter_id = shelter.id
        #site_id = shelter.site_id

        overview = DIV(_class="occupancy-overview")

        # Get all housing units for this shelter
        # - id, name, status, capacity, blocked_capacity
        utable = s3db.cr_shelter_unit
        query = (utable.shelter_id == shelter_id) & \
                (utable.deleted == False)
        units = db(query).select(utable.id,
                                 utable.name,
                                 utable.status,
                                 utable.capacity,
                                 utable.blocked_capacity,
                                 utable.transitory,
                                 orderby = utable.name,
                                 )

        # Get all active shelter registrations for any of these units
        # person_id, status, check-in date
        rtable = s3db.cr_shelter_registration
        #query = (rtable.shelter_id == shelter_id) & \
                #(rtable.registration_status != 3) & \
                #(rtable.deleted == False)

        #registrations = db(query).select(rtable.id,
                                         #rtable.person_id,
                                         #rtable.shelter_unit_id,
                                         #rtable.registration_status,
                                         #rtable.check_in_date,
                                         #)
        #print(registrations)
        #persons = {r.person_id: None for r in registrations}
        #person_ids = set(persons.keys())

        # Get all persons
        # pe_label, label, first_name, last_name, gender, DoB
        # Left joins:
        # pr_person_details: nationality
        # dvr_case: last_seen_on
        # pr_group_membership (filtered to case groups) <= orderby
        ptable = s3db.pr_person
        dtable = s3db.pr_person_details
        ctable = s3db.dvr_case
        mtable = s3db.pr_group_membership

        left = [dtable.on((dtable.person_id == ptable.id) & \
                          (dtable.deleted == False)),
                mtable.on((mtable.person_id == ptable.id) & \
                          (mtable.deleted == False)),
                ]
        join = [rtable.on((rtable.person_id == ptable.id) & \
                          (rtable.shelter_id == shelter_id) & \
                          (rtable.registration_status != 3) & \
                          (rtable.deleted == False)),
                ctable.on((ctable.person_id == ptable.id) & \
                          (ctable.deleted == False)),
                ]
        query = (ptable.deleted == False)
        rows = db(query).select(ptable.id,
                                ptable.pe_label,
                                ptable.last_name,
                                ptable.first_name,
                                ptable.gender,
                                ptable.date_of_birth,
                                dtable.nationality,
                                ctable.last_seen_on,
                                mtable.id,
                                mtable.group_id,
                                rtable.shelter_unit_id,
                                rtable.registration_status,
                                rtable.check_in_date,
                                join = join,
                                left = left,
                                orderby = (rtable.shelter_unit_id,
                                           mtable.group_id,
                                           ptable.last_name,
                                           ptable.date_of_birth,
                                           ~ctable.id,
                                           ~mtable.id,
                                           ~dtable.id,
                                           ),
                                )

        all_residents = {}
        for row in rows:
            registration = row.cr_shelter_registration
            unit_id = registration.shelter_unit_id
            if unit_id in all_residents:
                residents = all_residents[unit_id]
            else:
                residents = all_residents[unit_id] = []
            residents.append(row)

        occupancy_data = TABLE(_class="occupancy-data")

        for unit in units:
            unit_id = unit.id
            residents = all_residents.get(unit_id)
            self.add_residents(occupancy_data, unit, residents)

        unassigned = all_residents.get(None)
        if unassigned:
            self.add_residents(occupancy_data, None, unassigned)

        overview.append(occupancy_data)
        return overview

        #checked_in = {}
        #planned = {}
        #seen = set()
        #for row in rows:
            #person_id = row.pr_person.id
            #if person_id in seen:
                #continue
            #seen.add(person_id)
            #reg = row.cr_shelter_registration
            #unit_id = reg.shelter_unit_id
            #if reg.registration_status == 1:
                #if unit_id in planned:
                    #records = planned[unit_id]
                #else:
                    #records = planned[unit_id] = []
                #records.append(row)
            #elif reg.registration_status == 2:
                #if unit_id in checked_in:
                    #records = checked_in[unit_id]
                #else:
                    #records = checked_in[unit_id] = []
                #records.append(row)


    # -------------------------------------------------------------------------
    @classmethod
    def add_residents(cls, overview, unit, residents):

        unit_header = cls.unit_header(unit)
        overview.append(unit_header)

        c, p = [], []
        if residents:
            for row in residents:
                reg = row.cr_shelter_registration
                if reg.registration_status == 2:
                    c.append(row)
                else:
                    p.append(row)

        for i, persons in enumerate((c, p)):

            T = current.T

            if i == 0:
                # Checked-in residents
                css = "reg-checked-in"
                overview.append(cls.resident_header())
            else:
                # Planned residents
                css = "reg-planned"
                subheader = cls.unit_subheader(1, len(persons) if persons else 0, css=css)
                overview.append(subheader)

            group_id = None
            even, odd = "group-even", "group-odd"
            for person in persons:
                gid = person.pr_group_membership.group_id
                if not gid or gid != group_id:
                    group_id = gid
                    even, odd = odd, even
                overview.append(cls.resident(person, css="%s %s" % (css, even)))

            # Render placeholder rows for unoccupied capacity
            if unit and i == 0 and not unit.transitory:
                occupied = len(persons)
                total = unit.capacity
                free = total - occupied
                blocked = min(free, unit.blocked_capacity)
                free = free - blocked

                for _ in range(free):
                    empty = TR(TD(I(_class="fa fa-bed")),
                               TD(T("Available"), _colspan = 7),
                               _class = "capacity-free",
                               )
                    overview.append(empty)
                for _ in range(blocked):
                    empty = TR(TD(I(_class="fa fa-times-circle")),
                               TD(T("Unavailable"), _colspan = 7),
                               _class = "capacity-blocked",
                               )
                    overview.append(empty)

        return overview

    # -------------------------------------------------------------------------
    @staticmethod
    def unit_header(unit):

        header = DIV(unit.name if unit else T("No housing unit"),
                     _class="occupancy-unit-header",
                     )
        return TR(TD(header, _colspan=8),
                  _class="occupancy-unit",
                  )

    # -------------------------------------------------------------------------
    @staticmethod
    def unit_subheader(status, number, css=None):

        s3db = current.s3db
        rtable = s3db.cr_shelter_registration

        status_label = rtable.registration_status.represent(status)

        css_class = "occupancy-registration-status"
        if css:
            css_class = "%s %s" % (css_class, css)

        header = DIV("%s: %s" % (status_label, number),
                     _class=css_class,
                     )
        return TR(TD(header, _colspan=8),
                  _class="occupancy-subset"
                  )

    # -------------------------------------------------------------------------
    @staticmethod
    def resident_header():

        T = current.T

        return TR(TH(),
                  TH(T("ID")),
                  TH(T("Last Name")),
                  TH(T("First Name")),
                  TH(T("Gender")),
                  TH(T("Age")),
                  TH(T("Nationality")),
                  TH(T("Last seen on")),
                  _class = "occupancy-resident-header",
                  )

    # -------------------------------------------------------------------------
    @staticmethod
    def resident(row, css=None):

        s3db = current.s3db
        ptable = s3db.pr_person
        dtable = s3db.pr_person_details
        ctable = s3db.dvr_case

        person = row.pr_person
        details = row.pr_person_details
        case = row.dvr_case

        # ID and names
        represent_str = lambda s: s if s else "-"
        label = represent_str(person.pe_label)
        lname = represent_str(person.last_name)
        fname = represent_str(person.first_name)

        # Gender, age, nationality
        gender = ptable.gender.represent(person.gender)
        dob = person.date_of_birth
        now = current.request.utcnow.date()
        age = str(relativedelta(now, dob).years) if dob else "-"
        nationality = dtable.nationality.represent(details.nationality)

        # Last-seen-date
        last_seen_on = ctable.last_seen_on.represent(case.last_seen_on)

        # Registration status
        reg = row.cr_shelter_registration
        if reg.registration_status == 2:
            icon = "fa fa-bed"
        else:
            icon = "fa fa-suitcase"

        trow = TR(TD(I(_class=icon)),
                  TD(label),
                  TD(lname),
                  TD(fname),
                  TD(gender),
                  TD(age),
                  TD(nationality),
                  TD(last_seen_on),
                  _class = css,
                  )
        return trow

# END =========================================================================
