"""
    Shelter overview

    License: MIT
"""

import datetime

from dateutil.relativedelta import relativedelta

from gluon import current, URL, \
                  A, DIV, H4, H5, I, TABLE, TD, TR, TH

from core import CustomController, CRUDMethod, PresenceRegistration, s3_format_fullname

# =============================================================================
class ShelterOverview(CRUDMethod):
    """
        Overview page for a single shelter
    """

    def apply_method(self, r, **attr):
        # TODO docstring

        if not r.record:
            r.error(405, current.ERROR.BAD_METHOD)

        if r.http == "GET" and r.representation == "html":
            output = self.overview(r, **attr)
        else:
            r.error(405, current.ERROR.BAD_METHOD)

        return output

    # -------------------------------------------------------------------------
    def overview(self, r, **attr):
        # TODO docstring

        T = current.T

        record = r.record
        shelter_id = record.id

        status = ShelterStatus(shelter_id)

        # 1) Capacity
        total_capacity = status.capacity()

        cap_regular = total_capacity.get("regular", 0)
        cap_transitory = total_capacity.get("transitory", 0)
        blocked = total_capacity.get("blocked", 0)

        unallocated = total_capacity.get("unallocated", 0)
        allocable = total_capacity.get("allocable", 0)

        capacity = DIV(H5(T("Capacity")),
                       TABLE(TR(TH(T("Housing Units")), TD(cap_regular)),
                             TR(TH(T("Staging Area")), TD(cap_transitory)),
                             TR(TD(_class="spacer", _colspan=2)),
                             TR(TH(T("Free places")), TD(unallocated)),
                             TR(TH(T("Non-allocable##shelter")), TD(blocked)),
                             TR(TH(T("Allocable##shelter")), TD(allocable), _class="stats-total"),
                             ),
                       _class="shelter-overview-stats",
                       )

        # 2) Current Occupancy
        population = status.population()

        regular = population.get("regular", 0)
        transitory = population.get("transitory", 0)
        total = regular + transitory

        families = status.families()
        children = status.children(max_age=18)

        cap_total = cap_regular + cap_transitory
        regular_css = "stats-excess" if regular > cap_regular else None
        transit_css = "stats-excess" if transitory > cap_transitory else None
        total_css = "stats_excess" if total > cap_total else ""

        occupancy = DIV(H5(T("Current Population##shelter")),
                        TABLE(TR(TH(T("Housing Units")), TD(regular), _class=regular_css),
                              TR(TH(T("Staging Area")), TD(transitory), _class=transit_css),
                              TR(TH(T("Total##set")), TD(total), _class="stats-total %s" % total_css),
                              TR(TD(_class="spacer", _colspan=2)),
                              TR(TH(T("Families")), TD(families)),
                              TR(TH(T("Children")), TD(children)),
                              ),
                        _class="shelter-overview-stats",
                        )

        # 3) Change
        days = 7
        ins = status.arrivals(days=days)
        outs = status.leavings(days=days)
        planned = status.planned(days=days)

        change = "%+d" % (ins - outs)
        available = max(0, total_capacity.get("available", 0) - planned)

        change = DIV(H5(T("Change (%(days)s Days)") % {"days": days}),
                     TABLE(TR(TH(T("Arrivals##shelter")), TD(ins)),
                           TR(TH(T("Leavings##shelter")), TD(outs)),
                           TR(TH(T("Change")), TD(change)),
                           TR(TD(_class="spacer", _colspan=2)),
                           TR(TH(T("Planned")), TD(planned)),
                           TR(TH(T("Available##disposable")), TD(available), _class="stats-total"),
                           ),
                     _class="shelter-overview-stats",
                     )

        # 4) Residents Overview
        residents = self.residents_overview(record)

        # 5) Presence Registration
        if PresenceRegistration.permitted("cr_shelter", record.site_id):
            # Action button for presence registration
            registration = A(T("Presence Registration"),
                             _href = r.url(method="presence"),
                             _class = "action-btn dashboard-action",
                             )
        else:
            registration = ""

        output = {"title": T("Shelter Overview"),
                  "occupancy": occupancy,
                  "capacity": capacity,
                  "change": change,
                  "residents": residents,
                  "registration": registration,
                  }
        CustomController._view("MRCMS", "shelter_overview.html")
        return output

    # -------------------------------------------------------------------------
    def residents_overview(self, shelter):
        """
            Generates a HTML representation of residents per housing unit

            Args:
                shelter: the cr_shelter Row
            Returns:
                DIV
        """

        T = current.T
        db = current.db
        s3db = current.s3db

        shelter_id = shelter.id
        site_id = shelter.site_id

        overview = DIV(H4(T("Residents Overview")),
                       _class = "shelter-overview-residents",
                       )

        # Get all housing units for this shelter
        utable = s3db.cr_shelter_unit
        query = (utable.shelter_id == shelter_id) & \
                (utable.deleted == False)
        units = db(query).select(utable.id,
                                 utable.shelter_id,
                                 utable.name,
                                 utable.status,
                                 utable.capacity,
                                 utable.blocked_capacity,
                                 utable.transitory,
                                 orderby = utable.name,
                                 )

        # Get all active shelter registrations for this shelter
        rtable = s3db.cr_shelter_registration
        ptable = s3db.pr_person
        dtable = s3db.pr_person_details
        ctable = s3db.dvr_case
        mtable = s3db.pr_group_membership
        sptable = s3db.org_site_presence

        left = [dtable.on((dtable.person_id == ptable.id) & \
                          (dtable.deleted == False)),
                mtable.on((mtable.person_id == ptable.id) & \
                          (mtable.deleted == False)),
                sptable.on((sptable.person_id == ptable.id) & \
                           (sptable.site_id == site_id) & \
                           (sptable.deleted == False)),
                ]
        join = [rtable.on((rtable.person_id == ptable.id) & \
                          (rtable.shelter_id == shelter_id) & \
                          (rtable.registration_status != 3) & \
                          (rtable.deleted == False)),
                ctable.on((ctable.person_id == ptable.id) & \
                          (ctable.organisation_id == shelter.organisation_id) & \
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
                                sptable.status,
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

        # Split up residents by housing unit
        all_residents = {}
        for row in rows:
            registration = row.cr_shelter_registration
            unit_id = registration.shelter_unit_id
            if unit_id in all_residents:
                residents = all_residents[unit_id]
            else:
                residents = all_residents[unit_id] = []
            residents.append(row)

        # Generate residents list
        # TODO verify user is permitted to read cases of the shelter org
        # TODO if not permitted to read cases, show anonymous
        # TODO show_links always true if permitted
        show_links = current.auth.s3_has_permission("read", "pr_person", c="dvr", f="person")
        occupancy_data = TABLE(_class="residents-list")
        for unit in units:
            unit_id = unit.id
            residents = all_residents.get(unit_id)
            self.add_residents(occupancy_data, unit, residents, show_links=show_links)
        # Append residents not assigned to any housing unit
        unassigned = all_residents.get(None)
        if unassigned:
            self.add_residents(occupancy_data, None, unassigned, show_links=show_links)

        overview.append(occupancy_data)
        return overview

    # -------------------------------------------------------------------------
    @classmethod
    def add_residents(cls, overview, unit, residents, show_links=False):
        # TODO docstring

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
                css = "resident-checked-in"
                overview.append(cls.resident_header())
            elif persons:
                # Planned residents
                css = "resident-planned"
                subheader = cls.unit_subheader(1, len(persons) if persons else 0, css="residents-planned")
                overview.append(subheader)

            group_id = None
            even, odd = "group-even", "group-odd"
            for person in persons:
                gid = person.pr_group_membership.group_id
                if not gid or gid != group_id:
                    group_id = gid
                    even, odd = odd, even
                overview.append(cls.resident(person,
                                             css = "resident-data %s %s" % (css, even),
                                             show_link = show_links,
                                             ))

            # Render placeholder rows for unoccupied capacity
            if i == 0 and unit and not unit.transitory:
                # Total and free capacity
                total = unit.capacity
                if total is None:
                    total = 0
                free = max(total - len(persons), 0)

                # Blocked capacity (can be at most all free capacity)
                blocked = min(free, unit.blocked_capacity)

                # Remaining free capacity
                if unit.status == 1:
                    free = free - blocked
                else:
                    blocked = 0
                    free = 0

                for _ in range(free):
                    empty = TR(TD(I(_class="fa fa-bed")),
                               TD(T("Allocable##shelter"), _colspan = 6),
                               _class = "capacity-free",
                               )
                    overview.append(empty)
                for _ in range(blocked):
                    empty = TR(TD(I(_class="fa fa-times-circle")),
                               TD(T("Not allocable"), _colspan = 6),
                               _class = "capacity-blocked",
                               )
                    overview.append(empty)

        overview.append(TR(TD(_colspan=8),_class="residents-list-spacer"))
        return overview

    # -------------------------------------------------------------------------
    @staticmethod
    def unit_header(unit):
        # TODO docstring

        T = current.T

        if not unit:
            label = T("Not assigned")
        else:
            # TODO also show capacity/occupancy data
            if unit.status == 1:
                icon = I(_class="fa fa-check", _title=T("Available"))
            else:
                icon = I(_class="fa fa-ban", _title=T("Not allocable"))
            label = A(icon,
                      unit.name,
                      _href=URL(c = "cr",
                                f = "shelter",
                                args = [unit.shelter_id, "shelter_unit", unit.id],
                                ),
                      _class = "residents-unit-link",
                      )

        header = DIV(label,
                     _class = "residents-unit-header",
                     )
        return TR(TD(header, _colspan=7),
                  _class = "residents-unit",
                  )

    # -------------------------------------------------------------------------
    @staticmethod
    def unit_subheader(status, number, css=None):
        # TODO docstring

        s3db = current.s3db
        rtable = s3db.cr_shelter_registration

        status_label = rtable.registration_status.represent(status)

        css_class = "residents-status-header"
        if css:
            css_class = "%s %s" % (css_class, css)

        header = DIV("%s: %s" % (status_label, number),
                     _class = css_class,
                     )
        return TR(TD(header, _colspan=7),
                  _class="residents-status"
                  )

    # -------------------------------------------------------------------------
    @staticmethod
    def resident_header():
        # TODO docstring

        T = current.T

        return TR(TH(),
                  TH(T("ID")),
                  TH(T("Name")),
                  TH(T("Gender")),
                  TH(T("Age")),
                  TH(T("Nationality")),
                  TH(T("Presence")),
                  _class = "residents-header",
                  )

    # -------------------------------------------------------------------------
    @staticmethod
    def resident(row, css=None, show_link=False):
        # TODO docstring

        #T = current.T
        s3db = current.s3db

        ptable = s3db.pr_person
        dtable = s3db.pr_person_details
        #sptable = s3db.org_site_presence

        person = row.pr_person
        details = row.pr_person_details
        presence = row.org_site_presence

        # ID and names
        represent_str = lambda s: s if s else "-"
        label = represent_str(person.pe_label)
        lname = represent_str(person.last_name)
        fname = represent_str(person.first_name)
        name = s3_format_fullname(fname = fname,
                                  lname = lname,
                                  truncate = True,
                                  )
        if show_link:
            case_file = URL(c="dvr", f="person", args=[person.id])
            name = A(name, _href=case_file)

        # Gender, age, nationality
        gender = ptable.gender.represent(person.gender)
        dob = person.date_of_birth
        now = current.request.utcnow.date()
        age = str(relativedelta(now, dob).years) if dob else "-"
        nationality = dtable.nationality.represent(details.nationality)

        # Presence
        # TODO use workflow options
        if presence.status == "IN":
            p = I(_class="fa fa-check")
        elif presence.status == "OUT":
            p = I(_class="fa fa-times")
        else:
            p = "-"

        # Registration status
        reg = row.cr_shelter_registration
        if reg.registration_status == 2:
            icon = "fa fa-bed"
        else:
            icon = "fa fa-suitcase"

        trow = TR(TD(I(_class=icon)),
                  TD(label),
                  TD(name, _class="resident-name"),
                  TD(gender),
                  TD(age),
                  TD(nationality),
                  TD(p, _class="resident-presence"),
                  _class = css,
                  )
        return trow

# =============================================================================
class ShelterStatus:
    """ The current capacity/occupation status of a shelter """

    def __init__(self, shelter_id):
        """
            Args:
                shelter_id: the cr_shelter record ID
        """

        self.shelter_id = shelter_id

        # Initialize
        self._units = None
        self._capacity = None
        self._population = None

    # -------------------------------------------------------------------------
    @property
    def units(self):
        """
            The housing units of the shelter (lazy property)

            Returns:
                a dict {unit_id: Row(cr_shelter_unit)}
        """

        units = self._units
        if not units:

            table = current.s3db.cr_shelter_unit
            query = (table.shelter_id == self.shelter_id) & \
                    (table.deleted == False)
            rows = current.db(query).select(table.id,
                                            table.transitory,
                                            table.status,
                                            table.capacity,
                                            table.blocked_capacity,
                                            )
            units = self._units = {row.id: row for row in rows}

        return units

    # -------------------------------------------------------------------------
    @property
    def unit_capacity(self):
        """
            The capacities of housing units in the shelter (lazy property)

            Returns:
                a dict {unit_id: {"total": the total capacity of the unit,
                                  "excess": excess occupancy,
                                  "unallocated": number of unallocated places,
                                  "blocked": number of blocked places,
                                  "allocable": number of allocable places,
                                  }}
        """

        capacity = self._capacity

        if capacity is None:
            self._capacity = capacity = {}

            population = self.unit_population
            for unit_id, unit in self.units.items():

                total_capacity = unit.capacity if unit.status == 1 else 0

                occupied = population.get(unit_id, 0)
                free = total_capacity - occupied

                unallocated = max(0, free)
                excess_occupancy = max(0, -free)

                blocked = min(unallocated, unit.blocked_capacity)
                allocable = unallocated - blocked

                capacity[unit_id] = {"total": total_capacity,
                                     "excess": excess_occupancy,
                                     "unallocated": unallocated,
                                     "blocked": blocked,
                                     "allocable": allocable,
                                     }
        return capacity

    # -------------------------------------------------------------------------
    @property
    def unit_population(self):
        """
            The current population numbers of housing units in the
            shelter (lazy property)

            Returns: a dict {unit_id: population number}
        """

        population = self._population
        if not population:
            # Initialize
            population = {u: 0 for u in self.units}
            population[None] = 0

            # Look up registrations
            table = current.s3db.cr_shelter_registration
            query = (table.shelter_id == self.shelter_id) & \
                    (table.registration_status == 2) & \
                    (table.deleted == False)
            unit_id = table.shelter_unit_id
            num_persons = table.person_id.count()
            rows = current.db(query).select(num_persons,
                                            table.shelter_unit_id,
                                            groupby = table.shelter_unit_id,
                                            )

            # Update population numbers from registrations
            for row in rows:
                population[row[unit_id]] = row[num_persons]

            self._population = population

        return population

    # -------------------------------------------------------------------------
    def capacity(self):
        """
            Calculates the overall capacity details for the shelter

            Returns:
                a dict {"regular": total capacity in regular housing units,
                        "transitory": total capacity in transitory units,
                        "unallocated": number of unallocated places,
                        "blocked": number of blocked places,
                        "allocable": number of allocable places,
                        "available": number of available places,
                        }
        """

        units = self.units
        unit_capacity = self.unit_capacity

        capacity_regular = 0
        capacity_transitory = 0
        capacity_unallocated = 0
        capacity_blocked = 0

        allocable = excess = 0
        for unit_id, u in unit_capacity.items():

            unit = units.get(unit_id)
            if not unit:
                continue

            if unit.transitory:
                capacity_transitory += u["total"]
                continue

            capacity_regular += u["total"]
            capacity_blocked += u["blocked"]
            capacity_unallocated += u["unallocated"]

            allocable += u["allocable"]
            excess += u["excess"]

        # Unallocated places and excess occupancy in units even out
        capacity_unallocated = max(0, capacity_unallocated - excess)

        # Allocable capacity is reduced by excess occupancy too
        capacity_allocable = max(0, allocable - excess)

        # Available capacity is allocable capacity reduced by unassigned people
        unassigned = self.population().get("transitory", 0)
        capacity_available = max(0, capacity_allocable - unassigned)

        return {"regular": capacity_regular,
                "transitory": capacity_transitory,
                "unallocated": capacity_unallocated,
                "blocked": capacity_blocked,
                "allocable": capacity_allocable,
                "available": capacity_available,
                }

    # -------------------------------------------------------------------------
    def population(self):
        """
            Returns the current total population of the shelter,
            differentiated by housing unit group_type

            Returns:
                a dict {"regular": population in regular housing,
                        "transitory": population in transitory housing
                        }

        """

        population_regular = population_transitory = 0

        units = self.units
        for unit_id, population in self.unit_population.items():
            if not unit_id:
                # No assigned housing counts as transitory housing
                population_transitory += population
            else:
                unit = units.get(unit_id)
                if unit.transitory:
                    population_transitory += population
                else:
                    population_regular += population

        return {"regular": population_regular,
                "transitory": population_transitory,
                }

    # -------------------------------------------------------------------------
    def families(self):
        """
            Returns the number of families (=case groups) with more than
            one member currently registered at the shelter
        """

        db = current.db
        s3db = current.s3db

        rtable = s3db.cr_shelter_registration
        gtable = s3db.pr_group
        mtable = s3db.pr_group_membership

        # Subquery for registered people
        query = (rtable.shelter_id == self.shelter_id) & \
                (rtable.registration_status == 2) & \
                (rtable.deleted == False)
        checked_in_persons = db(query)._select(rtable.person_id)

        num_checked_in_members = mtable.person_id.count()

        join = gtable.on((gtable.id == mtable.group_id) &
                         (gtable.group_type == 7))
        query = mtable.person_id.belongs(checked_in_persons)
        rows = db(query).select(mtable.group_id,
                                join = join,
                                groupby = mtable.group_id,
                                having = (num_checked_in_members>1),
                                )
        return len(rows)

    # -------------------------------------------------------------------------
    def children(self, max_age=18):
        """
            Returns the number of children currently checked-in at the shelter

            Args:
                max_age: the maximum age of the children
            Returns:
                integer
        """

        s3db = current.s3db
        rtable = s3db.cr_shelter_registration
        ptable = s3db.pr_person

        now = datetime.datetime.utcnow()
        earliest = now - relativedelta(years=max_age)

        join = ptable.on((ptable.id == rtable.person_id) & \
                         (ptable.date_of_birth > earliest.date()))
        query = (rtable.shelter_id == self.shelter_id) & \
                (rtable.registration_status == 2) & \
                (rtable.deleted == False)
        num_children = rtable.person_id.count()
        row = current.db(query).select(num_children, join=join).first()

        return row[num_children]

    # -------------------------------------------------------------------------
    def arrivals(self, days=7):
        """
            Returns the number of people currently checked-in with a
            check-in date within the last n days

            Args:
                days: the number of days
            Returns:
                integer
        """

        today = datetime.datetime.utcnow().date()
        earliest = today - relativedelta(days=days)

        rtable = current.s3db.cr_shelter_registration
        query = (rtable.shelter_id == self.shelter_id) & \
                (rtable.registration_status == 2) & \
                (rtable.check_in_date >= earliest) & \
                (rtable.deleted == False)
        num_persons = rtable.person_id.count(distinct=True)
        row = current.db(query).select(num_persons).first()

        return row[num_persons]

    # -------------------------------------------------------------------------
    def leavings(self, days=7):
        """
            Returns the number of persons checked-out from the shelter
            within the past n days

            Args:
                days: the number of days
            Returns:
                integer
        """

        db = current.db
        s3db = current.s3db

        today = datetime.datetime.utcnow().date()
        earliest = today - relativedelta(days=days)

        # Subquery to exclude all currently checked-in people
        rtable = s3db.cr_shelter_registration
        query = (rtable.shelter_id == self.shelter_id) & \
                (rtable.registration_status == 2) & \
                (rtable.deleted == False)
        checked_in = db(query)._select(rtable.person_id)

        htable = s3db.cr_shelter_registration_history
        query = (htable.shelter_id == self.shelter_id) & \
                (~(htable.person_id.belongs(checked_in))) & \
                (htable.status == 3) & \
                (htable.previous_status == 2) & \
                (htable.date >= earliest) & \
                (htable.deleted == False)
        num_persons = htable.person_id.count(distinct=True)
        row = db(query).select(num_persons).first()

        return row[num_persons]

    # -------------------------------------------------------------------------
    def planned(self, days=7):
        """
            Returns the number of planned check-ins for the shelter

            Args:
                days: the planning horizon (latest planned check-in date)
            Returns:
                integer
        """

        today = datetime.datetime.utcnow().date()
        latest = today + relativedelta(days=days)

        rtable = current.s3db.cr_shelter_registration
        query = (rtable.shelter_id == self.shelter_id) & \
                (rtable.registration_status == 1) & \
                (rtable.check_in_date <= latest) & \
                (rtable.deleted == False)
        num_persons = rtable.person_id.count()
        row = current.db(query).select(num_persons).first()

        return row[num_persons]

# END =========================================================================
