"""
    Shelter overview

    License: MIT
"""

from gluon import current, \
                  A, DIV, P, TABLE, TD, TR

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
        from dateutil.relativedelta import relativedelta
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

        output["item"] = overview
        current.response.view = self._view(r, "display.html")

        return output

# END =========================================================================
