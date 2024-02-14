"""
    Custom checkpoints for MRCMS

    License: MIT
"""

from gluon import current

from core import Checkpoint

# =============================================================================
class ActivityParticipation(Checkpoint):

    EVENT_CLASS = "B"

    @staticmethod
    def ajax_url(r):

        return r.url(None, method="register_activity", representation="json")

    # -------------------------------------------------------------------------
    @classmethod
    def get_event_types(cls, organisation_id=None, type_filter=None):
        """
            Looks up all available event types for the organisation;
            deviates from parent class method in that it restricts to event
            types linked to current activities (i.e. excluding past/future
            activities)

            Args:
                organisation_id: the organisation record ID
                type_filter: a filter query for event type selection

            Returns:
                a dict {event_type_id: event_type_row}
        """

        s3db = current.s3db
        atable = s3db.act_activity
        ttable = s3db.dvr_case_event_type

        # Get current activities
        today = current.request.utcnow.date()
        query = (atable.organisation_id == organisation_id) & \
                ((atable.date == None) | (atable.date <= today)) & \
                ((atable.end_date == None) | (atable.end_date >= today)) & \
                (atable.deleted == False)
        activity_ids = current.db(query)._select(atable.id)

        return super().get_event_types(organisation_id,
                                       ttable.activity_id.belongs(activity_ids),
                                       )

# =============================================================================
class FoodDistribution(Checkpoint):

    EVENT_CLASS = "F"

    @staticmethod
    def ajax_url(r):

        return r.url(None, method="register_food", representation="json")

# END =========================================================================
