"""
    Food distribution GUI for MRCMS

    License: MIT
"""

from core import Checkpoint

# =============================================================================
class FoodDistribution(Checkpoint):

    EVENT_CLASSES = ("F",)

    @staticmethod
    def ajax_url(r):

        return r.url(None, method="register_food", representation="json")

# END =========================================================================
