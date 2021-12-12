# -*- coding: utf-8 -*-

from gluon import redirect, URL
from core import CustomController

# =============================================================================
class index(CustomController):
    """ Custom Home Page """

    def __call__(self):

        redirect(URL(c="setup", f="index"))

# END =========================================================================
