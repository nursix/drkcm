"""
    BRCMS: Beneficiary Registry and Case Management System

    License: MIT
"""

def config(settings):

    # PrePopulate data
    settings.base.prepopulate.append("BRCMS/Standard")
    settings.base.prepopulate_demo.append("BRCMS/Standard/Demo")

# END =========================================================================
