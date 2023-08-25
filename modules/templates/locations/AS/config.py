from gluon import current

def config(settings):
    """
        Template settings for American Samoa
        - designed to be used in a Cascade with an application template
    """

    #T = current.T

    # Pre-Populate
    settings.base.prepopulate.append("locations/AS")

    # Restrict to specific country/countries
    settings.gis.countries.append("AS")
    # Dosable the Postcode selector in the LocationSelector
    #settings.gis.postcode_selector = False

    # L10n (Localization) settings
    settings.L10n.languages["sm"] = "Samoan"
    # Default Language (put this in custom template if-required)
    #settings.L10n.default_language = "sm"
    # Default timezone for users
    settings.L10n.timezone = "Pacific/Pago_Pago"
    # Default Country Code for telephone numbers
    settings.L10n.default_country_code = 1684

    settings.fin.currencies["USD"] = "United States Dollars"
    settings.fin.currency_default = "USD"

# END =========================================================================
