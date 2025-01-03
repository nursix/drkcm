"""
    Anonymizer rules for MRCMS

    License: MIT
"""

from core import anonymous_address, obscure_dob

# =============================================================================
def anonymize_rules():
    """ Rules to anonymize a case file """

    ANONYMOUS = "-"

    # Helper to produce an anonymous ID (pe_label)
    anonymous_id = lambda record_id, f, v: "NN%06d" % int(record_id)

    # Identity documents
    identity_documents = ("pr_identity_document", {"key": "identity_id",
                                                   "match": "id",
                                                   "fields": {"file": "remove",
                                                              },
                                                   "delete": True,
                                                   })

    # Attached documents
    documents = ("doc_document", {"key": "doc_id",
                                  "match": "doc_id",
                                  "fields": {"name": ("set", ANONYMOUS),
                                             "file": "remove",
                                             "comments": "remove",
                                             },
                                  "delete": True,
                                  })

    # Case activity updates
    activity_updates = ("dvr_case_activity_update", {"key": "case_activity_id",
                                                     "match": "id",
                                                     "fields": {"comments": ("set", ANONYMOUS),
                                                                },
                                                     "delete": True,
                                                     })

    # Response theme details
    response_details = ("dvr_response_action_theme", {"key": "action_id",
                                                      "match": "id",
                                                      "fields": {"comments": ("set", ANONYMOUS),
                                                                 },
                                                      })

    rules = [# Remove identity of beneficiary
             {"name": "default",
              "title": "Names, IDs, Reference Numbers, Contact Information, Addresses",

              "fields": {"first_name": ("set", ANONYMOUS),
                         "middle_name": "remove",
                         "last_name": ("set", ANONYMOUS),
                         "pe_label": anonymous_id,
                         "date_of_birth": obscure_dob,
                         "comments": "remove",
                         },

              "cascade": [("dvr_case", {"key": "person_id",
                                        "match": "id",
                                        "fields": {"comments": "remove",
                                                   "reference": "remove",
                                                   },
                                        }),
                          ("pr_person_tag", {"key": "person_id",
                                             "match": "id",
                                             "fields": {"value": ("set", ANONYMOUS),
                                                        },
                                             "delete": True,
                                             }),
                          ("pr_person_details", {"key": "person_id",
                                                 "match": "id",
                                                 "fields": {"education": "remove",
                                                            "occupation": "remove",
                                                            },
                                                 }),
                          ("pr_contact", {"key": "pe_id",
                                          "match": "pe_id",
                                          "fields": {"contact_description": "remove",
                                                     "value": ("set", ""),
                                                     "comments": "remove",
                                                     },
                                          "delete": True,
                                          }),
                          ("pr_contact_emergency", {"key": "pe_id",
                                                    "match": "pe_id",
                                                    "fields": {"name": ("set", ANONYMOUS),
                                                               "relationship": "remove",
                                                               "phone": "remove",
                                                               "comments": "remove",
                                                               },
                                                    "delete": True,
                                                    }),
                          ("pr_address", {"key": "pe_id",
                                          "match": "pe_id",
                                          "fields": {"location_id": anonymous_address,
                                                     "comments": "remove",
                                                     },
                                          }),
                          ("pr_identity", {"key": "person_id",
                                           "match": "id",
                                           "fields": {"value": ("set", ANONYMOUS),
                                                      "description": "remove",
                                                      "image": "remove",
                                                      "vhash": "remove",
                                                      "comments": "remove"
                                                      },
                                           "cascade": [identity_documents],
                                           "delete": True,
                                           }),
                          ("dvr_residence_status", {"key": "person_id",
                                                    "match": "id",
                                                    "fields": {"reference": ("set", ANONYMOUS),
                                                               "comments": "remove",
                                                               },
                                                    }),
                          ("dvr_service_contact", {"key": "person_id",
                                                   "match": "id",
                                                   "fields": {"organisation": "remove",
                                                              "reference": "remove",
                                                              "contact": "remove",
                                                              "phone": "remove",
                                                              "email": "remove",
                                                              "comments": "remove",
                                                              },
                                                   "delete": True,
                                                   }),
                          ],
              },

             # Remove activity details, appointments and notes
             {"name": "activities",
              "title": "Counseling Details, Appointments, Notes",
              "cascade": [("dvr_case_language", {"key": "person_id",
                                                 "match": "id",
                                                 "fields": {"comments": "remove",
                                                            },
                                                 }),
                          ("dvr_case_appointment", {"key": "person_id",
                                                    "match": "id",
                                                    "fields": {"comments": "remove",
                                                               },
                                                    }),
                          ("dvr_case_event", {"key": "person_id",
                                              "match": "id",
                                              "fields": {"comments": "remove",
                                                         },
                                              }),
                          ("dvr_case_activity", {"key": "person_id",
                                                 "match": "id",
                                                 "fields": {"subject": ("set", ANONYMOUS),
                                                            "need_details": "remove",
                                                            "outcome": "remove",
                                                            "achievement": "remove",
                                                            "activity_details": "remove",
                                                            "outside_support": "remove",
                                                            "comments": "remove",
                                                            },
                                                 "cascade": [activity_updates],
                                                 }),
                          ("dvr_response_action", {"key": "person_id",
                                                   "match": "id",
                                                   "fields": {"comments": "remove",
                                                              },
                                                   "cascade": [response_details],
                                                   }),
                          ("dvr_vulnerability", {"key": "person_id",
                                                 "match": "id",
                                                 "fields": {"comments": "remove",
                                                            "description": ("set", ANONYMOUS),
                                                            },
                                                 }),
                          ("dvr_note", {"key": "person_id",
                                        "match": "id",
                                        "fields": {"note": "remove",
                                                   },
                                        "delete": True,
                                        }),
                          ],
              },

             # Remove photos and attachments
             {"name": "documents",
              "title": "Photos and Documents",
              "cascade": [("dvr_case", {"key": "person_id",
                                        "match": "id",
                                        "cascade": [documents],
                                        }),
                          ("dvr_case_activity", {"key": "person_id",
                                                 "match": "id",
                                                 "cascade": [documents],
                                                 }),
                          ("pr_image", {"key": "pe_id",
                                        "match": "pe_id",
                                        "fields": {"image": "remove",
                                                   "url": "remove",
                                                   "description": "remove",
                                                   },
                                        "delete": True,
                                        }),
                          ],
              },
             ]

    return rules

# END =========================================================================
