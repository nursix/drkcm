About Controllers
=================

Controllers are functions defined inside Python scripts in the *controllers*
directory, which handle HTTP requests and produce a response.

Basic Request Routing
---------------------

Web2py maps the first three elements of the URL path to controllers as follows::

   https:// server.domain.tld / application / controller / function

The *application* refers to the subdirectory in web2py's application directory,
which in the case of Eden ASP is normally **eden** (it is possible to name it
differently, however).

The **controller** refers to a Python script in the *controllers* directory inside
the application, which is executed.

For instance::

   https:// server.domain.tld / eden / my / page

executes the script::

   controllers / my.py

The **function** refers to a *parameter-less* function defined in the controller
script, which is subsequently called. In the example above, that would mean this
function:

.. code-block:: python
   :caption: In controllers/my.py

   def page():
       ...
       return output

If the output format is HTML, the output of the controller function is further
passed to the view compiler to render the HTML which is then returned to the
client in the HTTP response.

Every controller having its own URL also means that every *page* in the web
GUI has its own controller - and Eden ASP (like any web2py application) is a
*multi-page application* (MPA). Therefore, in the context of the web GUI, the
terms "controller function" and "page" are often used synonymously.

That said, not every controller function actually produces a web page. Some
controllers exclusively serve non-interactive requests.

CRUD Controllers
----------------

The basic database functions **create**, **read**, **update** and **delete**
(short: *CRUD*) are implemented in Eden ASP as one generic function:

.. code-block:: python
   :caption: In controllers/my.py

   def page():

       return crud_controller()

This single function call automatically generates web forms to create and
update records, displays filterable tables, generates pivot table reports
and more - including a generic RESTful API for non-interactive clients.

If called without parameters, *crud_controller* will interpret *controller*
and *function* of the page URL as prefix and name of the database table which
to provide the functionality for, i.e. in the above example, CRUD functions
would be provided for the table *my_page*.

It is possible to override the default table, by passing prefix and name
explicitly to *crud_controller*, e.g.:

.. code-block:: python
   :caption: In controllers/my.py

   def page():

       return crud_controller("org", "organisation")

...will provide CRUD functions for the *org_organisation* table instead.

CRUD URLs and Methods
---------------------

The *crud_controller* extends web2py's URL schema with two additional path elements::

   https:// server.domain.tld / a / c / f / record / method

Here, the **record** is the primary key (*id*) of a record in the table served
by the crud_controller function - while the **method** specifies how to access
that record, e.g. *read* or *update*.

For instance, the following URL::

   https:// server.domain.tld / eden / org / organisation / 4 / update

...accesses the workflow to update the record #4 in the org_organisation table
(with HTTP GET to retrieve the update-form, and POST to submit it and perform
the update).

Without a *record* key, the URL accesses the table itself - as some methods, like
*create*, only make sense in the table context::

   https:// server.domain.tld / eden / org / organisation / create

The *crud_controller* comes pre-configured with a number of standard methods,
including:

========  ========  ===========================================================
Method    Target    Description
========  ========  ===========================================================
create    *Table*   Create a new record (form)
read      *Record*  View a record (read-only representation)
update    *Record*  Update a record (form)
delete    *Record*  Delete a record
list      *Table*   A tabular view of records
report    *Table*   Pivot table report with charts
timeplot  *Table*   Statistics over a time axis
map       *Table*   Show location context of records on a map
summary   *Table*   Meta-method with list, report, map on the same page (tabs)
import    *Table*   Import records from spreadsheets
organize  *Table*   Calendar-based manipulation of records
========  ========  ===========================================================

.. note::

   Both *models* and *templates* can extend the *crud_controller* by adding
   further methods, or overriding the standard methods with specific
   implementations.

Default REST API
----------------

If no *method* is specified in the URL, then the *crud_controller* will treat
the request as **RESTful** - i.e. the HTTP verb (GET, PUT, POST or DELETE)
determines the access method, e.g.::

   GET https:// server.domain.tld / eden / org / organisation / 3.xml

...produces a XML representation of the record #3 in the org_organisation table.
A *POST* request to the same URL, with XML data in the request body, will update
the record.

This **REST API** is a simpler, lower-level interface that is primarily used by
certain client-side scripts, e.g. the map viewer. It does not implement complete
CRUD workflows, but rather each function individually (stateless).

.. note::

   A data format extension in the URL is required for the REST API, as it can
   produce and process multiple data formats (extensible). Without extension,
   HTML format will be assumed and one of the interactive *read*, *update*,
   *delete* or *list* methods will be chosen to handle the request instead.

The default REST API *could* be used to integrate Eden ASP with other
applications, but normally such integrations require process-specific end
points (rather than just database end points) - which would be implemented
as explicit methods instead.
