# -*- coding: utf-8 -*-

""" Asynchronous Task Execution
    - falls back to Synchronous if no workers are alive

    To run a worker node: python web2py.py -K eden
    or use UWSGI's 'Mule'
    or use nssm on Win32: http://web2py.com/books/default/chapter/29/13/deployment-recipes#Using-nssm-to-run-as-a-Windows-service

    NB
        Need WEB2PY_PATH environment variable to be defined (e.g. /etc/profile)
        Tasks need to be defined outside conditional model loads (e.g. models/tasks.py)
        Avoid passing state into the async call as state may change before the message is executed (race condition)

    Old screencast: http://www.vimeo.com/27478796

    @requires: U{B{I{gluon}} <http://web2py.com>}

    @copyright: 2011-2021 (c) Sahana Software Foundation
    @license: MIT

    Permission is hereby granted, free of charge, to any person
    obtaining a copy of this software and associated documentation
    files (the "Software"), to deal in the Software without
    restriction, including without limitation the rights to use,
    copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the
    Software is furnished to do so, subject to the following
    conditions:

    The above copyright notice and this permission notice shall be
    included in all copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
    EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
    OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
    NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
    HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
    WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
    FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
    OTHER DEALINGS IN THE SOFTWARE.
"""

__all__ = ("S3Task",)

import datetime
import json

from gluon import current, IS_EMPTY_OR, IS_INT_IN_RANGE
from gluon.storage import Storage

from .calendar import S3DateTime
from .validators import IS_UTC_DATETIME

# -----------------------------------------------------------------------------
class S3Task(object):
    """ Asynchronous Task Execution """

    TASK_TABLENAME = "scheduler_task"

    # -------------------------------------------------------------------------
    def __init__(self):

        migrate = current.deployment_settings.get_base_migrate()
        tasks = current.response.s3.tasks

        # Instantiate Scheduler
        try:
            from gluon.scheduler import Scheduler
        except ImportError:
            # Warning should already have been given by eden_update_check.py
            self.scheduler = None
        else:
            self.scheduler = Scheduler(current.db,
                                       tasks,
                                       migrate = migrate,
                                       #use_spawn = True # Possible subprocess method with Py3
                                       )

    # -------------------------------------------------------------------------
    def configure_tasktable_crud(self,
                                 task = None,
                                 function = None,
                                 args = None,
                                 vars = None,
                                 period = 3600, # seconds, so 1 hour
                                 status_writable = False,
                                 ):
        """
            Configure the task table for interactive CRUD,
            setting defaults, widgets and hiding unnecessary fields

            @param task: the task name (will use a UUID if omitted)
            @param function: the function name (won't hide if omitted)
            @param args: the function position arguments
            @param vars: the function named arguments
            @param period: the default period for tasks
            @param status_writable: make status and next run time editable
        """

        from ..ui import S3CalendarWidget, S3TimeIntervalWidget

        T = current.T
        NONE = current.messages["NONE"]
        UNLIMITED = T("unlimited")

        tablename = self.TASK_TABLENAME
        table = current.db[tablename]

        # Configure start/stop time fields
        for fn in ("start_time", "stop_time", "next_run_time"):
            field = table[fn]
            field.represent = lambda dt: \
                            S3DateTime.datetime_represent(dt, utc=True)
            set_min = set_max = None
            if fn == "start_time":
                field.requires = IS_UTC_DATETIME()
                set_min = "#scheduler_task_stop_time"
            elif fn == "stop_time":
                field.requires = IS_EMPTY_OR(IS_UTC_DATETIME())
                set_max = "#scheduler_task_start_time"
            else:
                field.requires = IS_UTC_DATETIME()
            field.widget = S3CalendarWidget(past = 0,
                                            set_min = set_min,
                                            set_max = set_max,
                                            timepicker = True,
                                            )

        # Task name (default use UUID)
        if task is None:
            from uuid import uuid4
            task = str(uuid4())
        field = table.task_name
        field.default = task
        field.readable = field.writable = False

        # Function (default+hide if specified as parameter)
        if function:
            field = table.function_name
            field.default = function
            field.readable = field.writable = False

        # Args and vars
        if isinstance(args, list):
            field = table.args
            field.default = json.dumps(args)
            field.readable = field.writable = False
        else:
            field.default = "[]"
        if isinstance(vars, dict):
            field = table.vars
            field.default = json.dumps(vars)
            field.readable = field.writable = False
        else:
            field.default = {}

        # Fields which are always editable
        field = table.repeats
        field.label = T("Repeat")
        field.comment = T("times (0 = unlimited)")
        field.default = 0
        field.represent = lambda opt: \
            opt and "%s %s" % (opt, T("times")) or \
            opt == 0 and UNLIMITED or \
            NONE

        field = table.period
        field.label = T("Run every")
        field.default = period
        field.widget = S3TimeIntervalWidget.widget
        field.requires = IS_INT_IN_RANGE(0, None)
        field.represent = S3TimeIntervalWidget.represent
        field.comment = None

        table.timeout.default = 600
        table.timeout.represent = lambda opt: \
                                    opt and "%s %s" % (opt, T("seconds")) or \
                                    opt == 0 and UNLIMITED or \
                                    NONE

        # Always use "default" controller (web2py uses current controller),
        # otherwise the anonymous worker does not pass the controller
        # permission check and gets redirected to login before it reaches
        # the task function which does the s3_impersonate
        field = table.application_name
        field.default = "%s/default" % current.request.application
        field.readable = field.writable = False

        # Hidden fields
        hidden = ("uuid",
                  "broadcast",
                  "group_name",
                  "times_run",
                  "assigned_worker_name",
                  "sync_output",
                  "times_failed",
                  "cronline",
                  )
        for fn in hidden:
            table[fn].readable = table[fn].writable = False

        # Optionally editable fields
        fields = ("next_run_time", "status", "prevent_drift")
        for fn in fields:
            table[fn].readable = table[fn].writable = status_writable

        list_fields = ["id",
                       "enabled",
                       "start_time",
                       "repeats",
                       "period",
                       (T("Last run"), "last_run_time"),
                       (T("Last status"), "status"),
                       (T("Next run"), "next_run_time"),
                       "stop_time"
                       ]
        if not function:
            list_fields[1:1] = ["task_name", "function_name"]

        current.s3db.configure(tablename,
                               list_fields = list_fields,
                               )

        response = current.response
        if response:
            response.s3.crud_strings[tablename] = Storage(
                label_create = T("Create Job"),
                title_display = T("Job Details"),
                title_list = T("Job Schedule"),
                title_update = T("Edit Job"),
                label_list_button = T("List Jobs"),
                msg_record_created = T("Job added"),
                msg_record_modified = T("Job updated"),
                msg_record_deleted = T("Job deleted"),
                msg_list_empty = T("No jobs configured yet"),
                msg_no_match = T("No jobs configured"))

    # -------------------------------------------------------------------------
    # API Function run within the main flow of the application
    # -------------------------------------------------------------------------
    def run_async(self, task, args=None, vars=None, timeout=300):
        """
            Wrapper to call an asynchronous task.
            - run from the main request

            @param task: The function which should be run
                         - async if a worker is alive
            @param args: The list of unnamed args to send to the function
            @param vars: The list of named vars to send to the function
            @param timeout: The length of time available for the task to complete
                            - default 300s (5 mins)
        """

        if args is None:
            args = []
        if vars is None:
            vars = {}

        # Check that task is defined (and callable)
        tasks = current.response.s3.tasks
        if not tasks or not callable(tasks.get(task)):
            return False

        # Check that args/vars are JSON-serializable
        try:
            json.dumps(args)
        except (ValueError, TypeError):
            msg = "S3Task.run_async args not JSON-serializable: %s" % args
            current.log.error(msg)
            raise
        try:
            json.dumps(vars)
        except (ValueError, TypeError):
            msg = "S3Task.run_async vars not JSON-serializable: %s" % vars
            current.log.error(msg)
            raise

        # Run synchronously if scheduler not running
        if not self._is_alive():
            tasks[task](*args, **vars)
            return None # No task ID in this case

        # Queue the task (async)
        try:
            # Add the current user to the vars
            vars["user_id"] = current.auth.user.id
        except AttributeError:
            pass
        queued = self.scheduler.queue_task(task,
                                           pargs = args,
                                           pvars = vars,
                                           application_name = "%s/default" % \
                                                              current.request.application,
                                           function_name = task,
                                           timeout = timeout,
                                           )

        # Return task ID so that status can be polled
        return queued.id

    # -------------------------------------------------------------------------
    def schedule_task(self,
                      task,
                      args = None, # args to pass to the task
                      vars = None, # vars to pass to the task
                      function_name = None,
                      start_time = None,
                      next_run_time = None,
                      stop_time = None,
                      repeats = None,
                      retry_failed = None,
                      period = None,
                      timeout = None,
                      enabled = None, # None = Enabled
                      group_name = None,
                      ignore_duplicate = False,
                      sync_output = 0,
                      user_id = True
                      ):
        """
            Schedule a task in web2py Scheduler

            @param task: name of the function/task to be scheduled
            @param args: args to be passed to the scheduled task
            @param vars: vars to be passed to the scheduled task
            @param function_name: function name (if different from task name)
            @param start_time: start_time for the scheduled task
            @param next_run_time: next_run_time for the the scheduled task
            @param stop_time: stop_time for the the scheduled task
            @param repeats: number of times the task to be repeated (0=unlimited)
            @param retry_failed: number of times the task to be retried (-1=unlimited)
            @param period: time period between two consecutive runs (seconds)
            @param timeout: set timeout for a running task
            @param enabled: enabled flag for the scheduled task
            @param group_name: group_name for the scheduled task
            @param ignore_duplicate: disable or enable duplicate checking
            @param sync_output: sync output every n seconds (0 = disable sync)
            @param user_id: Add the user_id to task vars if logged in
        """

        if args is None:
            args = []
        if vars is None:
            vars = {}

        if not ignore_duplicate and self._duplicate_task_exists(task, args, vars):
            # if duplicate task exists, do not insert a new one
            current.log.warning("Duplicate Task, Not Inserted", value=task)
            return False

        kwargs = {}

        if function_name is None:
            function_name = task

        # storing valid keyword arguments only if they are provided
        if start_time:
            kwargs["start_time"] = start_time

        if next_run_time:
            kwargs["next_run_time"] = next_run_time
        elif start_time:
            # default it to start_time
            kwargs["next_run_time"] = start_time

        if stop_time:
            kwargs["stop_time"] = stop_time
        elif start_time:
            # default it to one day ahead of given start_time
            if not isinstance(start_time, datetime.datetime):
                start_time = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
            stop_time = start_time + datetime.timedelta(days=1)

        if repeats is not None:
            kwargs["repeats"] = repeats

        if retry_failed is not None:
            kwargs["retry_failed"] = retry_failed

        if period:
            kwargs["period"] = period

        if timeout:
            kwargs["timeout"] = timeout

        if enabled != None:
            # NB None => enabled
            kwargs["enabled"] = enabled

        if group_name:
            kwargs["group_name"] = group_name

        if sync_output != 0:
            kwargs["sync_output"] = sync_output

        if user_id:
            auth = current.auth
            if auth.is_logged_in():
                # Add the current user to the vars
                vars["user_id"] = auth.user.id

        # Add to DB for pickup by Scheduler task
        # @ToDo: Switch to API: self.scheduler.queue_task()
        task_id = current.db.scheduler_task.insert(application_name = "%s/default" % \
                                                   current.request.application,
                                                   task_name = task,
                                                   function_name = function_name,
                                                   args = json.dumps(args),
                                                   vars = json.dumps(vars),
                                                   **kwargs)
        return task_id

    # -------------------------------------------------------------------------
    @staticmethod
    def _duplicate_task_exists(task, args, vars):
        """
            Checks if given task already exists in the Scheduler and both coincide
            with their execution time

            @param task: name of the task function
            @param args: the job position arguments (list)
            @param vars: the job named arguments (dict)
        """

        db = current.db
        ttable = db.scheduler_task

        args_json = json.dumps(args)

        query = ((ttable.function_name == task) & \
                 (ttable.args == args_json) & \
                 (ttable.status.belongs(["RUNNING", "QUEUED", "ALLOCATED"])))
        jobs = db(query).select(ttable.vars)
        for job in jobs:
            job_vars = json.loads(job.vars)
            if job_vars == vars:
                return True
        return False

    # -------------------------------------------------------------------------
    @staticmethod
    def _is_alive():
        """
            Returns True if there is at least 1 active worker to run scheduled tasks
            - run from the main request

            NB Can't run this 1/request at the beginning since the tables
               only get defined in zz_last
        """

        #if self.scheduler:
        #    return self.scheduler.is_alive()
        #else:
        #    return False

        db = current.db
        table = db.scheduler_worker

        now = datetime.datetime.now()
        offset = datetime.timedelta(minutes = 1)

        query = (table.last_heartbeat > (now - offset))
        cache = current.response.s3.cache
        worker_alive = db(query).select(table.id,
                                        limitby = (0, 1),
                                        cache = cache,
                                        ).first()

        return True if worker_alive else False

    # -------------------------------------------------------------------------
    @staticmethod
    def reset(task_id):
        """
            Reset the status of a task to QUEUED after FAILED

            @param task_id: the task record ID
        """

        db = current.db
        ttable = db.scheduler_task

        query = (ttable.id == task_id) & (ttable.status == "FAILED")
        task = db(query).select(ttable.id,
                                limitby = (0, 1)
                                ).first()
        if task:
            task.update_record(status = "QUEUED")

    # =========================================================================
    # Functions run within the Task itself
    # =========================================================================
    @staticmethod
    def authenticate(user_id):
        """
            Activate the authentication passed from the caller to this new request
            - run from within the task

            NB This is so simple that we don't normally run via this API
               - this is just kept as an example of what needs to happen within the task
        """

        current.auth.s3_impersonate(user_id)

# END =========================================================================
