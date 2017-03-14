=================
Release 0.167-t
=================

Presto 0.167-t is equivalent to Presto release 0.167, with some additional features and patches.

**TIMESTAMP limitations**

Presto supports a granularity of milliseconds for the ``TIMESTAMP`` data type, while Hive
supports microseconds.

**TIMESTAMP semantic changes**

The Teradata distribution of Presto fixes the semantics of the ``TIMESTAMP`` type to align with the SQL standard.

Previously, the ``TIMESTAMP`` type described an instance in time in the Presto session's time zone.
Now, Presto treats ``TIMESTAMPS`` as a set of the following fields representing wall time:

 * ``YEAR OF ERA``
 * ``MONTH OF YEAR``
 * ``DAY OF MONTH``
 * ``HOUR OF DAY``
 * ``MINUTE OF HOUR``
 * ``SECOND OF MINUTE`` - as decimal with precision 3

For that reason, a ``TIMESTAMP`` value is not linked with the session time zone in any way until a time zone is needed explicitly,
such as when casting to a ``TIMESTAMP WITH TIME ZONE`` or ``TIME WITH TIME ZONE``.
In those cases, the time zone offset of the session time zone is applied, as specified in the SQL standard.

For various compatibility reasons, when casting from date/time type without a time zone to one with a time zone, a fixed time zone 
is used as opposed to the named on that may be set for the session.

eg. with ``-Duser.timezone="Asia/Kathmandu"`` on CLI

 * Query: ``SELECT CAST(TIMESTAMP '2000-01-01 10:00' AS TIMESTAMP WITH TIME ZONE);``
 * Previous result: ``2000-01-01 10:00:00.000 Asia/Kathmandu``
 * Current result: ``2000-01-01 10:00:00.000 +05:45``

**TIME semantic changes**

The ``TIME`` type was changed similarly to the ``TIMESTAMP`` type.

**TIME WITH TIME ZONE semantic changes**

Due to compatibility requirements, having ``TIME WITH TIME ZONE`` completely aligned with the SQL standard was not possible yet.
For that reason, when calculating the time zone offset for ``TIME WITH TIME ZONE``, the Teradata distribution of Presto uses the session's
start date and time.

This can be seen in queries using ``TIME WITH TIME ZONE`` in a time zone that has had time zone policy changes or uses DST.
eg. With session start time on 1 March 2017

 * Query: ``SELECT TIME '10:00:00 Asia/Kathmandu' AT TIME ZONE 'UTC'``
 * Previous result: ``04:30:00.000 UTC``
 * Current result: ``04:15:00.000 UTC``

**Bugfixes**

 * ``current_time`` and ``localtime`` functions were fixed to return the correct value for non-UTC timezones.
