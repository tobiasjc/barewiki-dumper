BareWiki Dumper
===

Very minimalist wikipedia dump processor, that should be able to achieve $\approx{1000}$ pages and revisions per second in most modern machines, directly from the bz2 archive - no archive extraction needed. Importing the whole Wikipedia dump with $\approx{22}$ million formulas in $6$ hours and a couple of minutes.

Performance may vary, please configure your database accordingly, and know that database connections are calculated as:

$\text{database connections} = \text{file threads} * \text{loader threads} * 3$

The way it was designed is to be very expansible. I recommend you forking this repository from a desirable point and proceeding from that.