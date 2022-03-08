BareWiki Dumper
===

Very minimalist wikipedia dump processor, that should be able to achieve approximately 1000 pages and revisions per second in most modern machines, directly from the bz2 archive - no archive extraction needed. Importing the whole Wikipedia dump with approximately 22 million pages/revisions in 6 hours and a couple of minutes.

Performance may vary, please configure your database accordingly, and know that database connections are calculated as:

> database connections = file threads * loader threads * 3

If you don't want to use the default configurations, please create your own configuration file with the properties displayed on the `resources/barewiki-default.properties` file, then pass it as the first argument when running your .jar, as in:

> java -jar barewiki-dumper-1.0.jar your-file-path-here.properties

The way this dump processor was designed is to be very expansible. If you wish to change anything, or to add more functionality, I would recommend you forking this repository from a desirable point and making it your own.