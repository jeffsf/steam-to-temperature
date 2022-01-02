..
    Copyright © 2022 Jeff Kletsky. All Rights Reserved.

========
Overview
========

This application provides steam-to-temperature for pyDE1 that should be
accurate to within 1°C or better, without weighing the milk, or the like.

Potentially it could be adapted for the VAPE1 or other steam-by-wire machines.

The design goals have mainly been met, which include:

* Uses generally available thermometer from a reputable maker, at under US$100
* Does not require any UI other than the thermometer itself
* Notifies the user when the thermometer is successfully connected
* Notifies the user about the DE1's steam readiness
* Delays the auto-purge until the user is ready
* Does not dedicate the thermometer to the app when the DE1 is not in use
* Conserves thermometer battery

======
Status
======

The steam-to-temperature algorithm has proven very solid and I would call it
"beta" quality. It has been tested and performed well down to 50 mL of water
in both Celsius and Fahrenheit modes. Larger volumes are much easier to work
with as the rate of rise of temperature is much slower.

The logic to notify the user when the thermometer successfully connects
works well.

The logic around automatically releasing and recapturing the thermometer
seems to be generally working and is undergoing test. I would call it
"alhpa" quality.

Although various approaches, including reflecting the current steam
temperature on the thermometer, have been considered, a "not ready to steam"
affordance has not yet been implemented.


============
Installation
============

Install ``bluedot.py`` in a suitable location on the file system, with
appropriately restricted permissions.

The default location of ``/usr/local/etc/pyde1/bluedot.conf`` is likely
already present if pyDE1 is installed on the same machine.

Edit ``bluedot.conf`` as appropriate. The BlueDOT MAC address can be found
on the back of the unit as the serial number.

``bluedot.service`` is provided. It will need to be edited for the location
of ``bluedot.py`` and the proper venv path.

===
Use
===

Set the BlueDOT to either °C or °F, as desired.

The program will attempt to connect to the BlueDOT when the DE1 is not
sleeping. If not immediately found, it will retry, falling back to once
every 30 seconds. The BlueDOT will beep briefly to indicate connection.
If you're looking at the display, you'll see the high alarm displaying
freezing (0°C or 32°F) while beeping.

If the DE1 sleeps, it will disconnect from the BlueDOT, allowing it to be
used elsewhere with the Thermoworks app. As long as it is disconnected from
the Thermoworks app and is on and in range, it will reconnect when the DE1
wakes.

Set the desired target temperature as the high alarm.

Put the probe in the steaming pitcher.

Start steaming with the GHC (or app control for non-GHC machines)

The steam will pause automatically, going into "puff mode". Remove the
pitcher. (For tiny volumes, the puffs can be sufficient to raise
the temperature slightly above target.)

Stop the steaming with the GHC or app control. This will trigger
the usual auto-purge sequence.





