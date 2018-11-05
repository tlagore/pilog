from piloggee.pilogee import PiLoggee
import time

loggee = PiLoggee()
loggee.start_server()

time.sleep(10)

loggee.stop_server()