from pilogger.pilogger import PiLogger
import sys
import traceback

def main():
    if len(sys.argv) != 3 and len(sys.argv) != 4:
        print("Usage is python3 {0} <host> <port> [logDirectory]".format(sys.argv[0]), file=sys.stderr)
        return

    try:
        host = sys.argv[1]
        port = int(sys.argv[2])

        if len(sys.argv) == 4:
            logDir = sys.argv[3]
        else:
            logDir = None

        config = {
            "host": host,
            "port": port,
            "log_directory": logDir
        }

        if port < 1 or port > 65535:
            raise ValueError("port must be a valid integer between 1 and 65535")


        logger = PiLogger(config)

    except ValueError as e:
        print("port must be a valid integer between 1 and 65535")




if __name__ == "__main__":
    main()