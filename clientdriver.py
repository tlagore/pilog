from pilogger.pilogger import PiLogger
import sys
import traceback

def main():
    if len(sys.argv) != 3:
        print("Usage is python3 {0} <host> <port>".format(sys.argv[0]), file=sys.stderr)
        return

    try:
        host = sys.argv[1]
        port = int(sys.argv[2])

        if port < 1 or port > 65535:
            raise ValueError("port must be a valid integer between 1 and 65535")


        logger = PiLogger(host, port)

    except ValueError as e:
        print("port must be a valid integer between 1 and 65535")




if __name__ == "__main__":
    main()