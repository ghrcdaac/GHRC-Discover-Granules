import signal
import sys
import time


# class GracefulKiller:
#     kill_now = False
#
#     def __init__(self):
#         signal.signal(signal.SIGINT, self.exit_gracefully)
#         signal.signal(signal.SIGTERM, self.exit_gracefully)
#
#     def exit_gracefully(self, signum, frame):
#         self.kill_now = True


def test_main(event, context):
    pass


if __name__ == '__main__':
    # print(sys.argv[1:])
    #
    # if len(sys.argv) <= 1:
    #     killer = GracefulKiller()
    #     while not killer.kill_now:
    #         time.sleep(1)
    #         print('looping')
    # else:
    #     print('Calling function')
    #
    # print('terminating')
    pass
