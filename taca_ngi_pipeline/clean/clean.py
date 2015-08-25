__author__ = 'Pontus'

import signal

class CleanerError(Exception): pass
class DatabaseError(CleanerError): pass
class InterruptedError(CleanerError): pass

def _signal_handler(signal, frame):
    """ A custom signal handler which will raise a DelivererInterruptedError
        :raises DelivererInterruptedError:
            this exception will be raised
    """
    raise InterruptedError(
        "interrupt signal {} received while cleaning".format(signal))

class Cleaner(object):
    """

    """

    def __init__(self, projectid, sampleid, **kwargs):
        """

        :param string projectid: id of project to clean
        :param string sampleid: id of sample to clean

        :return:
        """

        # set a custom signal handler to intercept interruptions
        signal.signal(signal.SIGINT, _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)

        self.projectid = projectid
        self.sampleid = sampleid

    def __str__(self):
        return "{}:{}".format(
            self.projectid,self.sampleid) \
            if self.sampleid is not None else self.projectid
