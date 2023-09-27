import logging

class Logger:
    def __init__(self, prog_name):
        self.prog_name = prog_name
        # create logger
        self.logger = logging.getLogger(self.prog_name)
        self.logger.setLevel(logging.INFO)

        # create console handler and set level to debug
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)

        # create formatter
        # formatter = logging.Formatter('%(asctime)s [ %(name)s | %(levelname)s ] %(message)s')
        formatter = logging.Formatter('[%(name)s][%(asctime)s] %(levelname)s %(message)s', datefmt='%a, %d %b %Y %H:%M:%S')

        # add formatter to ch
        ch.setFormatter(formatter)

        # add ch to logger
        self.logger.addHandler(ch)


    def info(self, msg, *args, **kwargs):
        self.logger.info(msg, *args, **kwargs)

    def debug(self, msg, *args, **kwargs):
        self.logger.debug(msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        self.logger.warning(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        self.logger.error(msg, *args, **kwargs)

    def setLevel(self, level):
        if level == "debug":
            self.logger.setLevel(logging.DEBUG)
            self.logger.handlers[0].setLevel(logging.DEBUG)
        elif level == "info":
            self.logger.setLevel(logging.INFO)
            self.logger.handlers[0].setLevel(logging.INFO)
        elif level == "warning":
            self.logger.setLevel(logging.WARNING)
            self.logger.handlers[0].setLevel(logging.WARNING)
        elif level == "error":
            self.logger.setLevel(logging.ERROR)
            self.logger.handlers[0].setLevel(logging.ERROR)

# def initLogger(prog_name):

#     # create logger
#     logger = logging.getLogger(prog_name)
#     logger.setLevel(logging.INFO)

#     # create console handler and set level to debug
#     ch = logging.StreamHandler()
#     ch.setLevel(logging.INFO)

#     # create formatter
#     formatter = logging.Formatter('%(asctime)s [ %(name)s | %(levelname)s ] %(message)s')

#     # add formatter to ch
#     ch.setFormatter(formatter)

#     # add ch to logger
#     logger.addHandler(ch)

#     return logger

# def setLoggerLevel(logger, level):
#     if level == "debug":
#         logger.setLevel(logging.DEBUG)
#         logger.handlers[0].setLevel(logging.DEBUG)
#     elif level == "info":
#         logger.setLevel(logging.INFO)
#         logger.handlers[0].setLevel(logging.INFO)
#     elif level == "warning":
#         logger.setLevel(logging.WARNING)
#         logger.handlers[0].setLevel(logging.WARNING)
#     elif level == "error":
#         logger.setLevel(logging.ERROR)
#         logger.handlers[0].setLevel(logging.ERROR)


