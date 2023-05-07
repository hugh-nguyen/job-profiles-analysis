import logging

def log(function):
    def wrapper(*args, **kwargs):
        # create logger
        logger = logging.getLogger('job-profile-logger')
        logger.setLevel(logging.DEBUG)

        # create file handler
        fh = logging.FileHandler('logs/job-profile-analysis.log')
        fh.setLevel(logging.DEBUG)

        # create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)

        # add the file handler to the logger
        logger.addHandler(fh)

        logger.info(f'Started executing {function.__name__}')
        try:
            result = function(*args, **kwargs)
        except Exception as error:
            logger.exception(str(error))
            raise error
        logger.info(f'Finished executing {function.__name__}')
        return result
    return wrapper 