"""
    Module for controlling deliveries os samples and projects to GRUS
"""
import paramiko
import getpass
import glob
import time
import stat
import requests
import datetime
from dateutil.relativedelta import relativedelta
import os
import logging
import couchdb
import json
import subprocess
from dateutil import parser

from ngi_pipeline.database.classes import CharonSession, CharonError
from taca.utils.filesystem import do_copy, create_folder
from taca.utils.config import CONFIG

from deliver import ProjectDeliverer, SampleDeliverer, DelivererInterruptedError

logger = logging.getLogger(__name__)


class GrusProjectDeliverer(ProjectDeliverer):
    """ This object takes care of delivering project samples to castor's wharf.
    """
    def __init__(self, projectid=None, sampleid=None, **kwargs):
        super(GrusProjectDeliverer, self).__init__(
            projectid,
            sampleid,
            **kwargs
        )

        self.stagingpathhard = getattr(self, 'stagingpathhard', None)
        if self.stagingpathhard is None:
            raise AttributeError("staginpathhard is required when delivering to GRUS")

    def check_mover_delivery_status(self):
        # todo: maybe makes sense to re-implement method self.get_delivery_status?

        # calling super class method
        charon_status = super(GrusProjectDeliverer, self).get_delivery_status()
        # we don't care if delivery is not in progress
        if charon_status != 'IN_PROGRESS':
            return charon_status

        # if it's 'IN_PROGRESS', checking moverinfo
        delivery_token = self.db_entry().get('delivery_token')
        try:
            mover_status = subprocess.call('moverinfo -i {}'.format(delivery_token), shell=True)
        except Exception, e:
            logger.error('Cannot get the delivery status for project {}'.format(self.projectid))
            # write Traceback to the log file
            logger.exception(e)
            # we do not raise, but exit(1). Traceback will be written to log.
            exit(1)
        else:
            # I don't know what mover_status will look like, but for example 'DELIVERED', 'IN_PROGRESS' and 'FAILED'
            if mover_status == 'DELIVERED':
                # check the filesystem anyway
                if os.path.exists(self.stagingpathhard):
                    logger.error('moverinfo returned status "DELIVERED", but the project folder still exists in DELIVERY_HARD')
                    logger.info('Updating status in Charon: FAILED')
                    # make sure it updates all the samples
                    # but we don't know for which samples we started the delivery
                    self.update_delivery_status('FAILED')
                    self.delete_delivery_token_in_charon()
                    return 'FAILED'
                else:
                    self.update_delivery_status('DELIVERED') # implemented
                    self.delete_delivery_token_in_charon()   # implemented
                    self.send_email()                        # not implemented
                    return 'DELIVERED'
            elif mover_status == 'FAILED':
                self.update_delivery_status('FAILED') # implemented
                self.delete_delivery_token_in_charon()   # implemented
                # todo: to implement this
                self.send_email()                        # not implemented
                return 'FAILED'
            elif mover_status == 'IN_PROGRESS':
                # this is not in Charon yet, talk to Denis
                delivery_started = parser.parse(self.db_entry().get('delivery_started'))
                # alternative way: checking when the folder was created
                # delivery_started = datetime.datetime.fromtimestamp(os.path.getctime(path))
                today = datetime.datetime.now()
                if delivery_started + relativedelta(days=7) >= today:
                    logger.warning('Delivery has been ongoing for more than 7 days. Project will be marked as "FAILED"')
                    self.update_delivery_status('FAILED')
                    self.delete_delivery_token_in_charon()
                    # todo: to implement this
                    self.send_email()
                    return 'FAILED'
                else:
                    # do nothing
                    return 'IN_PROGRESS'



    def deliver_project(self):
        """ Deliver all samples in a project to grus
            :returns: True if all samples were delivered successfully, False if
                any sample was not properly delivered or ready to be delivered
        """

        # moved this part from constructor, as we can create an object without running the delivery (e.g. to check_delivery_status)

        #check if the project directory already exists, if so abort
        hard_stagepath = self.expand_path(self.stagingpathhard)
        if os.path.exists(hard_stagepath):
            logger.error("In {} found already folder {}. No multiple mover deliveries are allowed".format(
                    hard_stagepath, self.projectid))
            raise DelivererInterruptedError("Hard Staged Folder already present")
        else:
            #otherwise lock the delivery by creating the folder
            create_folder(hard_stagepath)
        import pdb; pdb.set_trace()
        logger.info("Delivering {} to GRUS".format(str(self)))
        if self.get_delivery_status() == 'DELIVERED' \
                and not self.force:
            logger.info("{} has already been delivered".format(str(self)))
            return True
        status = True
        try:
            # connect to charon, return list of sample objects
            samples_to_deliver = self.get_staged_samples_from_charon()
        except Exception, e:
            logger.error("Cannot get samples from Charon. Error says: {}".format(str(e)))
            logger.exception(e)
            raise

        if len(samples_to_deliver) == 0:
            logger.warning('No staged samples found in Charon')
            raise AssertionError('No staged samples found in Charon')

        hard_staged_samples = []
        for sample_id in samples_to_deliver:
            try:
                sample_deliverer = GrusSampleDeliverer(self.projectid, sample_id)
                sample_deliverer.deliver_sample()
            except Exception, e:
                logger.error('Sample {} has not been hard staged. Error says: {}'.format(sample_id, error))
                logger.exception(e)
                raise e
            else:
                hard_staged_samples.append(sample_id)
        if len(samples_to_deliver) != len(hard_staged_samples):
            # do we terminate or do we try to deliver partly?
            logger.warning('Not all the samples have been hard staged. Terminating')
            raise AssertionError('len(samples_to_deliver) != len(hard_staged_samples): {} != {}'.format(len(samples_to_deliver), len(hard_staged_samples)))

        try:
            pi_email = self._get_pi_email()
        except Exception, e:

            logger.error("Cannot fetch pi_email from StatusDB. Error says: {}".format(str(e)))
            # print the traceback, not only error message -> isn't it something more useful?
            logger.exception(e)
            status = False
            return status

        try:
            pi_id = self._get_pi_id(pi_email)
        except Exception, e:
            logger.error("Cannot fetch pi_id from snic API. Error says: {}".format(str(e)))
            logger.exception(e)
            status = False
            return status

        # '265' has been created from my local pc and really exists.
        delivery_project_id = '273'
        try:
            delivery_project_id = self._create_delivery_project(pi_id)
        except Exception, e:
            logger.error('Cannot create delivery project. Error says: {}'.format())
            logger.exception(e)
        # if do_delivery failed, no token
        delivery_token = self.do_delivery(delivery_project_id) # instead of to_outbox

        if delivery_token:
            # todo: save delivery_token in Charon
            self.save_delivery_token_in_charon(delivery_token)
        else:
            logger.error('Delivery project has not been created')
            status = False

        return status

    def save_delivery_token_in_charon(self, delivery_token):
        '''Updates delivery_token in Charon
        '''
        ## TODO: need to update ngi_pipeline.database.classes.project_update
        ## and add field in Charon
        charon_session = CharonSession()
        charon_session.project_update(self.projectid, delivery_token=delivery_token)

    def delete_delivery_token_in_charon(self):
        '''Removes delivery_token from Charon upon successful delivery
        '''
        charon_session = CharonSession()
        charon_session.project_update(self.projectid, delivery_token='')

    def do_delivery(self, delivery_project_id):
        # this one returns error : "265 is non-existing at /usr/local/bin/to_outbox line 214". (265 is delivery_project_id, created via api)
        # or: id=P6968-ngi-sw-1488209917 Error: receiver 274 does not exist or has expired.
        hard_stage = self.expand_path(self.stagingpathhard)
        output = subprocess.call('to_outbox {} {}'.format(hard_stage, delivery_project_id), shell=True)
        # if the format is this one: # id=P6968-ngi-sw-1488209917 Error: receiver 274 does not exist or has expired.
        delivery_token = output.split()[0].split('=')[-1]
        delivery_token = 'P6968-ngi-sw-1488209917'
        return delivery_token


    def get_staged_samples_from_charon(self):
        charon_session = CharonSession()
        result = charon_session.project_get_samples(self.projectid)
        samples = result.get('samples')
        if samples is None:
            raise AssertionError('CharonSession returned no results for project {}'.format(self.projectid))

        staged_samples = []
        for sample in samples:
            sample_id = sample.get('sampleid')
            delivery_status = sample.get('delivery_status')
            if delivery_status == 'STAGED':
                staged_samples.append(sample_id)
        return staged_samples


    def _create_delivery_project(self, pi_id):
        return '273'
        # "https://disposer.c3se.chalmers.se/supr-test/api/ngi_delivery/project/create/"
        create_project_url = self.config.get('snic_api_url_create_project')
        user = self.config.get('snic_api_user')
        password = self.config.get('snic_api_password')
        supr_date_format = '%Y-%m-%d'
        today = datetime.date.today()
        six_months_from_now = (today + relativedelta(months=+6))
        data = {
            'ngi_project_name': self.projectid,
            'title': "DELIVERY_{}_{}".format(self.projectid, today.strftime(supr_date_format)),
            'pi_id': pi_id,
            'start_date': today.strftime(supr_date_format),
            'end_date': six_months_from_now.strftime(supr_date_format),
            'continuation_name': '',
            # You can use this field to allocate the size of the delivery
            # 'allocated': size_of_delivery,
            # This field can be used to add any data you like
            'api_opaque_data': '',
            'ngi_ready': False,
            'ngi_delivery_status': ''
        }

        response = requests.post(create_project_url,
                                     data=json.dumps(data),
                                     auth=(user, password))
        if response.status_code != 200:
            raise AssertionError("API returned status code {}. Response: {}. URL: {}".format(response.status_code, response.content, create_project_url))
        # response will look like: {"links_incoming": [], "webpage": "", "abstract": "", "affiliation": "Stockholms universitet", "directory_name": "", "id": 231, "classification3": "", "classification2": "", "classification1": "", "title": "DELIVERY_P6968_2017-02-22", "pi": {"first_name": "Francesco", "last_name": "Vezzi", "id": 121, "email": "francesco.vezzi@scilifelab.se"}, "type": "NGI Delivery", "start_date": "2017-02-22 00:00:00", "ngi_ready": false, "end_date": "2017-08-22 00:00:00", "resourceprojects": [{"allocated": 1000, "resource": {"centre": {"name": "UPPMAX", "id": 4}, "capacity_unit": "GiB", "id": 42, "name": "Grus"}, "id": 255, "allocations": [{"allocated": 1000, "start_date": "2017-02-22", "end_date": "2017-08-22", "id": 278}]}], "links_outgoing": [], "members": [{"first_name": "Francesco", "last_name": "Vezzi", "id": 121, "email": "francesco.vezzi@scilifelab.se"}], "continuation_name": "", "name": "delivery00114", "managed_in_supr": true, "modified": "2017-02-22 13:56:12", "api_opaque_data": "", "ngi_delivery_status": "", "ngi_project_name": "P6968"}
        delivery_id = response.content.get('id')
        return delivery_id

    def _get_pi_id(self, pi_email):
        return '121'

        get_user_url = self.config.get('snic_api_url_get_user')
        get_user_url = '{}?email={}'.format(get_user_url, pi_email)
        username = self.config.get('snic_api_user')
        password = self.config.get('snic_api_password')
        response = requests.get(get_user_url, auth=(username, password))

        if response.status_code != 200:
            raise AssertionError("Status code returned when trying to get PI id for email: {} was not 200. Response was: {}".format(pi_email, response.content))
        result = json.loads(response.content)
        matches = result.get("matches")
        if matches is None:
            raise AssertionError('The response returned unexpected data')
        if len(matches) < 1:
            raise AssertionError("There were no hits in SUPR for email: {}".format(pi_email))
        if len(matches) > 1:
            raise AssertionError("There we more than one hit in SUPR for email: {}".format(pi_email))

        pi_id = matches[0].get("id")
        return pi_id


    def _get_pi_email(self):
        url = CONFIG.get('statusdb', {}).get('url')
        username = CONFIG.get('statusdb', {}).get('username')
        password = CONFIG.get('statusdb', {}).get('password')
        port = CONFIG.get('statusdb', {}).get('port')
        status_db_url = 'http://{}:{}@{}:{}'.format(username, password, url, port)

        status_db = couchdb.Server(status_db_url)
        orderportal_db = status_db['orderportal_ngi']
        view = orderportal_db.view('taca/project_id_to_pi_email')
        rows = view[self.projectid].rows
        if len(rows) < 1:
            raise AssertionError("Project {} not found in StatusDB: {}".format(self.projecid, url))
        if len(rows) > 1:
            raise AssertionError('Project {} has more than one entry in orderportal_db'.format(self.projectid))

        pi_email = rows[0].value
        return pi_email


class GrusSampleDeliverer(SampleDeliverer):
    """
        A class for handling sample deliveries to castor
    """

    def __init__(self, projectid=None, sampleid=None, **kwargs):
        super(GrusSampleDeliverer, self).__init__(
            projectid,
            sampleid,
            **kwargs)

    def deliver_sample(self, sampleentry=None):
        """ Deliver a sample to the destination specified via command line of on Charon.
            Will check if the sample has already been delivered and should not
            be delivered again or if the sample is not yet ready to be delivered.
            Delivers only samples that have been staged.

            :params sampleentry: a database sample entry to use for delivery,
                be very careful with caching the database entries though since
                concurrent processes can update the database at any time
            :returns: True if sample was successfully delivered or was previously
                delivered, False if sample was not yet ready to be delivered
            :raises taca_ngi_pipeline.utils.database.DatabaseError: if an entry corresponding to this
                sample could not be found in the database
            :raises DelivererReplaceError: if a previous delivery of this sample
                has taken place but should be replaced
            :raises DelivererError: if the delivery failed
        """
        # propagate raised errors upwards, they should trigger notification to operator
        # try:
        logger.info("Delivering {} to GRUS with MOVER!!!!!".format(str(self)))
        hard_stagepath = self.expand_path(self.stagingpathhard)
        soft_stagepath = self.expand_path(self.stagingpath)

        try:
            logger.info("Delivering {} to GRUS with MOVER!!!!!".format(str(self)))
            hard_stagepath = self.expand_path(self.stagingpathhard)
            soft_stagepath = self.expand_path(self.stagingpath)
            try:
                if self.get_delivery_status(sampleentry) != 'STAGED':
                    logger.info("{} has not been stages and will not be delivered".format(str(self)))
                    return False
                if self.get_delivery_status(sampleentry) == 'DELIVERED' \
                        and not self.force:
                    logger.info("{} has already been delivered".format(str(self)))
                    return True
                elif self.get_delivery_status(sampleentry) == 'IN_PROGRESS' \
                        and not self.force:
                    logger.info("delivery of {} is already in progress".format(
                            str(self)))
                    return False
                elif self.get_sample_status(sampleentry) == 'FRESH' \
                        and not self.force:
                    logger.info("{} is marked as FRESH (new unporcessed data is available)and will not be delivered".format(str(self)))
                    return False
                elif not os.path.exists(os.path.join(soft_stagepath,self.sampleid)):
                    logger.info("Sample {} marked as STAGED on charon but not found in the soft stage dir {}".format(                            str(self), soft_stagepath))
                    return False
                elif self.get_delivery_status(sampleentry) == 'FAILED':
                        logger.info("retrying delivery of previously failed sample {}".format(str(self)))
            except db.DatabaseError as e:
                logger.error("error '{}' occurred during delivery of {}".format(
                        str(e), str(self)))
                raise
            #at this point copywith deferance the softlink folder
            self.update_delivery_status(status="IN_PROGRESS")
            #call do_delivery
            import pdb
            pdb.set_trace()
        #in case of faiulure put again the status to STAGED
        except DelivererInterruptedError:
            self.update_delivery_status(status="STAGED")
            raise
        except Exception:
            self.update_delivery_status(status="STAGED")
            raise


    def do_delivery(self):
        """ Creating a hard copy of staged data
            :returns: True if delivery was successful, False if unsuccessful
            :raises DelivererTOBEDEFINEDError: if an exception occurred when creating a hard copy
        """

        logger.info("Creating hard copy of sample {}".format(self.sampleid))

        # try:
            # stagingpath is a project directory
        # join stage dir with sample dir
        source = os.path.join(self.expand_path(self.stagingpath), self.sampleid)
        destination = os.path.join(self.expand_path(self.stagingpathhard), self.sampleid)

        # destination must NOT exist
        # if it's already exists, we can:
        # 1. mover is currently moving data -> throw an error
        # 2. we delete folder and create a new copy
        do_copy(source, destination)


        # except Exception as e:
            # print 'Caught exception: {}: {}'.format(e.__class__, e)
            # raise

        logger.info("Sample {} has been hard staged to {}".format(self.sampleid, destination))

        # if something went wrong we got an exception before
        return True