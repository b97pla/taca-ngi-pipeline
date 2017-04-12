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
    def __init__(self, projectid=None, sampleid=None, pi_email=None, **kwargs):
        super(GrusProjectDeliverer, self).__init__(
            projectid,
            sampleid,
            **kwargs
        )

        self.stagingpathhard = getattr(self, 'stagingpathhard', None)
        if self.stagingpathhard is None:
            raise AttributeError("stagingpathhard is required when delivering to GRUS")
        self.config_snic = CONFIG.get('snic',None)
        if self.config_snic is None:
            raise AttributeError("snic confoguration is needed  delivering to GRUS (snic_api_url, snic_api_user, snic_api_password")
        self.config_statusdb = CONFIG.get('statusdb',None)
        if self.config_statusdb is None:
            raise AttributeError("statusdc configuration is needed  delivering to GRUS (url, username, password, port")
        self.pi_email = pi_email


    def get_delivery_status(self, dbentry=None):
        """ Returns the delivery status for this sample. If a sampleentry
        dict is supplied, it will be used instead of fethcing from database
        
        :params sampleentry: a database sample entry to use instead of
        fetching from db
        :returns: the delivery status of this sample as a string
        """
        dbentry = dbentry or self.db_entry()
        if dbentry.get('delivery_token'):
            if dbentry.get('delivery_token') == 'NO-TOKEN':
                return 'NOT_DELIVERED'
            return 'IN_PROGRESS'
        else:
            return 'NOT_DELIVERED'

    def check_mover_delivery_status(self):
        """ This functions checks is project is under delivery. If so it waits until projects is delivered or a certain threshold is met
        """
        charon_status = self.get_delivery_status()
        # we don't care if delivery is not in progress
        if charon_status != 'IN_PROGRESS':
            logger.info("Project {} has no delivery token. Project is not being delivered at the moment".format(self.projectid))
            return
        # if it's 'IN_PROGRESS', checking moverinfo
        delivery_token = self.db_entry().get('delivery_token')
        logger.info("Project {} under delivery. Delivery token is {}. Starting monitoring:".format(self.projectid, delivery_token))
        delivery_status = 'IN_PROGRESS'
        not_monitoring = False
        max_delivery_time = relativedelta(days=2)
        monitoring_start = datetime.datetime.now()
        while ( not not_monitoring ):
            try:
                cmd = ['moverinfo', '-i', delivery_token]
                output=subprocess.check_output(cmd, stderr=subprocess.STDOUT)
            except Exception, e:
                logger.error('Cannot get the delivery status for project {}'.format(self.projectid))
                # write Traceback to the log file
                logger.exception(e)
                # we do not raise, but exit(1). Traceback will be written to log.
                exit(1)
            else:
                #Moverinfo output with option -i can be: InProgress, Accepted, Failed,
                mover_status = output.split(':')[0]
                if mover_status == 'Delivered':
                    # check the filesystem anyway
                    if os.path.exists(self.expand_path(self.stagingpathhard)):
                        logger.error('Delivery {} for project {} delivered done but project folder found in DELIVERY_HARD. Failing delivery.'.format(delivery_token, self.projectid))
                        delivery_status =  'FAILED'
                    else:
                        logger.info("Project {} succefully delivered. Delivery token is {}.".format(self.projectid, delivery_token))
                        delivery_status = 'DELIVERED'
                    not_monitoring = True #stop the monitoring, it is either failed or delivered
                    continue
                else:
                    #check for how long time delivery has been going on
                    if self.db_entry().get('delivery_started'):
                        delivery_started = self.db_entry().get('delivery_started')
                    else:
                        delivery_started = monitoring_start #the first time I checked the status, not necessarly when it begun
                    now = datetime.datetime.now()
                    if now -  max_delivery_time > delivery_started:
                        logger.error('Delivery {} for project {} has been ongoing for more than 48 hours. Check what the f**k is going on. The project status will be reset'.format(delivery_token, self.projectid))
                        delivery_status = 'FAILED'
                        not_monitoring = True #stop the monitoring, it is taking too long
                        continue
                if  mover_status == 'Accepted':
                    logger.info("Project {} under delivery. Status for delivery-token {} is : {}".format(self.projectid, delivery_token, mover_status))
                elif mover_status == 'Failed':
                    logger.warn("Project {} under delivery (attention mover returned {}). Status for delivery-token {} is : {}".format(self.projectid, mover_status, delivery_token, mover_status))
                elif mover_status == 'InProgress':
                    #this is an error because it is a new status
                    logger.info("Project {} under delivery. Status for delivery-token {} is : {}".format(self.projectid, delivery_token, mover_status))
                else:
                    logger.warn("Project {} under delivery. Unexpected status-delivery returned by mover for delivery-token {}: {}".format(self.projectid, delivery_token, mover_status))
            time.sleep(60) #sleep for 15 minutes and then check again the status
        #I am here only if not_monitoring is True, that is only if mover status was delivered or the delivery is ongoing for more than 48h
        if delivery_status == 'DELIVERED' or delivery_status == 'FAILED':
            #fetch all samples that were under delivery
            in_progress_samples = self.get_samples_from_charon(delivery_status="IN_PROGRESS")
            # now update them
            for sample_id in in_progress_samples:
                try:
                    sample_deliverer = GrusSampleDeliverer(self.projectid, sample_id)
                    sample_deliverer.update_delivery_status(status=delivery_status)
                except Exception, e:
                    logger.error('Sample {}: Problems in setting sample status on charon. Error: {}'.format(sample_id, error))
                    logger.exception(e)
            #now reset delivery
            self.delete_delivery_token_in_charon()
            #now check, if all samples in charon are DELIVERED or are ABORTED as status, then the all projecct is DELIVERED
            



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
        #check that this project is not under delivery with mover already in this case stop delivery
        delivery_token = self.get_delivery_token_in_charon()
        if delivery_token is not None:
            logger.error("Project {} is already under delivery {}. No multiple mover deliveries are allowed".format(
                    self.projectid, delivery_token))
            raise DelivererInterruptedError("Proejct already under delivery with Mover")
        logger.info("Delivering {} to GRUS".format(str(self)))
        if self.get_delivery_status() == 'DELIVERED' \
                and not self.force:
            logger.info("{} has already been delivered".format(str(self)))
            return True
        status = True
        #otherwise lock the delivery by creating the folder
        create_folder(hard_stagepath)
        # connect to charon, return list of sample objects that have been staged
        try:
            samples_to_deliver = self.get_samples_from_charon(delivery_status="STAGED")
        except Exception, e:
            logger.error("Cannot get samples from Charon. Error says: {}".format(str(e)))
            logger.exception(e)
            exit(1)
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
                exit(1)
            else:
                hard_staged_samples.append(sample_id)
        if len(samples_to_deliver) != len(hard_staged_samples):
            # do we terminate or do we try to deliver partly?
            logger.warning('Not all the samples have been hard staged. Terminating')
            raise AssertionError('len(samples_to_deliver) != len(hard_staged_samples): {} != {}'.format(len(samples_to_deliver), len(hard_staged_samples)))
        #retrive pi-email
        if self.pi_email is None:
            try:
                self.pi_email = self._get_pi_email()
                logger.info("email for PI for project {} found: {}".format(self.projectid, self.pi_email))
            except Exception, e:
                logger.error("Cannot fetch pi_email from StatusDB. Error says: {}".format(str(e)))
                # print the traceback, not only error message -> isn't it something more useful?
                logger.exception(e)
                status = False
                return status
        else:
            logger.warning("email for PI for project {} specified by user: {}".format(self.projectid,
                        self.pi_email))
        pi_id = ''
        try:
            import pdb
            pdb.set_trace()
            pi_id = self._get_pi_id(self.pi_email)
            logger.info("PI-id for delivering of project {} is {}".format(self.projectid, pi_id))
        except Exception, e:
            logger.error("Cannot fetch pi_id from snic API. Error says: {}".format(str(e)))
            logger.exception(e)
            status = False
            return status

        # create a delivery project id
        supr_name_of_delivery = ''
        try:
            #comment on irma
            delivery_project_info = self._create_delivery_project(pi_id)
            supr_name_of_delivery = delivery_project_info['name']
            logger.info("Delivery project for project {} has been created. Delivery IDis {}".format(self.projectid, supr_name_of_delivery))
        except Exception, e:
            logger.error('Cannot create delivery project. Error says: {}'.format())
            logger.exception(e)

        delivery_token = self.do_delivery(supr_name_of_delivery) # instead of to_outbox
        if delivery_token:
            self.save_delivery_token_in_charon(delivery_token)
        else:
            logger.error('Delivery project has not been created')
            status = False
        return status

    def save_delivery_token_in_charon(self, delivery_token):
        '''Updates delivery_token in Charon
        '''
        charon_session = CharonSession()
        charon_session.project_update(self.projectid, delivery_token=delivery_token)

    def delete_delivery_token_in_charon(self):
        '''Removes delivery_token from Charon upon successful delivery
        '''
        charon_session = CharonSession()
        charon_session.project_update(self.projectid, delivery_token='NO-TOKEN')
    
    def get_delivery_token_in_charon(self):
        '''fetches delivery_token from Charon
        '''
        charon_session = CharonSession()
        project_charon = charon_session.project_get(self.projectid)
        if project_charon.get('delivery_token'):
            return project_charon.get('delivery_token')
        else:
            return None

    def do_delivery(self, supr_name_of_delivery):
        # this one returns error : "265 is non-existing at /usr/local/bin/to_outbox line 214". (265 is delivery_project_id, created via api)
        # or: id=P6968-ngi-sw-1488209917 Error: receiver 274 does not exist or has expired.
        hard_stage = self.expand_path(self.stagingpathhard)
        #need to change group to all files
        os.chown(hard_stage, -1, 47537)
        for root, dirs, files in os.walk(hard_stage):
            for dir in dirs:
                dir_path = os.path.join(root, dir)
                os.chown(dir_path, -1, 47537) #gr_id is the one of ngi2016003
            for file in files:
                fname = os.path.join(root, file)
                os.chown(fname, -1, 47537)
        ##this one is Johan code... check how to parse it
        import pdb
        pdb.set_trace()
        cmd = ['to_outbox', hard_stage, supr_name_of_delivery]
        output=subprocess.check_output(cmd, stderr=subprocess.STDOUT)
        "result looks like this"
        "'id=P6968-ngi2016003-1490007371 Found receiver delivery00009 with end date: 2017-09-17\nP6968 queued for delivery to delivery00009, id = P6968-ngi2016003-1490007371\n'"
        delivery_token = output.split()[0].split('=')[-1]
        return delivery_token


    def get_samples_from_charon(self, delivery_status='STAGED'):
        """Takes as input a delivery status and return all samples with that delivery status
        """
        charon_session = CharonSession()
        result = charon_session.project_get_samples(self.projectid)
        samples = result.get('samples')
        if samples is None:
            raise AssertionError('CharonSession returned no results for project {}'.format(self.projectid))
        samples_of_interest = []
        for sample in samples:
            sample_id = sample.get('sampleid')
            cahron_delivery_status = sample.get('delivery_status')
            if cahron_delivery_status == delivery_status:
                samples_of_interest.append(sample_id)
        return samples_of_interest


    def _create_delivery_project(self, pi_id):
        # "https://disposer.c3se.chalmers.se/supr-test/api/ngi_delivery/project/create/"
        create_project_url = '{}/ngi_delivery/project/create/'.format(self.config_snic.get('snic_api_url'))
        user               = self.config_snic.get('snic_api_user')
        password           = self.config_snic.get('snic_api_password')
        supr_date_format = '%Y-%m-%d'
        today = datetime.date.today()
        three_months_from_now = (today + relativedelta(months=+3))
        data = {
            'ngi_project_name': self.projectid,
            'title': "DELIVERY_{}_{}".format(self.projectid, today.strftime(supr_date_format)),
            'pi_id': pi_id,
            'start_date': today.strftime(supr_date_format),
            'end_date': three_months_from_now.strftime(supr_date_format),
            'continuation_name': '',
            # You can use this field to allocate the size of the delivery
            # 'allocated': size_of_delivery,
            # This field can be used to add any data you like
            'api_opaque_data': '',
            'ngi_ready': False,
            'ngi_delivery_status': ''
        }

        response = requests.post(create_project_url, data=json.dumps(data), auth=(user, password))
        if response.status_code != 200:
            raise AssertionError("API returned status code {}. Response: {}. URL: {}".format(response.status_code, response.content, create_project_url))
        result = json.loads(response.content)
        return result


    def _get_pi_id(self):
        get_user_url = '{}/person/search/'.format(self.config_snic.get('snic_api_url'))
        user         = self.config_snic.get('snic_api_user')
        password     = self.config_snic.get('snic_api_password')
        params   = {'email_i': self.pi_email}
        response = requests.get(get_user_url, params=params, auth=(user, password))

        if response.status_code != 200:
            raise AssertionError("Status code returned when trying to get PI id for email: {} was not 200. Response was: {}".format(self.pi_email, response.content))
        result = json.loads(response.content)
        matches = result.get("matches")
        if matches is None:
            raise AssertionError('The response returned unexpected data')
        if len(matches) < 1:
            raise AssertionError("There were no hits in SUPR for email: {}".format(self.pi_email))
        if len(matches) > 1:
            raise AssertionError("There we more than one hit in SUPR for email: {}".format(self.pi_email))

        pi_id = matches[0].get("id")
        return pi_id


    def _get_pi_email(self):
        url      = self.config_statusdb.get('url')
        username = self.config_statusdb.get('username')
        password = self.config_statusdb.get('password')
        port     = self.config_statusdb.get('port')
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
            logger.info("Trying to deliver {} to GRUS with MOVER".format(str(self)))
            hard_stagepath = self.expand_path(self.stagingpathhard)
            soft_stagepath = self.expand_path(self.stagingpath)
            try:
                if self.get_delivery_status(sampleentry) != 'STAGED':
                    logger.info("{} has not been staged and will not be delivered".format(str(self)))
                    return False
            except db.DatabaseError as e:
                logger.error("error '{}' occurred during delivery of {}".format(str(e), str(self)))
                logger.exception(e)
                raise(e)
            #at this point copywith deferance the softlink folder
            self.update_delivery_status(status="IN_PROGRESS")
            self.do_delivery()
        #in case of faiulure put again the status to STAGED
        except DelivererInterruptedError, e:
            self.update_delivery_status(status="STAGED")
            logger.exception(e)
            raise(e)
        except Exception, e:
            self.update_delivery_status(status="STAGED")
            logger.exception(e)
            raise(e)


    def do_delivery(self):
        """ Creating a hard copy of staged data
        """
        logger.info("Creating hard copy of sample {}".format(self.sampleid))
        # join stage dir with sample dir
        source = os.path.join(self.expand_path(self.stagingpath), self.sampleid)
        destination = os.path.join(self.expand_path(self.stagingpathhard), self.sampleid)
        # destination must NOT exist
        do_copy(source, destination)
        logger.info("Sample {} has been hard staged to {}".format(self.sampleid, destination))
        return
